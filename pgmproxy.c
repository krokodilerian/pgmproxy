#define _GNU_SOURCE

#include <sys/socket.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <pthread.h>
#include <errno.h>
#include <err.h>

#include <glib.h>

#include <pwd.h>

#include <pgm/pgm.h>



#include "config.h"

/*

	Simple threaded 'tee' replacement.

	The basic idea is to use one reader and multiple writer threads
	that share a ring buffer containing the data. 

*/

#define err_x(x...) errx(1, ## x)

// Assert and print errno (for syscalls)
#define ASSERTF(c, x...) if (c) errx(1, ## x)
#define ASSERTF_E(c, x...) if (c) err(errno, ## x)


#ifdef DEBUG
  #define DBG(x...) fprintf(stderr,## x)
#else
  #define DBG(x...)
#endif

struct pkt {
	size_t len;
	char data[PKTSIZE];
};

typedef size_t (*datafunc) (void *, char *, size_t);

struct wrdata {
	int rpos; // read thread pointer
	int wpos; // write thread pointer
	pthread_mutex_t wrlock;


	struct pkt buff[BUFFER];

	void *reader, *writer;

	datafunc rcv, snd;

	uint64_t dropped, processed;
	int eof;

};


static int              g_max_tpdu = 1500;
static int              g_max_rte = 400*1000;
static int              g_sqns = 100;
static gboolean         g_multicast_loop = FALSE;
static int              g_udp_encap_port = 8000;


size_t read_socket(void *fd, char *buff, size_t len) {
	return read(*((int *) fd), buff, len);	
};

size_t write_socket(void *fd, char *buff, size_t len) {
	return write(*((int *) fd), buff, len);	
};

size_t write_pgm(void *gsock, char *buff, size_t len) {
	size_t out, clen=len;
	int status = PGM_IO_STATUS_NORMAL;

	status = pgm_send(gsock, buff, clen, &out);

	if (status == PGM_IO_STATUS_NORMAL) return out;
	return -1;

}

size_t read_pgm(void *gsock, char *buff, size_t len) {
	size_t out, clen=len;
	int status = PGM_IO_STATUS_NORMAL;

	status = pgm_recv(gsock, buff, clen, 0, &out, NULL);

	if (status == PGM_IO_STATUS_NORMAL) return out;
	return -1;

}


#ifndef max
	#define max( a, b ) ( ((a) > (b)) ? (a) : (b) )
#endif

#ifndef min
	#define min( a, b ) ( ((a) < (b)) ? (a) : (b) )
#endif

inline void sleep_10ms() {
	struct timespec tv;
	tv.tv_sec = 0;
	tv.tv_nsec = 1000*1000*10; // 10ms
	nanosleep( &tv, NULL );
}

/*
	returns the distance between positions a and b in a
	ringbuffer with size of BUFFER
*/
int diffpos(int a, int b) {
	if (b>=a) return b-a;
	return a-b + BUFFER;
}

/*
	check wether if we read a packet at position rp in a ring buffer
	with size of BUFFER, we won't clobber wp

	it's basically like this:

	if we don't overflow the buffer, then wp needs to be either before
	rp, or after rp+len

	if we overflow the buffer, then wp needs to be before rp AND after
	the remainder of the piece of data (len + rp - BUFFER)

	if we are exactly at the end of the buffer and the wp is 0, we 
	have a fucking specific corner case.

*/
int overflows (int rp, int len, int wp) {
	int rem;

	if (wp == rp) return 0;

	if ( ( (rp+len) < BUFFER ) && ( (wp < rp) || (wp > rp + len) ) )
		return 0;

	// really fugly corner case
	if ( (rp+len) % BUFFER == wp )
		return 1;

	rem = len + rp - BUFFER;

	if ( (wp < rp) && (wp > rem) )
		return 0;

	return 1;
}


#define RP dat->rpos
#define WP dat->wpos

void *rcv_thread(void *ptr) {
	char buff[PKTSIZE];
	struct wrdata *dat = (struct wrdata *) ptr;
	int buffdat = 0;

	int overflow;

	while (42) {

		if ( (buffdat = dat->rcv (dat->reader, buff, PKTSIZE ) ) <=0 ) {
			if ( (buffdat == -1) && (errno = EAGAIN))
				continue;
			__sync_fetch_and_add(&dat->eof, 1);
			
			pthread_exit(NULL);
		}

		while (43) {
			pthread_mutex_lock(&dat->wrlock);

			if (dat->processed==0) break;
			// the current buffer is full, switch to a new one
			// or the timeout passed
			overflow = overflows (RP,  1, WP);

			if (!overflow) 
				break;

			pthread_mutex_unlock( &dat->wrlock );
			DBG("Ring buffer overflow, sleeping - RP %d WP %d ofw %d\n", RP, WP, overflow);
			sleep_10ms();
		}

		dat->buff[RP].len = buffdat;
		memcpy(dat->buff[RP].data, buff, buffdat);

		RP = (RP + 1) % BUFFER;
		DBG ("RP moved to %d\n", RP);

		pthread_mutex_unlock( &dat->wrlock );
		dat->processed ++;
	}
	return NULL;
}


void *wrt_thread(void *ptr) {
	struct pkt packet;

	struct wrdata *dat = (struct wrdata *) ptr;

	memset(&packet, 0, sizeof(struct pkt));

	DBG("Starting write thread\n");

	while (42) {

		if ( RP == WP ) {

			if (dat->eof > 0) {
				__sync_fetch_and_add(&dat->eof, 1);
				if (dat->eof == 1)
					exit(0);
				pthread_exit(NULL);
			}

			sleep_10ms();
			continue;
		}

		for ( ; WP != RP; ) {
			pthread_mutex_lock( &dat->wrlock );

			memcpy(&packet, &dat->buff[WP], sizeof(struct pkt));

			WP ++;;
			WP %= BUFFER;

			pthread_mutex_unlock( &dat->wrlock );// Dumping the data, leave the reader to work	

			// Write the data 

			DBG("writing pos %d len %ld\n", WP, packet.len);
			if ( dat->snd( dat->writer, packet.data, packet.len ) < packet.len ) {
				DBG("failed writing, error %s\n", strerror(errno));
				exit(3);
			}

		}
	}
	return NULL;
}

#undef RP
#undef WP


// direction: 0 - recv, 1 - send

struct pgm_sock_t *create_pgm_socket(char *net, char *port, int direction) {
	pgm_error_t* pgm_err = NULL;
	pgm_sock_t*  g_sock = NULL;
	sa_family_t sa_family = AF_INET;
	struct pgm_addrinfo_t* res = NULL;

	ASSERTF (!pgm_getaddrinfo (net, NULL, &res, &pgm_err), "Parsing network parameter: %s", pgm_err->message);

	DBG("len send %d recv %d\n", res->ai_send_addrs_len, res->ai_recv_addrs_len);

	sa_family = res->ai_send_addrs[0].gsr_group.ss_family;

	ASSERTF(!pgm_init (&pgm_err), "Error intializing openpgm");
	ASSERTF(!pgm_socket (&g_sock, sa_family, SOCK_SEQPACKET, IPPROTO_PGM, &pgm_err), "Failed PGM socket creation");

	const int no_router_assist = 0;
	pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_IP_ROUTER_ALERT, &no_router_assist, sizeof(no_router_assist));

	int send_only, recv_only;

	if (direction == 0) {
		recv_only = 1;
		send_only = 0;
	} else {
		recv_only = 0;
		send_only = 1;
	}

	const int ambient_spm = pgm_secs (30),
		heartbeat_spm[] = { pgm_msecs (100), /* XXX tune these! */
				    pgm_msecs (100),
				    pgm_msecs (100),
				    pgm_msecs (100),
				    pgm_msecs (1300),
				    pgm_secs  (7),
				    pgm_secs  (16),
				    pgm_secs  (25),
				    pgm_secs  (30) };

	pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_SEND_ONLY, &send_only, sizeof(send_only));
	pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_RECV_ONLY, &recv_only, sizeof(recv_only));
	pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_MTU, &g_max_tpdu, sizeof(g_max_tpdu));
	if (direction == 0)  {
		const int	peer_expiry = pgm_secs (300),
				spmr_expiry = pgm_msecs (250),
				nak_bo_ivl = pgm_msecs (50),
				nak_rpt_ivl = pgm_secs (2),
				nak_rdata_ivl = pgm_secs (2),
				nak_data_retries = 50,
				nak_ncf_retries = 50,
				passive = 0;

		pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_PASSIVE, &passive, sizeof(passive));
		pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_RXW_SQNS, &g_sqns, sizeof(g_sqns));
		pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_PEER_EXPIRY, &peer_expiry, sizeof(peer_expiry));
		pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_SPMR_EXPIRY, &spmr_expiry, sizeof(spmr_expiry));
		pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_NAK_BO_IVL, &nak_bo_ivl, sizeof(nak_bo_ivl));
		pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_NAK_RPT_IVL, &nak_rpt_ivl, sizeof(nak_rpt_ivl));
		pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_NAK_RDATA_IVL, &nak_rdata_ivl, sizeof(nak_rdata_ivl));
		pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_NAK_DATA_RETRIES, &nak_data_retries, sizeof(nak_data_retries));
		pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_NAK_NCF_RETRIES, &nak_ncf_retries, sizeof(nak_ncf_retries));

	} else {
		pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_TXW_SQNS, &g_sqns, sizeof(g_sqns));
		pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_TXW_MAX_RTE, &g_max_rte, sizeof(g_max_rte));

	}
	pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_AMBIENT_SPM, &ambient_spm, sizeof(ambient_spm));
	pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_HEARTBEAT_SPM, &heartbeat_spm, sizeof(heartbeat_spm));


	struct pgm_sockaddr_t addr;
	memset (&addr, 0, sizeof(addr));
	addr.sa_port = atoi(port);
	addr.sa_addr.sport = DEFAULT_DATA_SOURCE_PORT;
	ASSERTF (!pgm_gsi_create_from_hostname (&addr.sa_addr.gsi, &pgm_err), "Creating GSI: %s", pgm_err->message);

	struct pgm_interface_req_t if_req;
	memset (&if_req, 0, sizeof(if_req));
	if_req.ir_interface = res->ai_recv_addrs[0].gsr_interface;
	if_req.ir_scope_id  = 0;
	if (AF_INET6 == sa_family) {
		struct sockaddr_in6 sa6;
		memcpy (&sa6, &res->ai_recv_addrs[0].gsr_group, sizeof(sa6));
		if_req.ir_scope_id = sa6.sin6_scope_id;
	}

	ASSERTF(!pgm_bind3 (g_sock,
			&addr, sizeof(addr),
			&if_req, sizeof(if_req),
			&if_req, sizeof(if_req),
			&pgm_err), 
			"Binding PGM socket: %s", pgm_err->message
		);

	/* join IP multicast groups */
	for (unsigned i = 0; i < res->ai_recv_addrs_len; i++)
		pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_JOIN_GROUP, &res->ai_recv_addrs[i], sizeof(struct group_req));
	pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_SEND_GROUP, &res->ai_send_addrs[0], sizeof(struct group_req));
	pgm_freeaddrinfo (res);

	/* set IP parameters */
	const int blocking = 0, 
		  multicast_loop = g_multicast_loop ? 1 : 0,
		  multicast_hops = 16,
		  dscp = 0x2e << 2;	     /* Expedited Forwarding PHB for network elements, no ECN. */

	pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_MULTICAST_LOOP, &multicast_loop, sizeof(multicast_loop));
	pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_MULTICAST_HOPS, &multicast_hops, sizeof(multicast_hops));
	if (AF_INET6 != sa_family)
		pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_TOS, &dscp, sizeof(dscp));
	pgm_setsockopt (g_sock, IPPROTO_PGM, PGM_NOBLOCK, &blocking, sizeof(blocking));

	ASSERTF (!pgm_connect (g_sock, &pgm_err), "Connecting PGM socket: %s", pgm_err->message);

	return g_sock;

}


int main(int argc, char **argv) {

	struct timespec tv;
	struct wrdata *data;

	int intype, outtype;

	pthread_t thread;
	pthread_attr_t attr;

	setlinebuf(stdout);
	setlinebuf(stderr);


	if (argc != 7 ) {
		printf ("Usage: %s intype ip port outtype ip port\n", argv[0]);
		printf ("\tTypes: 0 - stdin/stdout, 1 - UDP, 2 - PGM\n");
		printf ("\tFor PGM use INTERFACE;IP\n");
		exit(3);
	}

	data = (struct wrdata *) calloc (1, sizeof(struct wrdata));
	if (!data)
		exit(4);

	pthread_mutex_init(&data->wrlock, NULL);



	intype = atoi(argv[1]);


	if (intype == 0) {
		data->reader = (int *) malloc(sizeof(int));
		*((int *)data->reader) = STDIN_FILENO;
		data->rcv = read_socket;
	} else if (intype == 1) {
		int sock;
		struct sockaddr_in sin;

		memset(&sin, 0, sizeof(struct sockaddr_in));
		sin.sin_family = AF_INET;
		sin.sin_addr.s_addr = inet_addr(argv[2]);
		sin.sin_port = htons(atoi(argv[3]));
		ASSERTF_E((sock = socket(AF_INET, SOCK_DGRAM, 0))<0, "Failed to create input socket");

		ASSERTF_E(bind(sock, &sin, sizeof(struct sockaddr_in)), "Failed to bind to %s:%s", argv[2], argv[3]);

		data->reader = (int *) malloc(sizeof(int));
		*((int *)data->reader) = sock;

		data->rcv = read_socket;
	} else if (intype == 2) {

		data->reader = create_pgm_socket(argv[2], argv[3], 0);
		data->rcv = read_pgm;

		
	} else err_x("Unknown in-type %d", intype);

	outtype = atoi(argv[4]);


	if (outtype == 0) {
		data->writer = (int *) malloc(sizeof(int));
		*((int *)data->writer) = STDIN_FILENO;
		data->snd = write_socket;
	} else if (outtype == 1) {
		int sock;
		struct sockaddr_in sin;

		memset(&sin, 0, sizeof(struct sockaddr_in));
		sin.sin_family = AF_INET;
		sin.sin_addr.s_addr = inet_addr(argv[5]);
		sin.sin_port = htons(atoi(argv[6]));
		ASSERTF_E((sock = socket(AF_INET, SOCK_DGRAM, 0))<0, "Failed to create input socket");

		ASSERTF_E(connect(sock, &sin, sizeof(struct sockaddr_in)), "Failed to connect to %s:%s", argv[2], argv[3]);

		data->writer = (int *) malloc(sizeof(int));
		*((int *)data->writer) = sock;

		data->snd = write_socket;
	} else if (outtype == 2) {
		data->writer = create_pgm_socket(argv[5], argv[6], 1);
		data->snd = write_pgm;


	} else err_x("Unknown in-type %d", outtype);

		
	pthread_attr_init(&attr);
	pthread_create(&thread, &attr, rcv_thread, data);

	pthread_attr_init(&attr);
	pthread_create(&thread, &attr, wrt_thread, data);

	while (42) {
		tv.tv_sec = STATS_INTERVAL;
		tv.tv_nsec = 0;
		nanosleep( &tv, NULL);
		fprintf(stderr, "Procesed:\t%ld\tWP %d RP", data->processed, data->rpos);
		fprintf (stderr, " %d", diffpos(data->rpos, data->wpos));
		fprintf(stderr, "\n");
	}

	return 0;
}
