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


size_t read_socket(void *fd, char *buff, size_t len) {
	return read(*((int *) fd), buff, len);	
};

size_t write_socket(void *fd, char *buff, size_t len) {
	return write(*((int *) fd), buff, len);	
};

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
		pgm_error_t* pgm_err = NULL;
		pgm_sock_t*  g_sock = NULL;
		sa_family_t sa_family = AF_INET;
		

		ASSERTF(!pgm_init (&pgm_err), "Error intializing openpgm");
		ASSERTF(!pgm_socket (&g_sock, sa_family, SOCK_SEQPACKET, IPPROTO_PGM, &pgm_err), "Failed PGM socket creation");
		data->reader = g_sock;

		
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
