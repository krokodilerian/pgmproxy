CC=gcc -Wall -O3 -g -lpthread -I/usr/include/pgm-5.2 -I/usr/lib/x86_64-linux-gnu/pgm-5.2/include -I/usr/include/glib-2.0 -I/usr/lib/x86_64-linux-gnu/glib-2.0/include


TARGETS=pgmproxy

all: $(TARGETS)

pgmproxy: pgmproxy.c config.h
	$(CC) -o pgmproxy pgmproxy.c -lm -lpgm

install: $(TARGETS)
	install $(TARGETS) /usr/local/bin/


clean:

	rm -f *~ $(TARGETS)




