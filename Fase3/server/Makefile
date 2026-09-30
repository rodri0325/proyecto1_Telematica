CC = gcc
CFLAGS = -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Wpedantic -O2 -pthread
.PHONY: all clean
all: servidor identidad
servidor: server.c protocol.c protocol.h
	$(CC) $(CFLAGS) -o $@ server.c protocol.c
identidad: identity_service.c
	$(CC) $(CFLAGS) -o $@ identity_service.c
clean:
	rm -f servidor identidad
