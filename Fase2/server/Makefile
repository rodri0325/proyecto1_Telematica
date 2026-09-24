CC = gcc
CFLAGS = -Wall -Wextra -pthread -std=c11
TARGET = servidor

all: $(TARGET)

$(TARGET): server.c
	$(CC) $(CFLAGS) -o $(TARGET) server.c

clean:
	rm -f $(TARGET) *.o

.PHONY: all clean
