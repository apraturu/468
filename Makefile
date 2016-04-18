CC = gcc
FLAGS = -Wall -g

bufferTest: bufferTest.c bufferManager.h bufferTest.h
	$(CC) $(FLAGS) -o bufferTest bufferTest.c libDisk.o libTinyFS.o

mac: bufferTest.c bufferManager.h bufferTest.h
	$(CC) $(FLAGS) -o bufferTest bufferTest.c libDisk32.o libTinyFS32.o

TFS468:	
	wget http://users.csc.calpoly.edu/~foaad/class/468/TFS468/libTinyFS.h 
	wget http://users.csc.calpoly.edu/~foaad/class/468/TFS468/libTinyFS.o 
	wget http://users.csc.calpoly.edu/~foaad/class/468/TFS468/libDisk.h 
	wget http://users.csc.calpoly.edu/~foaad/class/468/TFS468/libDisk.o 
	wget http://users.csc.calpoly.edu/~foaad/class/468/TFS468/tinyFS.h
	wget http://users.csc.calpoly.edu/~foaad/class/468/TFS468/tests.c
	wget http://users.csc.calpoly.edu/~foaad/class/468/TFS468/README
tests: tests.c 
	$(CC) $(CFLAGS) -o tests tests.c libDisk.o libTinyFS.o
