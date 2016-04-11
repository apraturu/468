CC = gcc
FLAGS = -Wall -g

bufferManager: bufferManager.c bufferManager.h bufferTest.c bufferTest.h
	$(CC) $(FLAGS) -o bufferManager bufferManager.c bufferTest.c libDisk.o libTinyFS.o

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
