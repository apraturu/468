CC = gcc
FLAGS = -Wall -g

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
