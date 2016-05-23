#include "btree.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

/*
 *	table: the name of the table for which the B+ tree is created
 * 	key: describes the key used by the index
 *	volatileFlag: persistent or volatile
 */
int createBTree(char * table, RecordDesc key, int volatileFlag) {
	return 0;
}

/*
 * btreeName: the name of the disk file storing the B+ tree.
 */
int dropBTree(char * btreeName) {
	return 0;

}

/* Insertion, Deletion, Search */
/*
 * btree: name of btree
 * key: byte array representing a specific key
 * page: an out param where diskaddress will be retrieved
 * recordId: optional out parameter for record ID (are we using record IDs?)
 */
int bTreeInsert(char * btree, char * key, char * value) {
	return 0;

}

int bTreeDelete(char * btree, char * key) {
	return 0;

}

int bTreeFind(char * btree, char * key, DiskAddress * page, int * recordId) {
	return 0;

}

int main() {
	return 0;
}