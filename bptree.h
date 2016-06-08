#ifndef BTREE_H
#define BTREE_H
/*
 * @author: Theo
 * @team: Team TBD
 * btree implementation header file, source at btree.c.
 * let me know if this stuff is broken and i'll fix it!
 * Leaf nodes need to allow for storing an entire tuple.
 */
#include "heap.h"
#include "bufferManager.h"
 
/*
 *	table: the name of the table for which the B+ tree is created
 * 	key: describes the key used by the index
 *	volatileFlag: persistent or volatile
 */
int createBTree(char * table, char * key, int volatileFlag);
/*
 * btreeName: the name of the disk file storing the B+ tree.
 */
int dropBTree(char * btreeName);

/* Insertion, Deletion, Search */
/*
 * btree: name of btree
 * key: byte array representing a specific key
 * page: an out param where diskaddress will be retrieved
 * recordId: optional out parameter for record ID (are we using record IDs?)
 */
int bTreeInsert(char * btree, char * key, char * value);
int bTreeDelete(char * btree, char * key);
int bTreeFind(char * btree, char * key, DiskAddress * page, int * recordId);

#endif 