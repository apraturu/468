/* The default size of the disk and file system block */
#define BLOCKSIZE 2048

#define MAX_FILES 256

/* TinyFS general block format offsets */
#define BLOCK_TYPE  0
#define BLOCK_MAGIC 1
#define BLOCK_LINK  2
#define BLOCK_LINK2 3

/* TinyFS Block Types */
#define SUPERBLOCK  1
#define INODE       2
#define EXTENT      3
#define FREE        4

/* Superblock specific format offsets */
#define SB_DISKNAME 4   /* 16 bytes for disk name */
#define SB_DISKSIZE 20  /* 2 bytes for total size in blocks */
#define SB_TOPINODE 22  /* 2 bytes to address top inode */
#define SB_FREEVECT 24  /* rest = 1000 bytes for free bitvector */

/* Inode block specific format offsets */
#define IN_FILENAME 4   /* 8 bytes for file name */
#define IN_PAGENUM  12  /* 2 bytes for page number, for TFS468 */
#define IN_FILESIZE 14  /* 4 bytes for size in bytes */
#define IN_FIRSTFE  18  /* 2 bytes link to first file Extent */
#define IN_CREATED  20  /* 4 bytes for creation date */
#define IN_DATA     24  /* 1000 bytes for data */ 

#define FE_PAGENUM  4   /* 2 bytes for page number, for TFS468 */
#define FE_DATA     24   /* data starts here in File Extent blocks */

#define TFS_DISK_NAME_SIZE 16
#define TFS_NAME_SIZE 8
#define TFS_MAGICNUMBER 0x46

#define DEFAULT_DISK_SIZE 100000
/* use this name for a default disk file name */
#define DEFAULT_DISK_NAME "tinyFS.dsk"
