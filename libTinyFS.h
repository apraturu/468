typedef int fileDescriptor;

/* Makes a blank TinyFS file system of size nBytes on the file specified by
 * 'filename'. This function should use the emulated disk library to open the
 * specified file, and upon success, format the file to be mountable. This
 * includes initializing all data to 0x00, setting magic numbers, initializing
 * and writing the superblock and inodes, etc.
 * Must return a specified success/error code. */
int tfs_mkfs(char *filename, int nBytes);

/* tfs_mount(char *filename) mounts a TinyFS file system located within
 * 'filename'. tfs_unmount(void) unmounts the currently mounted file system.
 * As part of the mount operation, tfs_mount should verify the file system is
 * the correct type. Only one file system may be mounted at a time. Use
 * tfs_unmount to cleanly unmount the currently mounted file system. Must
 * return a specified success/error code. */
int tfs_mount(char *filename);
int tfs_unmount(void);

/* Opens a file for reading and writing on the currently mounted file system.
 * Creates a dynamic resource table entry for the file, and returns a file
 * descriptor (integer) that can be used to reference this file while
 * the filesystem is mounted. */
fileDescriptor tfs_openFile(char *name);

/* Closes the file, de-allocates all system/disk resources, and
 * removes table entry */
int tfs_closeFile(fileDescriptor FD);

/* Writes buffer 'buffer' of size 'size', which represents an entire file's
 * content, to the file system. Sets the file pointer to 0 (the start of
 * file) when done. Returns success/error codes. */
int tfs_writeFile(fileDescriptor FD,char *buffer, int size);

/* TFS468 read/write page */
int tfs_writePage_size(fileDescriptor FD,unsigned int page, unsigned char * data, int size);
int tfs_writePage(fileDescriptor FD,unsigned int page, unsigned char * data);
int tfs_readPage(fileDescriptor FD,unsigned int page, unsigned char * data);

/* returns number of pages on a TFS486 file including the first page (inode) */
int tfs_numPages(fileDescriptor FD);

/* deletes a file and marks its blocks as free on disk. */
int tfs_deleteFile(fileDescriptor FD);

/* reads one byte from the file and copies it to buffer, using the current file
 * pointer location and incrementing it by one upon success. If the file
 * pointer is already at the end of the file then tfs_readByte() should return
 * an error and not increment the file pointer. */
int tfs_readByte(fileDescriptor FD, char *buffer);

/* change the file pointer location to offset (absolute).
 * Returns success/error codes.*/
int tfs_seek(fileDescriptor FD, int offset);

/* Renames the file with name 'old' to the name 'new'.
 * Returns an error if the old file cannot be found. */
int tfs_rename(char* oldname, char* newname);

/* Lists all files on the disk, and some information about each */
int tfs_readDir(void);


/* Displays the creation, last modification, and last access times of a file */
int tfs_readFileInfo(fileDescriptor FD);
