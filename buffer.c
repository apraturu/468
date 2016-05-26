int writePage(Buffer *buf, DiskAddress diskPage) {
    int found = 0;
    for(int i = 0; i < buf->nBlocks; i++){
        if(buf->Block[i].diskAdress.FD == diskPage.FD) {
            found == 1;
            break;
        }
    }
    
    if(found == 1){
        
    } else {
        readPage(buf, diskPage);
        buf->dirty[i] = 1;
        buf->timeStamp[i] = (unsigned)time(NULL);
    }
    return 1;
}

int flushPage(Buffer *buf, DiskAddress diskPage) {
    int found = 0;
    for(int i = 0; i < buf->nBlocks; i++){
        if(buf->Block[i].diskAdress.FD == diskPage.FD) {
            found == 1;
            break;
        }
    }
    
    if(found == 1){
        tfs_writePage(diskPage.FD, buf->Block[i].diskAddress.pageId, buf->Block[i].block)
        buf->dirty[i] = 0;
    } else {
        return -1;
    }
    
    return 1;
}