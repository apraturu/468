#define EMPTY "empty"

void checkpoint(Buffer *buf);

int pageDump(Buffer *buf, int index);

int printPage(Buffer *buf, DiskAddress diskPage);

int printBlock(Buffer *buf, DiskAddress diskPage);
