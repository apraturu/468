
all: FLOPPY_parser.cpp FLOPPY_lexer.cpp

FLOPPY_parser.cpp: FLOPPY_parser.y
	bison FLOPPY_parser.y -v

FLOPPY_lexer.cpp: FLOPPY_lexer.l
	flex FLOPPY_lexer.l

clean:
	rm -f FLOPPY_parser.cpp FLOPPY_lexer.cpp FLOPPY_parser.h FLOPPY_lexer.h *.output
