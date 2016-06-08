CXXFLAGS = -std=c++0x -O2 -MMD -MP
SRC = $(wildcard *.cpp) $(wildcard FLOPPY_statements/*.cpp) $(wildcard lex_parse/*.cpp)

floppy: $(SRC:.cpp=.o)
	g++ -o $@ $^

-include $(SRC:.cpp=.d)
