CXXFLAGS = -std=c++0x -O2 -MMD -MP
SRC = $(wildcard *.cpp)

floppy: $(SRC:.cpp=.o)
	g++ -o $@ $^

-include $(SRC:.cpp=.d)