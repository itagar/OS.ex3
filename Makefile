CXX= g++
CXXFLAGS= -c -Wall -std=c++11 -pthread -DNDEBUG
CODEFILES= ex3.tar Search.cpp MapReduceFramework.cpp Makefile README
LIBOBJECTS= MapReduceFramework.o


# Default
default: MapReduceFramework Search


# Executables
MapReduceFramework: MapReduceFramework.o
	ar rcs MapReduceFramework.a $(LIBOBJECTS)
	-rm -f *.o

Search: MapReduceFramework Search.o
	$(CXX) Search.o -L. MapReduceFramework.a -lpthread -o Search
	-rm -f *.o


# Object Files
Search.o: MapReduceFramework.h MapReduceClient.h Search.cpp
	$(CXX) $(CXXFLAGS) Search.cpp -o Search.o
	
MapReduceFramework.o: MapReduceFramework.h MapReduceFramework.cpp
	$(CXX) $(CXXFLAGS) MapReduceFramework.cpp -o MapReduceFramework.o


# tar
tar:
	tar -cvf $(CODEFILES)


# Other Targets
clean:
	-rm -vf *.o *.a *.tar Search

	
