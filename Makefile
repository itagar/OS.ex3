CXX= g++
CXXFLAGS= -c -Wall -std=c++11 -pthread -DNDEBUG
CODEFILES= ex3.tar Search.cpp MapReduceFramework.cpp Makefile README
LIBOBJECTS= MapReduceFramework.o Thread.o MapThread.o ReduceThread.o


# Default
default: MapReduceFramework Search


# Executables
MapReduceFramework: MapReduceFramework.o Thread.o MapThread.o ReduceThread.o
	ar rcs MapReduceFramework.a $(LIBOBJECTS)
	-rm -f *.o

Search: MapReduceFramework Search.o
	$(CXX) Search.o -L. MapReduceFramework.a -lpthread -o Search
	-rm -f *.o


# Object Files
Search.o: MapReduceFramework.h MapReduceClient.h Search.cpp
	$(CXX) $(CXXFLAGS) Search.cpp -o Search.o

Thread.o: Thread.h MapReduceFramework.h Thread.cpp
	$(CXX) $(CXXFLAGS) Thread.cpp -o Thread.o

MapThread.o: Thread.h MapThread.h MapThread.cpp
	$(CXX) $(CXXFLAGS) MapThread.cpp -o MapThread.o
	
ReduceThread.o: Thread.h ReduceThread.h ReduceThread.cpp
	$(CXX) $(CXXFLAGS) ReduceThread.cpp -o ReduceThread.o

MapReduceFramework.o: MapReduceFramework.h Thread.h MapThread.h ReduceThread.h MapReduceFramework.cpp
	$(CXX) $(CXXFLAGS) MapReduceFramework.cpp -o MapReduceFramework.o


# tar
tar:
	tar -cvf $(CODEFILES)


# Other Targets
clean:
	-rm -vf *.o *.a *.tar Search

# Valgrind
Valgrind: MapReduceFramework Search.cpp
	$(CXX) -g -Wall -std=c++11 test2.cpp -L. MapReduceFramework.a -lpthread -o Valgrind
	valgrind --leak-check=full --show-possibly-lost=yes --show-reachable=yes --undef-value-errors=yes ./Valgrind Itai Dir1 ASA Makefile Dir2 Itai README
	-rm -vf *.o *.a Valgrind

# Valgrind2
Valgrind2: MapReduceFramework Search.cpp
	$(CXX) -g -Wall -std=c++11 test1.cpp -L. MapReduceFramework.a -lpthread -o Valgrind
	valgrind --track-origins=yes ./Valgrind Itai Dir1 ASA Makefile Dir2 Itai README
	-rm -vf *.o *.a Valgrind

	
