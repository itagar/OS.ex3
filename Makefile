CXX= g++
CXXFLAGS= -c -Wall -std=c++11 -pthread -DNDEBUG
CODEFILES= ex3.tar Search.cpp MapReduceFramework.cpp Makefile README
LIBOBJECTS= MapReduceFramework.o Thread.h MapThread.h ReduceThread.h


# Default
default: MapReduceFramework Search


# Executables
MapReduceFramework: MapReduceFramework.o Thread.h MapThread.h ReduceThread.h
	ar rcs MapReduceFramework.a $(LIBOBJECTS)
	-rm -f *.o

Search: MapReduceFramework Search.o
	$(CXX) Search.o -L. MapReduceFramework.a -lpthread -o Search
	-rm -f *.o


# Object Files
Search.o: MapReduceFramework.h MapReduceClient.h Search.cpp
	$(CXX) $(CXXFLAGS) Search.cpp -o Search.o

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
	$(CXX) -g -Wall -std=c++11 Search.cpp -L. MapReduceFramework.a -lpthread -o Valgrind
	valgrind --leak-check=full --show-possibly-lost=yes --show-reachable=yes --undef-value-errors=yes ./Valgrind Itai Dir1 ASA Makefile Dir2 Itai README
	-rm -vf *.o *.a Valgrind

# Valgrind2
Valgrind2: MapReduceFramework Search.cpp
	$(CXX) -g -Wall -std=c++11 test1.cpp -L. MapReduceFramework.a -lpthread -o Valgrind
	valgrind --track-origins=yes ./Valgrind Itai Dir1 ASA Makefile Dir2 Itai README
	-rm -vf *.o *.a Valgrind

# Helgrind
Helgrind: MapReduceFramework Search.cpp
	$(CXX) -g -Wall -std=c++11 Search.cpp -L. MapReduceFramework.a -lpthread -o Helgrind
	valgrind --tool=helgrind ./Helgrind Itai Dir1 ASA Makefile Dir2 Itai README
	-rm -vf *.o *.a Helgrind
	
