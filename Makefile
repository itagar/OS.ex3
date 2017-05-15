CXX= g++
CXXFLAGS= -c -Wall -std=c++11 -pthread -DNDEBUG
CODEFILES= ex3.tar Search.cpp MapReduceFramework.cpp Thread.h MapThread.h ReduceThread.h Makefile README
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

MapReduceFramework.o: MapReduceFramework.h Thread.h MapThread.h ReduceThread.h MapReduceFramework.cpp
	$(CXX) $(CXXFLAGS) MapReduceFramework.cpp -o MapReduceFramework.o


# tar
tar:
	tar -cvf $(CODEFILES)


# Other Targets
clean:
	-rm -vf *.o *.a *.tar Search


# Valgrind
Valgrind: MapReduceFramework MyTest.cpp
	$(CXX) -g -Wall -std=c++11 MyTest.cpp -L. MapReduceFramework.a -lpthread -o Valgrind
	valgrind --leak-check=full --show-possibly-lost=yes --show-reachable=yes --undef-value-errors=yes ./Valgrind
	-rm -vf *.o *.a Valgrind


# Helgrind
Helgrind: MapReduceFramework MyTest.cpp
	$(CXX) -g -Wall -std=c++11 MyTest.cpp -L. MapReduceFramework.a -lpthread -o Helgrind
	valgrind --tool=helgrind ./Helgrind
	-rm -vf *.o *.a Helgrind


# Test
MyTest: MapReduceFramework MyTest.cpp
	$(CXX) $(CXXFLAGS) MyTest.cpp -o MyTest.o
	$(CXX) MyTest.o -L. MapReduceFramework.a -lpthread -o MyTest
	./MyTest
	-rm -vf *.o *.a MyTest
	
