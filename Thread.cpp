/**
 * @file Thread.cpp
 * @author Itai Tagar <itagar>
 *
 * @brief A Class Implementation for a Thread.
 */


/*-----=  Includes  =-----*/


#include "Thread.h"


/*-----=  Constructors & Destructors  =-----*/


// TODO: Doxygen.
Thread::Thread() : _thread(new pthread_t)
{

}

// TODO: Doxygen.
Thread::~Thread()
{
    delete _thread;
}


/*-----=  Class Functions Implementation  =-----*/


// TODO: Doxygen.
void Thread::insertItem(MAP_ITEM &mapItem)
{
    _mapItems.push_back(mapItem);
}