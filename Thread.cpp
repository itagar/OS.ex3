/**
 * @file Thread.cpp
 * @author Itai Tagar <itagar>
 *
 * @brief A Class Implementation for a Thread.
 */


/*-----=  Includes  =-----*/


#include "Thread.h"


/*-----=  Constructors & Destructors  =-----*/


/**
 * @brief The Constructor for the Thread Class.
 */
Thread::Thread() : _thread(new pthread_t), _mapMutex(PTHREAD_MUTEX_INITIALIZER)
{

}

/**
 * @brief The Destructor for the Thread Class.
 */
Thread::~Thread()
{
    pthread_mutex_destroy(&_mapMutex);
    delete _thread;
}


/*-----=  Class Functions Implementation  =-----*/


/**
 * @brief Inserts the given item produced during the Map procedure into
 *        this Threads MapItems Vector.
 * @param mapItem The item to insert.
 */
void Thread::insertItem(MAP_ITEM &mapItem)
{
    _mapItems.push_back(mapItem);
}