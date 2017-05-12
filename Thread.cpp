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
Thread::Thread() : _thread(new pthread_t), _isDone(false)
{

}

/**
 * @brief The Destructor for the Thread Class.
 */
Thread::~Thread()
{
    delete _thread;
    _thread = nullptr;
}
