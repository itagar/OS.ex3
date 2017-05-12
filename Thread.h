/**
 * @file Thread.h
 * @author Itai Tagar <itagar>
 *
 * @brief A Class Declaration for a Thread.
 */


#ifndef EX3_THREAD_H
#define EX3_THREAD_H


/*-----=  Includes  =-----*/


#include <pthread.h>
#include "MapReduceFramework.h"


/*-----=  Class Declaration  =-----*/


/**
 * @brief A Class representing a Thread in the Framework which wrap the
 *        pthread_t object and holding some more useful data in order
 *        to maintain the Framework flow more easily.
 */
class Thread
{
public:
    /**
     * @brief The Constructor for the Thread Class.
     */
    Thread();

    /**
     * @brief The Destructor for the Thread Class.
     */
    virtual ~Thread();

    /**
     * @brief Gets the pointer for the thread represented by this Class.
     * @return Pointer to pthread_t of the thread represented by this Class.
     */
    pthread_t *getThread() const { return _thread; };


    /**
     * @brief Get the value of isDone.
     * @return The value of isDone.
     */
    bool isDone() const { return _isDone; };

    /**
     * @brief Mark that this Thread job is done by setting isDone to true.
     */
    void markDone() { _isDone = true; };

private:
    /**
     * @brief Pointer to the Thread which this Class represent.
     */
    pthread_t *_thread;

    /**
     * @brief A flag which determine if this Thread job is done.
     */
    bool _isDone;
};


#endif
