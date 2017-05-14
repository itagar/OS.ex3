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
     * @brief The Thread which this Class represent.
     */
    pthread_t thread;
};


#endif
