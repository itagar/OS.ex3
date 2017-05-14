/**
 * @file ReduceThread.h
 * @author Itai Tagar <itagar>
 *
 * @brief A Class Declaration for a ReduceThread.
 */


#ifndef EX3_REDUCETHREAD_H
#define EX3_REDUCETHREAD_H


/*-----=  Includes  =-----*/


#include "Thread.h"


/*-----=  Class Declaration  =-----*/


/**
 * @brief A Class representing a ReduceThread in the Framework which wrap the
 *        pthread_t object and holding some more useful data in order
 *        to maintain the Framework flow more easily. Each ReduceThread holds
 *        the items it produced while running Reduce procedure.
 */
class ReduceThread : public Thread
{
public:
    /**
     * @brief Vector of items which produced by this Thread while using Reduce.
     */
    OUT_ITEMS_VEC reduceItems;

};


#endif
