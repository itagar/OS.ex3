/**
 * @file ReduceThread.cpp
 * @author Itai Tagar <itagar>
 *
 * @brief A Class Implementation for a ReduceThread.
 */


/*-----=  Includes  =-----*/


#include "ReduceThread.h"


/*-----=  Class Functions Implementation  =-----*/


/**
 * @brief Inserts the given item produced during the Reduce procedure into
 *        this Threads ReduceItems Vector.
 * @param reduceItem The item to insert.
 */
void ReduceThread::insertItem(const OUT_ITEM &reduceItem)
{
    _reduceItems.push_back(reduceItem);
}



