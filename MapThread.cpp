/**
 * @file MapThread.cpp
 * @author Itai Tagar <itagar>
 *
 * @brief A Class Implementation for a MapThread.
 */


/*-----=  Includes  =-----*/


#include "MapThread.h"


/*-----=  Constructors & Destructors  =-----*/


/**
 * @brief The Constructor for the MapThread Class.
 */
MapThread::MapThread() : Thread(), _mapMutex(PTHREAD_MUTEX_INITIALIZER)
{

}

/**
 * @brief The Destructor for the Thread Class.
 */
MapThread::~MapThread()
{
    pthread_mutex_destroy(&_mapMutex);  // TODO: Check Failure.
}


/*-----=  Class Functions Implementation  =-----*/


/**
 * @brief Inserts the given item produced during the Map procedure into
 *        this Threads MapItems Vector.
 * @param mapItem The item to insert.
 */
void MapThread::insertItem(const MAP_ITEM &mapItem)
{
    _mapItems.push_back(mapItem);
}



