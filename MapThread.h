/**
 * @file MapThread.h
 * @author Itai Tagar <itagar>
 *
 * @brief A Class Declaration for a MapThread.
 */


#ifndef EX3_MAPTHREAD_H
#define EX3_MAPTHREAD_H


/*-----=  Includes  =-----*/


#include "Thread.h"


/*-----=  Type Definitions  =-----*/


/**
 * @brief Type Definition for the pair that is the output of the Map procedure.
 *        i.e. the Emit2 output.
 */
typedef std::pair<k2Base*, v2Base*> MAP_ITEM;

/**
 * @brief Type Definition for the vector of Map Items.
 */
typedef std::vector<MAP_ITEM> MAP_ITEMS_VEC;


/*-----=  Class Declaration  =-----*/


/**
 * @brief A Class representing a MapThread in the Framework which wrap the
 *        pthread_t object and holding some more useful data in order
 *        to maintain the Framework flow more easily. Each MapThread holds the
 *        items it produced while running Map procedure.
 */
class MapThread : public Thread
{
public:
    /**
     * @brief A Vector of items which produced by this Thread while using Map.
     */
    MAP_ITEMS_VEC mapItems;

    /**
     * @brief Mutex for the MapItems Vector.
     */
    pthread_mutex_t mapMutex;

    /**
     * @brief A flag for this Map Thread which indicates that it called Emit2.
     */
    bool emitted;

};


#endif
