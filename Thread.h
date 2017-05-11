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
 * @brief A Class representing a Thread in the Framework which wrap the
 *        pthread_t object and holding some more useful data in order
 *        to maintain the Framework flow more easily. Each Thread holds the
 *        items it produced while running Map procedure.
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
    ~Thread();

    /**
     * @brief Gets the pointer for the thread represented by this Class.
     * @return Pointer to pthread_t of the thread represented by this Class.
     */
    pthread_t *get_thread() const { return _thread; };

    /**
     * @brief Inserts the given item produced during the Map procedure into
     *        this Threads MapItems Vector.
     * @param mapItem The item to insert.
     */
    void insertItem(MAP_ITEM& mapItem);

private:

    /**
     * @brief Pointer to the Thread which this Class represent.
     */
    pthread_t *_thread;

    /**
     * @brief A Vector of items which produced by this Thread while using Map.
     */
    MAP_ITEMS_VEC _mapItems;

    /**
     * @brief Mutex for the MapItems Vector.
     */
    pthread_mutex_t _mapMutex;
};


#endif
