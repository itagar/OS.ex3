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


// TODO: Doxygen.
class Thread
{
public:
    // TODO: Doxygen.
    Thread();

    // TODO: Doxygen.
    ~Thread();

    // TODO: Doxygen.
    pthread_t *get_thread() const { return _thread; };

    // TODO: Doxygen.
    void insertItem(MAP_ITEM& mapItem);

private:

    /**
     * @brief Pointer to the Thread which this Class represent.
     */
    pthread_t *_thread;

    // TODO: Doxygen.
    MAP_ITEMS_VEC _mapItems;

};


#endif
