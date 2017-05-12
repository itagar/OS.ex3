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
     * @brief The Constructor for the MapThread Class.
     */
    MapThread();

    /**
     * @brief The Destructor for the MapThread Class.
     */
    virtual ~MapThread() override;

    /**
     * @brief Gets the Map Mutex.
     * @return The Map Mutex.
     */
    pthread_mutex_t getMapMutex() const { return _mapMutex; };

    /**
     * @brief Get access to MapItems Vector of this Thread.
     * @return MapItems Vector of this Thread.
     */
    MAP_ITEMS_VEC getMapItems() { return _mapItems; };

    /**
     * @brief Inserts the given item produced during the Map procedure into
     *        this Threads MapItems Vector.
     * @param mapItem The item to insert.
     */
    void insertItem(const MAP_ITEM &mapItem);

    /**
     * @brief Clear all items from the MapItems Vector
     */
    void clearItems() { _mapItems.clear(); };

    /**
     * @brief Return the empty status of the MapItems Vector.
     * @return true if empty, false otherwise.
     */
    bool isItemsEmpty() const { return _mapItems.empty(); };

private:
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
