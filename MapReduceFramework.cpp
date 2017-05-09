// TODO: Valgrind
// TODO: README
// TODO: Makefile
// TODO: Implement Log File.

/**
 * @file MapReduceFramework.cpp
 * @author Itai Tagar <itagar>
 *
 * @brief An implementation of the Map Reduce Framework.
 */


/*-----=  Includes  =-----*/


#include <iostream>
#include <pthread.h>
#include <cassert>
#include "MapReduceFramework.h"


/*-----=  Definitions  =-----*/


/**
* @def INITIAL_INPUT_INDEX 0
* @brief A Macro that sets the value of the initial index in the index counters.
*/
#define INITIAL_INPUT_INDEX 0

/**
* @def MAP_CHUNK_SIZE 10
* @brief A Macro that sets the chunk size that the ExecMap will be applied.
*/
#define MAP_CHUNK_SIZE 10

/**
* @def REDUCE_CHUNK_SIZE 10
* @brief A Macro that sets the chunk size that the ExecReduce will be applied.
*/
#define REDUCE_CHUNK_SIZE 10

/**
* @def ERROR_MESSAGE_PREFIX "MapReduceFramework Failue: "
* @brief A Macro that sets error message prefix.
*/
#define ERROR_MESSAGE_PREFIX "MapReduceFramework Failue: "

/**
* @def ERROR_MESSAGE_SUFFIX " failed."
* @brief A Macro that sets error message suffix which follows a function name.
*/
#define ERROR_MESSAGE_SUFFIX " failed."


/*-----=  Type Definitions  =-----*/

// TODO: Doxygen.
typedef std::pair<k2Base*, v2Base*> MID_ITEM;
// TODO: Doxygen.
typedef std::vector<MID_ITEM> MID_ITEMS_VEC;


/*-----=  Thread Class  =-----*/


// TODO: Doxygen.
class Thread
{
public:
    Thread() {};

    ~Thread() {};


};


/*-----=  Shared Data  =-----*/


// TODO: Doxygen.
unsigned int currentInputIndex = INITIAL_INPUT_INDEX;
// TODO: Doxygen.
IN_ITEMS_VEC inputItems;


/*-----=  Mutex & Semaphore  =-----*/


// TODO: Doxygen.
pthread_mutex_t inputIndexMutex = PTHREAD_MUTEX_INITIALIZER;  // TODO: Maybe pthread_mutex_init?


/*-----=  Error Handling Functions  =-----*/


/**
 * @brief A function that prints an error message associated with the
 *        MapReduce Framework with an information of the library call which
 *        caused the failure.
 * @param functionName The name of the function that caused the failure.
 */
static void outputError(const char *functionName)
{
    std::cerr << ERROR_MESSAGE_PREFIX << functionName << ERROR_MESSAGE_SUFFIX;
}


/*-----=  Error Handling Functions  =-----*/


// TODO: Doxygen.
void *execMap(void *arg)
{
    unsigned int startIndex;  // Used to hold the current index value.

    // Attempt to gain access to the shared index of the current input.
    // If the access gained, we store the value of the index and update it.
    pthread_mutex_lock(&inputIndexMutex);
    startIndex = currentInputIndex;
    currentInputIndex += MAP_CHUNK_SIZE;
    pthread_mutex_unlock(&inputIndexMutex);

    // TODO: If the index exceeded input size, finish.
    if (startIndex >= inputItems.size())
    {

    }

    for (int i = startIndex; i < inputItems.size() && i < MAP_CHUNK_SIZE; ++i)
    {
        IN_ITEM currentInputPair = inputItems[i];
        // TODO: Call Map on each item.
    }

}

// TODO: Doxygen.
void spawnMapThreads(IN_ITEMS_VEC& itemsVec, int const multiThreadLevel)
{

}

// TODO: Doxygen.
void Emit2 (k2Base*, v2Base*)
{

}

// TODO: Doxygen.
void Emit3 (k3Base*, v3Base*)
{

}

// TODO: Doxygen.
OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce,
                                    IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel,
                                    bool autoDeleteV2K2)
{
    // Set the Input Items.
    inputItems = itemsVec;


}

