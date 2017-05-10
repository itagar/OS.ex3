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
#include "Thread.h"


/*-----=  Definitions  =-----*/


/**
* @def INITIAL_INPUT_INDEX 0
* @brief A Macro that sets the value of the initial index in the index counters.
*/
#define INITIAL_INPUT_INDEX 0

/**
* @def MINIMUM_THREADS_NUMBER 0
* @brief A Macro that sets the value of minimum number of Threads.
*/
#define MINIMUM_THREADS_NUMBER 0

/**
* @def MAP_CHUNK_SIZE 10
* @brief A Macro that sets the chunk size that the ExecMap will be applied.
*/
#define MAP_CHUNK_SIZE 2
// TODO : CHUNK SIZE
/**
* @def REDUCE_CHUNK_SIZE 10
* @brief A Macro that sets the chunk size that the ExecReduce will be applied.
*/
#define REDUCE_CHUNK_SIZE 10

/**
* @def ERROR_MESSAGE_PREFIX "MapReduceFramework Failure: "
* @brief A Macro that sets error message prefix.
*/
#define ERROR_MESSAGE_PREFIX "MapReduceFramework Failure: "

/**
* @def ERROR_MESSAGE_SUFFIX " failed."
* @brief A Macro that sets error message suffix which follows a function name.
*/
#define ERROR_MESSAGE_SUFFIX " failed."

/**
* @def PTHREAD_CREATE_NAME "pthread_create"
* @brief A Macro that sets function name for pthread_create.
*/
#define PTHREAD_CREATE_NAME "pthread_create"

/**
* @def PTHREAD_MUTEX_LOCK_NAME "pthread_mutex_lock"
* @brief A Macro that sets function name for pthread_mutex_lock.
*/
#define PTHREAD_MUTEX_LOCK_NAME "pthread_mutex_lock"

/**
* @def PTHREAD_MUTEX_UNLOCK_NAME "pthread_mutex_unlock"
* @brief A Macro that sets function name for pthread_mutex_unlock.
*/
#define PTHREAD_MUTEX_UNLOCK_NAME "pthread_mutex_unlock"

/**
* @def PTHREAD_JOIN_NAME "pthread_join"
* @brief A Macro that sets function name for pthread_join.
*/
#define PTHREAD_JOIN_NAME "pthread_join"


/*-----=  Type Definitions  =-----*/


/**
 * @brief Type Definition for the Vector of Threads.
 */
typedef std::vector<Thread> ThreadsVector;


/*-----=  Shared Data  =-----*/


// TODO: Doxygen.
ThreadsVector threads;

// TODO: Doxygen.
// TODO: Give a better name for this variable.
MapReduceBase *mapReduceDriver;

// TODO: Doxygen.
unsigned int currentInputIndex = INITIAL_INPUT_INDEX;

// TODO: Doxygen.
IN_ITEMS_VEC inputItems;


/*-----=  Mutex & Semaphore  =-----*/


// TODO: Doxygen.
pthread_mutex_t threadSpawnMutex = PTHREAD_MUTEX_INITIALIZER;  // TODO: Maybe pthread_mutex_init?

// TODO: Doxygen.
pthread_mutex_t inputIndexMutex = PTHREAD_MUTEX_INITIALIZER;  // TODO: Maybe pthread_mutex_init?


/*-----=  Error Handling Functions  =-----*/


/**
 * @brief A function that prints an error message associated with the
 *        MapReduce Framework with an information of the library call which
 *        caused the failure. The function exit with EXIT_FAILURE value.
 * @param functionName The name of the function that caused the failure.
 */
static void errorProcedure(const char *functionName)
{
    std::cerr << ERROR_MESSAGE_PREFIX << functionName << ERROR_MESSAGE_SUFFIX;
    exit(EXIT_FAILURE);
}


/*-----=  Error Handling Functions  =-----*/


// TODO: Doxygen.
static void *execMap(void *arg)
{
    // Lock a Mutex in order to make a barrier for the Threads execution
    // and halt their progress until all Threads are created.
    if (pthread_mutex_lock(&threadSpawnMutex))
    {
        errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
    }
    // After Thread creation is done we wish to continue the Threads progress.
    if (pthread_mutex_unlock(&threadSpawnMutex))
    {
        errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
    }

    unsigned int startIndex;  // Holds the current index value.

    // Attempt to gain access to the shared index of the current input.
    // If the access gained, we store the value of the index and update it.
    if (pthread_mutex_lock(&inputIndexMutex))
    {
        errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
    }
    startIndex = currentInputIndex;
    currentInputIndex += MAP_CHUNK_SIZE;
    if (pthread_mutex_unlock(&inputIndexMutex))
    {
        errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
    }

    // TODO: Check this.
    // If the input is empty.
    if (startIndex >= inputItems.size())
    {
        pthread_exit(nullptr);
    }

    // Perform Map on the input chunk. If the chunk size is greater then the
    // remaining input items then we perform Map on all the remaining items.
    for (int i = startIndex; i < inputItems.size() && i < MAP_CHUNK_SIZE; ++i)
    {
        mapReduceDriver->Map(inputItems[i].first, inputItems[i].second);
    }

    pthread_exit(nullptr);  // TODO: Check return value here.
}

// TODO: Doxygen.
static void setupMapThreads(int const multiThreadLevel)
{
    assert(multiThreadLevel >= MINIMUM_THREADS_NUMBER);
    // Initialize the size of the Threads Vector to the given number of Threads.
    threads = ThreadsVector((unsigned long) multiThreadLevel);

    // Lock a Mutex in order to make a barrier for the Threads execution
    // and halt their progress until all Threads are created.
    if (pthread_mutex_lock(&threadSpawnMutex))
    {
        errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
    }

    // Create Threads.
    for (auto i = threads.begin(); i != threads.end(); ++i)
    {
        if (pthread_create(i->get_thread(), NULL, execMap, NULL))
        {
            errorProcedure(PTHREAD_CREATE_NAME);
        }
    }

    // Thread creation is done. Now the Threads can start run.
    if (pthread_mutex_unlock(&threadSpawnMutex))
    {
        errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
    }
}

// TODO: Doxygen.
void Emit2(k2Base* k2, v2Base* v2)
{
    pthread_t currentThread = pthread_self();
    std::cerr << "EMIT2: " << currentThread << std::endl;

    // Search for the current Thread Object.
    for (auto i = threads.begin(); i != threads.end(); ++i)
    {
        if (currentThread == *(i->get_thread()))
        {
            // Insert to this Thread the current values of the Map procedure.
            MAP_ITEM mapItem = std::make_pair(k2, v2);
            i->insertItem(mapItem);
            return;
        }
    }
}

// TODO: Doxygen.
void Emit3(k3Base*, v3Base*)
{

}

// TODO: Doxygen.
OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce,
                                    IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel,
                                    bool autoDeleteV2K2)
{
    pthread_t currentThread = pthread_self();
    std::cerr << currentThread << std::endl;

    // TODO: Check mapReduceDriver.
    mapReduceDriver = &mapReduce;  // Set the MapReduce specific implementation.
    inputItems = itemsVec;  // Set the Input Items.
    setupMapThreads(multiThreadLevel);  // Spawn Threads for Map procedure.

    for (auto i = threads.begin(); i != threads.end(); ++i)
    {
        if (pthread_join(*(i->get_thread()), NULL))
        {
            errorProcedure(PTHREAD_JOIN_NAME);
        }
    }

    currentThread = pthread_self();
    std::cerr << currentThread << std::endl;

    OUT_ITEMS_VEC outputItems;  // The final vector to return.

    return outputItems;
}
