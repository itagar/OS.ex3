// TODO: Valgrind
// TODO: README
// TODO: Makefile
// TODO: Implement Log File.
// TODO: Destroy all threads, mutex, semaphores.
// TODO: Free memory of k2,v2.
// TODO: SET CHUNK SIZE TO 10.

/**
 * @file MapReduceFramework.cpp
 * @author Itai Tagar <itagar>
 *
 * @brief An implementation of the Map Reduce Framework.
 */


/*-----=  Includes  =-----*/


#include <iostream>
#include <pthread.h>
#include <semaphore.h>
#include <map>
#include "MapReduceFramework.h"
#include "Thread.h"


/*-----=  Definitions  =-----*/


/**
* @def INITIAL_INPUT_INDEX 0
* @brief A Macro that sets the value of the initial index in the index counters.
*/
#define INITIAL_INPUT_INDEX 0

/**
* @def MAP_CHUNK 10
* @brief A Macro that sets the chunk size that the ExecMap will be applied.
*/
#define MAP_CHUNK 2

/**
* @def REDUCE_CHUNK_SIZE 10
* @brief A Macro that sets the chunk size that the ExecReduce will be applied.
*/
#define REDUCE_CHUNK 10

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

/**
* @def SEM_INIT_NAME "sem_init"
* @brief A Macro that sets function name for sem_init.
*/
#define SEM_INIT_NAME "sem_init"

/**
* @def SEM_WAIT_NAME "sem_wait"
* @brief A Macro that sets function name for sem_wait.
*/
#define SEM_WAIT_NAME "sem_wait"

/**
* @def SEM_POST_NAME "sem_post"
* @brief A Macro that sets function name for sem_post.
*/
#define SEM_POST_NAME "sem_post"


/*-----=  Type Definitions  =-----*/


/**
 * @brief Type Definition for the Vector of Threads.
 */
typedef std::vector<Thread> ThreadsVector;

/**
 * @brief Type Definition for the Vector of pointers to V2 objects.
 */
typedef std::vector<v2Base*> V2Vector;

/**
 * @brief Type Definition for the map of Shuffle items.
 */
typedef std::map<k2Base*, V2Vector> SHUFFLE_ITEMS;

// TODO: Delete This.
pthread_mutex_t printMutex = PTHREAD_MUTEX_INITIALIZER;


/*-----=  Shared Data  =-----*/


/**
 * @brief The Vector of Threads used for the Map.
 */
ThreadsVector mapThreads;

/**
 * @brief The Thread used for the Shuffle.
 */
Thread shuffleThread;

/**
 * @brief The Vector of Threads used for the Reduce.
 */
ThreadsVector reduceThreads;

/**
 * @brief The MapReduce Driver which holds the implementation for the MapReduce.
 */
MapReduceBase *mapReduceDriver;

/**
 * @brief The shared Vector of the given input.
 */
IN_ITEMS_VEC inputItems;

/**
 * @brief The shared Map of the shuffle output.
 */
SHUFFLE_ITEMS shuffleItems;

/**
 * @brief The shared Vector of the result output.
 */
OUT_ITEMS_VEC outputItems;

/**
 * @brief The shared index in the input items.
 *        This index shared by the Map Threads.
 */
unsigned int currentInputIndex = INITIAL_INPUT_INDEX;

/**
 * @brief The shared iterator in the shuffle items.
 *        This iterator shared by the Reduce Threads.
 */
auto currentShuffleIterator = shuffleItems.begin();


/*-----=  Mutex & Semaphore  =-----*/


/**
 * @brief Mutex for Spawn Threads.
 */
pthread_mutex_t threadSpawnMutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * @brief Mutex for accessing the Input Items.
 */
pthread_mutex_t inputIndexMutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * @brief Mutex for accessing the Shuffle Items.
 */
pthread_mutex_t shuffleIteratorMutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * @brief Semaphore for the Shuffle and the Map Threads workflow.
 */
sem_t shuffleSemaphore;


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


/*-----=  Threads Functions  =-----*/


/**
 * @brief The function which execute the Map procedure by each Thread.
 */
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

    // TODO: Delete This.
    pthread_t currentThread = pthread_self();
    pthread_mutex_lock(&printMutex);
    std::cerr << "Spawn Map: " << currentThread << std::endl;
    pthread_mutex_unlock(&printMutex);

    // Attempt to read Chunk of input and perform Map.
    while (true)
    {
        // Attempt to gain access to the shared index of the current input.
        // If the access gained, we store the value of the index and update it.
        if (pthread_mutex_lock(&inputIndexMutex))
        {
            errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
        }
        unsigned int startIndex = currentInputIndex;
        currentInputIndex += MAP_CHUNK;
        if (pthread_mutex_unlock(&inputIndexMutex))
        {
            errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
        }

        // If the input is empty.
        if (startIndex >= inputItems.size())
        {
            // Before exiting this Thread it is necessary to change it's flag
            // of 'isDone' to be true.
            for (auto i = mapThreads.begin(); i != mapThreads.end(); ++i)
            {
                if (currentThread == *(i->getThread()))
                {
                    i->markDone();
                    pthread_exit(nullptr);
                }
            }
        }

        // Perform Map on the input chunk. If the chunk size is greater then the
        // remaining input items then we perform Map on all the remaining items.
        for (int i = startIndex; i < inputItems.size() && i < MAP_CHUNK; ++i)
        {
            mapReduceDriver->Map(inputItems[i].first, inputItems[i].second);
        }
    }
}

// TODO: Implement this.
/**
 * @brief The function which execute the Map procedure by a single Thread.
 */
static void *shuffle(void *arg)
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
    pthread_t currentThread = pthread_self();

    // TODO: Delete This.
    pthread_mutex_lock(&printMutex);
    std::cerr << "Spawn Shuffle: " << currentThread << std::endl;
    pthread_mutex_unlock(&printMutex);

    // TODO: Each time it takes only one item, maybe can take more.
    while (true)
    {
        // Wait for items to work with.
        if (sem_wait(&shuffleSemaphore))
        {
            errorProcedure(SEM_WAIT_NAME);
        }

        MAP_ITEMS_VEC itemsToShuffle;

        // Check if there is a Map Thread which hasn't been marked as done.
        auto threadsIterator = mapThreads.begin();
        for ( ; threadsIterator != mapThreads.end(); ++threadsIterator)
        {
            if (!threadsIterator->isDone() || !threadsIterator->isItemsEmpty())
            {
                // In case this Thread does not have empty items or it
                // is not marked as done we can continue to shuffle.
                break;
            }
        }
        if (threadsIterator == mapThreads.end())
        {
            // All Threads finished their work and all the items been shuffled.
            pthread_exit(nullptr);
        }

        // Iterate through MapThreads and check which has a non-empty container.
        for (auto i = mapThreads.begin(); i != mapThreads.end(); ++i)
        {
            if (!(i->isItemsEmpty()))
            {
                // Found a Thread with items to shuffle.
                // Attempt to lock this Thread MapItems Vector because it's
                // shared by this Thread and the Shuffle Thread.
                pthread_mutex_t currentMutex = i->getMapMutex();
                if (pthread_mutex_lock(&currentMutex))
                {
                    errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
                }

                // TODO: Attempt to get ALL the items of this Thread and clear it's vector.
                MAP_ITEM item = std::make_pair(i->getMapItems().back().first,
                                               i->getMapItems().back().second);
                i->getMapItems().pop_back();

                // Unlock this Thread MapItems Vector.
                if (pthread_mutex_unlock(&currentMutex))
                {
                    errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
                }

                // TODO: Perform the shuffle.
                shuffleItems[item.first].push_back(item.second);
            }
        }
    }
}

/**
 * @brief The function which execute the Reduce procedure by each Thread.
 */
static void *execReduce(void *arg)
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

    // Attempt to read Chunk of input and perform Reduce.
    while (true)
    {
        // Attempt to gain access to the shared iterator of the shuffle items.
        // If the access gained, we store the value of the iterator and update.
        if (pthread_mutex_lock(&shuffleIteratorMutex))
        {
            errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
        }
        auto startIterator = currentShuffleIterator;
        for (int i = 0; i < REDUCE_CHUNK; ++i)
        {
            // Update the shared iterator by the Chunk size. If the Chunk size
            // is greater then the number of remaining items we stop at the
            // end of shuffleItems.
            if (currentShuffleIterator != shuffleItems.end())
            {
                currentShuffleIterator++;
            }
        }
        auto endIterator = currentShuffleIterator;
        if (pthread_mutex_unlock(&shuffleIteratorMutex))
        {
            errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
        }

        // If the input is empty.
        if (startIterator == shuffleItems.end())
        {
            // Before exiting this Thread it is necessary to change it's flag
            // of 'isDone' to be true.
            pthread_t currentThread = pthread_self();
            for (auto i = reduceThreads.begin(); i != reduceThreads.end(); ++i)
            {
                if (currentThread == *(i->getThread()))
                {
                    i->markDone();
                    pthread_exit(nullptr);
                }
            }
        }

        // Perform Reduce on the shuffle chunk.
        for ( ; startIterator != endIterator; ++startIterator)
        {
            mapReduceDriver->Reduce(startIterator->first, startIterator->second);
        }
    }
}

// TODO: Doxygen.
static void setupMapThreads(int const multiThreadLevel)
{
    // Initialize the size of the Threads Vector to the given number of Threads.
    mapThreads = ThreadsVector((unsigned long) multiThreadLevel);

    // Lock a Mutex in order to make a barrier for the Threads execution
    // and halt their progress until all Threads are created.
    if (pthread_mutex_lock(&threadSpawnMutex))
    {
        errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
    }

    // Create Threads.
    for (auto i = mapThreads.begin(); i != mapThreads.end(); ++i)
    {
        if (pthread_create(i->getThread(), NULL, execMap, NULL))
        {
            errorProcedure(PTHREAD_CREATE_NAME);
        }
    }

    // Shuffle Thread creation and Semaphore initialization.
    if (pthread_create(shuffleThread.getThread(), NULL, shuffle, NULL))
    {
        errorProcedure(PTHREAD_CREATE_NAME);
    }
    if (sem_init(&shuffleSemaphore, false, 1))
    {
        errorProcedure(SEM_INIT_NAME);
    }

    // Thread creation is done. Now the Threads can start run.
    if (pthread_mutex_unlock(&threadSpawnMutex))
    {
        errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
    }
}

// TODO: Doxygen.
// TODO: Check if can combine the 2 setup functions.
static void setupReduceThreads(int const multiThreadLevel)
{
    // Initialize the size of the Threads Vector to the given number of Threads.
    reduceThreads = ThreadsVector((unsigned long) multiThreadLevel);

    // Lock a Mutex in order to make a barrier for the Threads execution
    // and halt their progress until all Threads are created.
    if (pthread_mutex_lock(&threadSpawnMutex))
    {
        errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
    }

    // Create Threads.
    for (auto i = reduceThreads.begin(); i != reduceThreads.end(); ++i)
    {
        if (pthread_create(i->getThread(), NULL, execReduce, NULL))
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

/**
 * @brief This function is called by the Map function in order to add a new
 *        pair of K2,V2 values. The function track the Thread which called it
 *        and find it's corresponding MapItems Vector to insert the given pair.
 * @param k2 The K2 value of the pair to add.
 * @param v2 The V2 value of the pair to add.
 */
void Emit2(k2Base* k2, v2Base* v2)
{
    // Search for the current Thread Object.
    pthread_t currentThread = pthread_self();

    // TODO: Delete This.
    pthread_mutex_lock(&printMutex);
    std::cerr << "Emit2: " << currentThread << std::endl;
    pthread_mutex_unlock(&printMutex);

    for (auto i = mapThreads.begin(); i != mapThreads.end(); ++i)
    {
        if (currentThread == *(i->getThread()))
        {
            // Attempt to lock this Thread MapItems Vector because it's
            // shared by this Thread and the Shuffle Thread.
            pthread_mutex_t currentMutex = i->getMapMutex();
            if (pthread_mutex_lock(&currentMutex))
            {
                errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
            }

            // Insert to this Thread the current values of the Map procedure.
            MAP_ITEM mapItem = std::make_pair(k2, v2);
            i->insertItem(mapItem);

            // Unlock this Thread MapItems Vector.
            if (pthread_mutex_unlock(&currentMutex))
            {
                errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
            }

            // Indicate Shuffle that there are items to shuffle.
            if (sem_post(&shuffleSemaphore))
            {
                errorProcedure(SEM_POST_NAME);
            }

            return;
        }
    }
}

// TODO: Doxygen.
void Emit3(k3Base*, v3Base*)
{
    std::cerr << "EMIT3" << std::endl;
}

// TODO: Doxygen.
OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce,
                                    IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel,
                                    bool autoDeleteV2K2)
{
    mapReduceDriver = &mapReduce;  // Set the MapReduce specific implementation.
    inputItems = itemsVec;  // Set the Input Items.
    setupMapThreads(multiThreadLevel);  // Spawn Threads for Map.

    // Join the Map Threads.
    for (auto i = mapThreads.begin(); i != mapThreads.end(); ++i)
    {
        if (pthread_join(*(i->getThread()), NULL))
        {
            errorProcedure(PTHREAD_JOIN_NAME);
        }
    }

    // Join the Shuffle Thread.
    if (pthread_join(*(shuffleThread.getThread()), NULL))
    {
        errorProcedure(PTHREAD_JOIN_NAME);
    }

    setupReduceThreads(multiThreadLevel);  // Spawn Threads for Reduce.

    // Join the Reduce Threads.
    for (auto i = reduceThreads.begin(); i != reduceThreads.end(); ++i)
    {
        if (pthread_join(*(i->getThread()), NULL))
        {
            errorProcedure(PTHREAD_JOIN_NAME);
        }
    }

    // TODO: Check Destructors for the Threads.

    return outputItems;
}
