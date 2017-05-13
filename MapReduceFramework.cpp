// TODO: Valgrind
// TODO: README
// TODO: Makefile
// TODO: Implement Log File.
// TODO: Destroy all threads, mutex, semaphores.
// TODO: Free memory of k2,v2 when auto delete.
// TODO: SET CHUNK SIZE TO 10.
// TODO: Check Destructors for the Threads.
// TODO: Check 80 chars in line.
// TODO: Split to helper functions.
// TODO: Shuffle + free resources.
// TODO: Check empty input.
// TODO: Every once in a while there is a deadlock or seg fault. check this.

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
#include <algorithm>
#include <cassert>
#include <functional>
#include "MapReduceFramework.h"
#include "Thread.h"
#include "MapThread.h"
#include "ReduceThread.h"


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
#define REDUCE_CHUNK 2

/**
 * @def SHUFFLE_SEMAPHORE_VALUE 0
 * @brief A Macro that sets the value of the Shuffle Semaphore.
 */
#define SHUFFLE_SEMAPHORE_VALUE 0

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
 * @def PTHREAD_JOIN_NAME "pthread_join"
 * @brief A Macro that sets function name for pthread_join.
 */
#define PTHREAD_JOIN_NAME "pthread_join"

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
 * @def PTHREAD_MUTEX_DESTROY_NAME "pthread_mutex_destroy"
 * @brief A Macro that sets function name for pthread_mutex_destroy.
 */
#define PTHREAD_MUTEX_DESTROY_NAME "pthread_mutex_destroy"

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

/**
 * @def SEM_DESTROY_NAME "sem_destroy"
 * @brief A Macro that sets function name for sem_destroy.
 */
#define SEM_DESTROY_NAME "sem_destroy"


/*-----=  Type Definitions  =-----*/


/**
 * @brief Type Definition for the Vector of MapThread.
 */
typedef std::vector<MapThread> MapThreadsVector;

/**
 * @brief Type Definition for the Vector of ReduceThread.
 */
typedef std::vector<ReduceThread> ReduceThreadsVector;

/**
 * @brief Type Definition for the Vector of pointers to V2 objects.
 */
typedef std::vector<v2Base*> V2Vector;

/**
 * @brief Type Definition for the pair of Shuffle item.
 */
typedef std::pair<k2Base*, V2Vector> SHUFFLE_ITEM;

/**
 * @brief Type Definition for the map of Shuffle items.
 */
typedef std::map<k2Base*, V2Vector> SHUFFLE_ITEMS;

/**
 * @brief Type Definition for the Threads routine pointer.
 */
typedef void *(*threadRoutine)(void *);


/*-----=  Shared Data  =-----*/


/**
 * @brief The Vector of Threads used for the Map.
 */
MapThreadsVector mapThreads;

/**
 * @brief The Thread used for the Shuffle.
 */
Thread shuffleThread;

/**
 * @brief The Vector of Threads used for the Reduce.
 */
ReduceThreadsVector reduceThreads;

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
 * @brief The shared index in the input items.
 *        This index shared by the Map Threads.
 */
unsigned int currentInputIndex = INITIAL_INPUT_INDEX;

/**
 * @brief The shared iterator in the shuffle items.
 *        This iterator shared by the Reduce Threads.
 */
auto currentShuffleIterator = shuffleItems.begin();

/**
 * @brief The flag which indicates autoDeleteV2K2 status given to the Framework.
 */
bool autoDeleteV2K2Flag;


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

// TODO: Delete This.
pthread_mutex_t printMutex = PTHREAD_MUTEX_INITIALIZER;


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

    // The size of the input.
    auto inputSize = inputItems.size();

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
        if (startIndex >= inputSize)
        {
            // Before exiting this Thread it is necessary to change it's flag
            // of 'isDone' to be true.
            pthread_t currentThread = pthread_self();
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
        for (unsigned int i = startIndex;
             i < inputSize && i < startIndex + MAP_CHUNK;
             ++i)
        {
            mapReduceDriver->Map(inputItems[i].first, inputItems[i].second);
        }

        // Indicate Shuffle that there are items to shuffle.
        if (sem_post(&shuffleSemaphore))
        {
            errorProcedure(SEM_POST_NAME);
        }
    }
}

/**
 * @brief Check Equality between 2 item keys from the Shuffle Map.
 *        The two items to compare are a pair of K2 and a Vector of V2 items,
 *        i.e. a Shuffle Item. The second item is a Map Item which first
 *        received by the ExecShuffle from a Map Thread.
 *        Both items are pairs with the K2 as first.
 *        K2 implement the operator <. We say that the first item equals
 *        the second item if the first K2 doesn't greater then the second one
 *        and also doesn't smaller then it.
 * @param lhs The first item to compare.
 * @param rhs The second item to compare.
 * @return True if the two items are equal, false otherwise.
 */
static bool shuffleEquality(const MAP_ITEM &lhs, const SHUFFLE_ITEM &rhs)
{
    assert(lhs.first != nullptr && rhs.first != nullptr);
    return !(*(lhs.first) < *(rhs.first)) && !(*(rhs.first) < *(lhs.first));
}

// TODO: Fix this function.
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

    while (true)
    {
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

        // TODO: Moved semaphore here, Check this!
        // Wait for items to work with.
        if (sem_wait(&shuffleSemaphore))
        {
            errorProcedure(SEM_WAIT_NAME);
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

                // Absorb all the items from this Thread and clear it's Vector.
                itemsToShuffle = i->getMapItems();
                i->clearItems();

                // Unlock this Thread MapItems Vector.
                if (pthread_mutex_unlock(&currentMutex))
                {
                    errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
                }

                break;
            }
        }

        // The Shuffle.
        for (auto i = itemsToShuffle.begin(); i != itemsToShuffle.end(); ++i)
        {
            // Create the current item to shuffle.
            MAP_ITEM item = std::make_pair(i->first, i->second);

            // Search for the current K2 value in the Shuffle Map.
            using namespace std::placeholders;
            auto shuffleFind = std::bind(shuffleEquality, std::cref(item), _1);
            auto K2Iterator = std::find_if(shuffleItems.begin(),
                                           shuffleItems.end(), shuffleFind);

            // If the current Key is already in the Shuffle Map we just add it's
            // value to the proper position in the container. If AutoDelete flag
            // is true then we also need to release the resources of this
            // specific K2 because we don't store it in the Shuffle Map.
            if (K2Iterator != shuffleItems.end())
            {
                // Add the value to the existing K2 key.
                K2Iterator->second.push_back(item.second);
                // Release resources of the current unused K2 key.
                if (autoDeleteV2K2Flag)
                {
                    delete item.first;
                }
                continue;
            }

            // If the current Key is not in the Shuffle we add it with it's
            // corresponding V2.
            shuffleItems[item.first].push_back(item.second);
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

    // Attempt to read Chunk of items and perform Reduce.
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

        // If the vector is empty.
        if (startIterator == shuffleItems.end())
        {
            pthread_exit(nullptr);
        }

        // Perform Reduce on the shuffle chunk.
        for ( ; startIterator != endIterator; ++startIterator)
        {
            mapReduceDriver->Reduce(startIterator->first, startIterator->second);
        }
    }
}


/*-----=  Spawn Thread Functions  =-----*/


/**
 * @brief Spawn the Shuffle Thread by creating it with the given routine.
 * @param routine The start routine of the Threads.
 */
static void setupShuffleThread(threadRoutine routine)
{
    // Shuffle Thread creation and Semaphore initialization.
    if (sem_init(&shuffleSemaphore, false, SHUFFLE_SEMAPHORE_VALUE))
    {
        errorProcedure(SEM_INIT_NAME);
    }
    if (pthread_create(shuffleThread.getThread(), NULL, routine, NULL))
    {
        errorProcedure(PTHREAD_CREATE_NAME);
    }
}

/**
 * @brief Spawn the MapThreads by creating them with the given routine.
 *        The amount of created Threads is as given by the multiThreadLevel.
 * @param multiThreadLevel The number of Threads to spawn.
 * @param routine The start routine of the Threads.
 */
static void setupMapThreads(int const multiThreadLevel, threadRoutine routine)
{
    // Initialize the size of the Threads Vector to the given number of Threads.
    mapThreads = MapThreadsVector((unsigned long) multiThreadLevel);

    // Lock a Mutex in order to make a barrier for the Threads execution
    // and halt their progress until all Threads are created.
    if (pthread_mutex_lock(&threadSpawnMutex))
    {
        errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
    }

    // Create Threads.
    for (auto i = mapThreads.begin(); i != mapThreads.end(); ++i)
    {
        if (pthread_create(i->getThread(), NULL, routine, NULL))
        {
            errorProcedure(PTHREAD_CREATE_NAME);
        }
    }

    // Create Shuffle Thread.
    setupShuffleThread(shuffle);

    // Thread creation is done. Now the Threads can start run.
    if (pthread_mutex_unlock(&threadSpawnMutex))
    {
        errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
    }
}

/**
 * @brief Spawn the ReduceThreads by creating them with the given routine.
 *        The amount of created Threads is as given by the multiThreadLevel.
 * @param multiThreadLevel The number of Threads to spawn.
 * @param routine The start routine of the Threads.
 */
static void setupReduceThreads(int const multiThreadLevel, threadRoutine routine)
{
    // Initialize the size of the Threads Vector to the given number of Threads.
    reduceThreads = ReduceThreadsVector((unsigned long) multiThreadLevel);

    // Lock a Mutex in order to make a barrier for the Threads execution
    // and halt their progress until all Threads are created.
    if (pthread_mutex_lock(&threadSpawnMutex))
    {
        errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
    }

    // Create Threads.
    for (auto i = reduceThreads.begin(); i != reduceThreads.end(); ++i)
    {
        if (pthread_create(i->getThread(), NULL, routine, NULL))
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


/*-----=  Framework Result Functions  =-----*/


/**
 * @brief Comparator for sorting two pairs of type OUT_ITEM.
 *        The comparison is done by comparing the Key values of each
 *        pair, i.e. by comparing K3 values.
 * @param lhs The first pair to compare.
 * @param rhs The second pair to compare.
 * @return true if lhs is smaller then rhs, false otherwise.
 */
static bool sortPairs(const OUT_ITEM &lhs, const OUT_ITEM &rhs)
{
    assert(lhs.first != nullptr && rhs.first != nullptr);
    return *(lhs.first) < *(rhs.first);
}

/**
 * @brief Absorb all the items from the given source and append it into the
 *        given destination.
 * @param dest The destination Vector.
 * @param src The source Vector.
 */
static void absorbItems(OUT_ITEMS_VEC& dest, const OUT_ITEMS_VEC& src)
{
    dest.insert(dest.end(), src.begin(), src.end());
}

/**
 * @brief Create and finalize the final output of the Framework.
 *        This function receive all the output items from all of the Reduce
 *        Threads and merge it together into one final Vector. In addition
 *        it sorts the final Vector by alphabet order.
 * @return OUT_ITEMS_VEC holds the final output of the Framework sorted.
 */
static OUT_ITEMS_VEC finalizeOutput()
{
    OUT_ITEMS_VEC outputItems;  // The final output.

    // Insert all the data from each ReduceThread container and merge
    // it into the final output.
    for (auto i = reduceThreads.begin(); i != reduceThreads.end(); ++i)
    {
        absorbItems(outputItems, i->getReduceItems());
    }

    // Sort the output according to alphabet order.
    std::sort(outputItems.begin(), outputItems.end(), sortPairs);
    return outputItems;
}


/*-----=  Resource Release Functions  =-----*/


/**
 * @brief Destroy all the Mutex created during the Framework.
 */
static void destroyAllMutex()
{
    if (pthread_mutex_destroy(&threadSpawnMutex))
    {
        errorProcedure(PTHREAD_MUTEX_DESTROY_NAME);
    }

    if (pthread_mutex_destroy(&inputIndexMutex))
    {
        errorProcedure(PTHREAD_MUTEX_DESTROY_NAME);
    }

    if (pthread_mutex_destroy(&shuffleIteratorMutex))
    {
        errorProcedure(PTHREAD_MUTEX_DESTROY_NAME);
    }
}

/**
 * @brief Destroy all the Semaphores created during the Framework.
 */
static void destroyAllSemaphores()
{
    if (sem_destroy(&shuffleSemaphore))
    {
        errorProcedure(SEM_DESTROY_NAME);
    }
}

/**
 * @brief Release all resources of K2 and V2 in the Shuffle Map. Note that if
 *        some K2 did not enter the Shuffle Map, the Shuffle procedure already
 *        took care of it's resources.
 */
static void freeK2V2Items()
{
    pthread_mutex_lock(&printMutex);
    std::cout << "FREE" << std::endl;
    pthread_mutex_unlock(&printMutex);
    for (auto i = shuffleItems.begin(); i != shuffleItems.end(); ++i)
    {
        delete i->first;
//        i->first = nullptr;  // TODO: Check this.
        for (auto j = i->second.begin(); j != i->second.end(); ++j)
        {
            delete *j;
            *j = nullptr;
        }
    }
}


/*-----=  MapReduce Framework Functions  =-----*/


/**
 * @brief This function is called by the Map function in order to add a new
 *        pair of K2,V2 values. The function track the Thread which called it
 *        and find it's corresponding MapItems Vector to insert the given pair.
 * @param k2 The K2 value of the pair to add.
 * @param v2 The V2 value of the pair to add.
 */
void Emit2(k2Base *k2, v2Base *v2)
{
    // Search for the current Thread Object.
    pthread_t currentThread = pthread_self();
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

            return;
        }
    }
}

/**
 * @brief This function is called by the Reduce function in order to add a new
 *        pair of K3,V3 values. The function track the Thread which called it
 *        and find it's corresponding ReduceItems Vector to insert the pair.
 * @param k3 The K3 value of the pair to add.
 * @param v3 The V3 value of the pair to add.
 */
void Emit3(k3Base *k3, v3Base *v3)
{
    // Search for the current Thread Object.
    pthread_t currentThread = pthread_self();
    for (auto i = reduceThreads.begin(); i != reduceThreads.end(); ++i)
    {
        if (currentThread == *(i->getThread()))
        {
            // Insert to this Thread the current values of the Reduce procedure.
            OUT_ITEM reduceItem = std::make_pair(k3, v3);
            i->insertItem(reduceItem);
            return;
        }
    }
}

/**
 * @brief Run the MapReduce Framework on the given input with the given
 *        MapReduce implementation.
 * @param mapReduce The MapReduce object which holds MapReduce implementation.
 * @param itemsVec The input to perform Map & Reduce on.
 * @param multiThreadLevel The number of Threads to work with.
 * @param autoDeleteV2K2 A flag indicates if the K2V2 items resources should
 *                       be freed by the Framework.
 * @return Output Items Vector with the result of the MapReduce sorted.
 */
OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce,
                                    IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel,
                                    bool autoDeleteV2K2)
{
    mapReduceDriver = &mapReduce;  // Set the MapReduce specific implementation.
    inputItems = itemsVec;  // Set the Input Items.
    autoDeleteV2K2Flag = autoDeleteV2K2;  // Set the auto delete flag.

    // Spawn Threads for Map.
    setupMapThreads(multiThreadLevel, execMap);

    // Join the Map Threads.
    for (auto i = mapThreads.begin(); i != mapThreads.end(); ++i)
    {
        if (pthread_join(*(i->getThread()), NULL))
        {
            errorProcedure(PTHREAD_JOIN_NAME);
        }
    }


    // Indicate Shuffle that there are items to shuffle.
    if (sem_post(&shuffleSemaphore))
    {
        errorProcedure(SEM_POST_NAME);
    }

    // Join the Shuffle Thread.
    if (pthread_join(*(shuffleThread.getThread()), NULL))
    {
        errorProcedure(PTHREAD_JOIN_NAME);
    }

    pthread_mutex_lock(&printMutex);
    std::cout << "JOIN SHUFFLE" << std::endl;
    pthread_mutex_unlock(&printMutex);

    // Set the Shuffle iterator to the container begin.
    currentShuffleIterator = shuffleItems.begin();

    // Spawn Threads for Reduce.
    setupReduceThreads(multiThreadLevel, execReduce);

    // Join the Reduce Threads.
    for (auto i = reduceThreads.begin(); i != reduceThreads.end(); ++i)
    {
        if (pthread_join(*(i->getThread()), NULL))
        {
            errorProcedure(PTHREAD_JOIN_NAME);
        }
    }

    OUT_ITEMS_VEC frameworkOutput = finalizeOutput();

    // Release all Resources.
    destroyAllMutex();
    destroyAllSemaphores();
    if (autoDeleteV2K2Flag)
    {
        freeK2V2Items();
    }

    return frameworkOutput;
}
