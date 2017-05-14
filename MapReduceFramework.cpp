// TODO: Valgrind
// TODO: README
// TODO: Makefile
// TODO: Implement Log File.
// TODO: SET CHUNK SIZE TO 10.
// TODO: Check Destructors for the Threads.
// TODO: Split to helper functions.
// TODO: Check empty input. and check if threads should be created.
// TODO: Add '.' to the log name.
// TODO: Flag for Emit2 before semaphore post.
// TODO: Fix Changes of mutex and check all error.
// TODO: Fix the Shuffle.

/**
 * @file MapReduceFramework.cpp
 * @author Itai Tagar <itagar>
 *
 * @brief An implementation of the Map Reduce Framework.
 */


/*-----=  Includes  =-----*/


#include <iostream>
#include <fstream>
#include <pthread.h>
#include <semaphore.h>
#include <map>
#include <algorithm>
#include <cassert>
#include <functional>
#include <iomanip>
#include <stdlib.h>
#include <sys/time.h>
#include "MapReduceFramework.h"
#include "Thread.h"
#include "MapThread.h"
#include "ReduceThread.h"


/*-----=  Definitions  =-----*/


/**
 * @def INITIAL_INPUT_INDEX 0
 * @brief A Macro that sets the value of initial index in the index counters.
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
* @def MICRO_TO_NANO_FACTOR 1000
* @brief A Macro that sets the factor to convert micro-seconds to nano-seconds.
*/
# define MICRO_TO_NANO_FACTOR 1000

/**
* @def SECONDS_TO_NANO_FACTOR 1000000000
* @brief A Macro that sets the factor to convert seconds to nano-seconds.
*/
# define SECONDS_TO_NANO_FACTOR 1000000000

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

/**
 * @def GETTIMEOFDAY_NAME "gettimeofday"
 * @brief A Macro that sets function name for gettimeofday.
 */
#define GETTIMEOFDAY_NAME "gettimeofday"

/**
 * @def LOG_FILE_NAME ".MapReduceFramework.log"
 * @brief A Macro that sets the name of the Log File.
 */
#define LOG_FILE_NAME "MapReduceFramework.log"


/*-----=  Type Definitions  =-----*/


/**
 * @brief Functor for the Shuffle Comparator.
 */
typedef struct ShuffleComparator
{
    /**
     * @brief Comparator for the Shuffle Map.
     * @param lhs The first item to compare.
     * @param rhs The second item to compare.
     * @return true if lhs is smaller then rhs, false otherwise.
     */
    bool operator()(k2Base * const &lhs, k2Base * const &rhs)
    {
        return *lhs < *rhs;
    };

} ShuffleComparator;

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
 * @brief Type Definition for the map of Shuffle items.
 */
typedef std::map<k2Base*, V2Vector, ShuffleComparator> SHUFFLE_ITEMS;

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

// TODO: Doxygen.
bool mapsDone;

/**
 * @brief The Log File of this Framework.
 */
std::ofstream logFile;


/*-----=  Mutex & Semaphore  =-----*/


/**
 * @brief Mutex for Spawn Threads.
 */
pthread_mutex_t threadSpawnMutex;

/**
 * @brief Mutex for accessing the Input Items.
 */
pthread_mutex_t inputIndexMutex;

/**
 * @brief Mutex for accessing the Shuffle Items.
 */
pthread_mutex_t shuffleIteratorMutex;

/**
 * @brief Mutex for writing to the Log File.
 */
pthread_mutex_t logMutex;

// TODO: Doxygen.
pthread_mutex_t mapsDoneMutex;

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


/*-----=  Log Functions  =-----*/


/**
 * @brief Initilize the Log File and write the opening line in it.
 * @param multiThreadLevel
 */
static void initLogFile(const int multiThreadLevel)
{
    logFile.open(LOG_FILE_NAME, std::ios_base::app | std::ios_base::out);
    logFile << "RunMapReduceFramework started with "
            << multiThreadLevel
            << " threads\n";
}

/**
 * @brief Write to the Log file the current time and date.
 */
static void getTimeAndDate()
{
    time_t now = time(0);
    tm *currentTime = localtime(&now);

    int day = currentTime->tm_mday;
    int month = currentTime->tm_mon + 1;
    int year = currentTime->tm_year + 1900;

    int hour = currentTime->tm_hour;
    int min = currentTime->tm_min;
    int sec = currentTime->tm_sec;

    logFile << "[" << std::setfill('0') << std::setw(2) << day
            << "." << std::setfill('0') << std::setw(2) << month
            << "." << std::setfill('0') << std::setw(2) << year
            << " " << std::setfill('0') << std::setw(2) << hour
            << ":" << std::setfill('0') << std::setw(2) << min
            << ":" << std::setfill('0') << std::setw(2) << sec
            << "]";
}

/**
 * @brief Writes to the Log file creation of a Thread.
 * @param threadName The name of the created Thread.
 */
static void logThreadCreate(const std::string &threadName)
{
    logFile << "Thread " << threadName << " created ";
    getTimeAndDate();
    logFile << "\n";
}

/**
 * @brief Writes to the Log file termination of a Thread.
 * @param threadName The name of the terminated Thread.
 */
static void logThreadTerminate(const std::string &threadName)
{
    logFile << "Thread " << threadName << " terminated ";
    getTimeAndDate();
    logFile << "\n";
}

/**
 * @brief Calculates the time elapsed from the given start timestamp to
 *        the end timestamp in nano-seconds.
 * @param start The start timestamp.
 * @param end The end timestamp.
 * @return The time elapsed in nano-seconds from start to end.
 */
static double calculateTime(const timeval &start, const timeval &end)
{
    double seconds = (end.tv_sec - start.tv_sec) * SECONDS_TO_NANO_FACTOR;
    double microSeconds = (end.tv_usec - start.tv_usec) * MICRO_TO_NANO_FACTOR;
    return seconds + microSeconds;
}

/**
 * @brief Writes to the Log file the elapsed time of Map and Shuffle.
 * @param start Start time.
 * @param end End Time.
 */
static void logMapShuffleTime(const timeval &start, const timeval &end)
{
    logFile << "Map and Shuffle took " << calculateTime(start, end) << " ns\n";
}

/**
 * @brief Writes to the Log file the elapsed time of Reduce.
 * @param start Start time.
 * @param end End Time.
 */
static void logReduceTime(const timeval &start, const timeval &end)
{
    logFile << "Reduce took " << calculateTime(start, end) << " ns\n";
}

/**
 * @brief Wrtes to the Log file the final line and close the file stream.
 */
static void finishLogFile()
{
    logFile << "RunMapReduceFramework finished\n";
    logFile.close();
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
            if (pthread_mutex_lock(&logMutex))
            {
                errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
            }
            logThreadTerminate("ExecMap");
            if (pthread_mutex_unlock(&logMutex))
            {
                errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
            }
            pthread_exit(nullptr);
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

        if (pthread_mutex_lock(&mapsDoneMutex))
        {
            errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
        }
        if (mapsDone)
        {
            if (pthread_mutex_unlock(&mapsDoneMutex))
            {
                errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
            }
            pthread_exit(nullptr);
        }
        if (pthread_mutex_unlock(&mapsDoneMutex))
        {
            errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
        }

        // Wait for items to work with.
        if (sem_wait(&shuffleSemaphore))
        {
            errorProcedure(SEM_WAIT_NAME);
        }

        // Iterate through MapThreads and check which has a non-empty container.
        for (auto i = mapThreads.begin(); i != mapThreads.end(); ++i)
        {
            if (!(i->mapItems.empty()))
            {
                // Found a Thread with items to shuffle.
                // Attempt to lock this Thread MapItems Vector because it's
                // shared by this Thread and the Shuffle Thread.
                if (pthread_mutex_lock(&i->mapMutex))
                {
                    errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
                }

                // Absorb all the items from this Thread and clear it's Vector.
                itemsToShuffle = i->mapItems;
                i->mapItems.clear();

                // Unlock this Thread MapItems Vector.
                if (pthread_mutex_unlock(&i->mapMutex))
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
            auto K2Iterator = shuffleItems.find(i->first);

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
            if (pthread_mutex_lock(&logMutex))
            {
                errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
            }
            logThreadTerminate("ExecReduce");
            if (pthread_mutex_unlock(&logMutex))
            {
                errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
            }
            pthread_exit(nullptr);
        }

        // Perform Reduce on the shuffle chunk.
        for ( ; startIterator != endIterator; ++startIterator)
        {
            mapReduceDriver->Reduce(startIterator->first, startIterator->second);
        }
    }
}


/*-----=  Spawn & Join Thread Functions  =-----*/


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
    if (pthread_create(&shuffleThread.thread, NULL, routine, NULL))
    {
        errorProcedure(PTHREAD_CREATE_NAME);
    }
    if (pthread_mutex_lock(&logMutex))
    {
        errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
    }
    logThreadCreate("Shuffle");
    if (pthread_mutex_unlock(&logMutex))
    {
        errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
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
        if (pthread_create(&(i->thread), NULL, routine, NULL))
        {
            errorProcedure(PTHREAD_CREATE_NAME);
        }
        if (pthread_mutex_lock(&logMutex))
        {
            errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
        }
        logThreadCreate("ExecMap");
        if (pthread_mutex_unlock(&logMutex))
        {
            errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
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
        if (pthread_create(&(i->thread), NULL, routine, NULL))
        {
            errorProcedure(PTHREAD_CREATE_NAME);
        }
        if (pthread_mutex_lock(&logMutex))
        {
            errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
        }
        logThreadCreate("ExecReduce");
        if (pthread_mutex_unlock(&logMutex))
        {
            errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
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
        absorbItems(outputItems, i->reduceItems);
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

    if (pthread_mutex_destroy(&logMutex))
    {
        errorProcedure(PTHREAD_MUTEX_DESTROY_NAME);
    }

    if (pthread_mutex_destroy(&mapsDoneMutex))
    {
        errorProcedure(PTHREAD_MUTEX_DESTROY_NAME);
    }

    for (auto i = mapThreads.begin(); i != mapThreads.end(); ++i)
    {
        if (pthread_mutex_destroy(&i->mapMutex))
        {
            errorProcedure(PTHREAD_MUTEX_DESTROY_NAME);
        }
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
    for (auto i = shuffleItems.begin(); i != shuffleItems.end(); ++i)
    {
        delete i->first;
        for (auto j = i->second.begin(); j != i->second.end(); ++j)
        {
            delete *j;
            *j = nullptr;
        }
    }
}

/**
 * @brief Release all resources of the Framework.
 */
static void releaseAllResources()
{
    destroyAllMutex();
    destroyAllSemaphores();
    if (autoDeleteV2K2Flag)
    {
        freeK2V2Items();
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
        if (currentThread == i->thread)
        {
            // Attempt to lock this Thread MapItems Vector because it's
            // shared by this Thread and the Shuffle Thread.
            if (pthread_mutex_lock(&i->mapMutex))
            {
                errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
            }

            // Insert to this Thread the current values of the Map procedure.
            i->mapItems.push_back(std::make_pair(k2, v2));

            // Unlock this Thread MapItems Vector.
            if (pthread_mutex_unlock(&i->mapMutex))
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
        if (currentThread == i->thread)
        {
            // Insert to this Thread the current values of the Reduce procedure.
            OUT_ITEM reduceItem = std::make_pair(k3, v3);
            i->reduceItems.push_back(reduceItem);
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
    // Init the Timestamps for the Log measurements.
    timeval startMapShuffle, endMapShuffle;
    timeval startReduce, endReduce;

    // Start Measuring the time of Map & Shuffle.
    if (gettimeofday(&startMapShuffle, nullptr))
    {
        errorProcedure(GETTIMEOFDAY_NAME);
    }

    initLogFile(multiThreadLevel);

    mapReduceDriver = &mapReduce;  // Set the MapReduce specific implementation.
    inputItems = itemsVec;  // Set the Input Items.
    autoDeleteV2K2Flag = autoDeleteV2K2;  // Set the auto delete flag.
    mapsDone = false;

    pthread_mutex_init(&threadSpawnMutex, nullptr);
    pthread_mutex_init(&inputIndexMutex, nullptr);
    pthread_mutex_init(&shuffleIteratorMutex, nullptr);
    pthread_mutex_init(&logMutex, nullptr);
    pthread_mutex_init(&mapsDoneMutex, nullptr);

    for (auto i = mapThreads.begin(); i != mapThreads.end(); ++i)
    {
        pthread_mutex_init(&i->mapMutex, nullptr);
    }

    // Spawn Threads for Map.
    setupMapThreads(multiThreadLevel, execMap);

    // Join the Map Threads.
    for (auto i = mapThreads.begin(); i != mapThreads.end(); ++i)
    {
        if (pthread_join(i->thread, NULL))
        {
            errorProcedure(PTHREAD_JOIN_NAME);
        }
    }

    if (pthread_mutex_lock(&mapsDoneMutex))
    {
        errorProcedure(PTHREAD_MUTEX_LOCK_NAME);
    }
    mapsDone = true;
    if (pthread_mutex_unlock(&mapsDoneMutex))
    {
        errorProcedure(PTHREAD_MUTEX_UNLOCK_NAME);
    }

    // Indicate Shuffle that all Threads finished.
    if (sem_post(&shuffleSemaphore))
    {
        errorProcedure(SEM_POST_NAME);
    }

    // Join the Shuffle Thread.
    if (pthread_join(shuffleThread.thread, NULL))
    {
        errorProcedure(PTHREAD_JOIN_NAME);
    }

    // Stop Measuring the time of Map & Shuffle.
    if (gettimeofday(&endMapShuffle, nullptr))
    {
        errorProcedure(GETTIMEOFDAY_NAME);
    }

    // Output to the Log File the elapsed time of Map & Shuffle.
    logMapShuffleTime(startMapShuffle, endMapShuffle);

    // Set the Shuffle iterator to the container begin.
    currentShuffleIterator = shuffleItems.begin();

    // Start Measuring the time of Map & Shuffle.
    if (gettimeofday(&startReduce, nullptr))
    {
        errorProcedure(GETTIMEOFDAY_NAME);
    }

    // Spawn Threads for Reduce.
    setupReduceThreads(multiThreadLevel, execReduce);

    // Join the Reduce Threads.
    for (auto i = reduceThreads.begin(); i != reduceThreads.end(); ++i)
    {
        if (pthread_join(i->thread, NULL))
        {
            errorProcedure(PTHREAD_JOIN_NAME);
        }
    }

    OUT_ITEMS_VEC frameworkOutput = finalizeOutput();

    // Stop Measuring the time of Map & Shuffle.
    if (gettimeofday(&endReduce, nullptr))
    {
        errorProcedure(GETTIMEOFDAY_NAME);
    }

    // Output to the Log File the elapsed time of Reduce.
    logReduceTime(startReduce, endReduce);

    // Release all Resources.
    releaseAllResources();

    finishLogFile();

    return frameworkOutput;
}
