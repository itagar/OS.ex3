// TODO: Valgrind
// TODO: README
// TODO: Makefile
// TODO: Check 'new' failure.
// TODO: Check if the Output is empty.

/**
 * @file Search.cpp
 * @author Itai Tagar <itagar>
 *
 * @brief A search program which uses the MapReduceFramework.
 *        The program searches all the given folders and prints to the screen
 *        all the files that contain the given substring in their name.
 */


/*-----=  Includes  =-----*/


#include <iostream>
#include <cassert>
#include <cstring>
#include <dirent.h>
#include <algorithm>
#include <typeinfo>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"


/*-----=  Definitions  =-----*/


/**
 * @def EMPTY_ARGUMENTS 1
 * @brief A Macro that sets the number for empty arguments count.
 */
#define EMPTY_ARGUMENTS 1

/**
 * @def SUBSTRING_ARGUMENT_INDEX 1
 * @brief A Macro that sets the index of the substring argument to search.
 */
#define SUBSTRING_ARGUMENT_INDEX 1

/**
 * @def DIRECTORY_START_INDEX 2
 * @brief A Macro that sets the index of the first directory argument.
 */
#define DIRECTORY_START_INDEX 2

/**
 * @def MULTI_THREAD_LEVEL 5
 * @brief A Macro that sets the multi thread level.
 */
#define MULTI_THREAD_LEVEL 5

/**
 * @def USAGE_MSG "Usage: <substring to search> <folders, separated by space>"
 * @brief A Macro that sets the error message when the usage is invalid.
 */
#define USAGE_MSG "Usage: <substring to search> <folders, separated by space>"

/**
 * @def SELF_DIRECTORY_NAME "."
 * @brief A Macro that sets the file name represent the directory which store it.
 */
#define SELF_DIRECTORY_NAME "."

/**
 * @def PARENT_DIRECTORY_NAME "."
 * @brief A Macro that sets the file name represent the parent directory of it.
 */
#define PARENT_DIRECTORY_NAME ".."

/**
 * @def EMPTY_CHAR ""
 * @brief A Macro that sets the empty character string representation.
 */
#define EMPTY_CHAR ""

/**
 * @def WHITE_SPACE " "
 * @brief A Macro that sets the white space string representation.
 */

#define WHITE_SPACE " "


/*-----=  Type Definitions  =-----*/


/**
 * @brief Type Definition for directory name.
 */
typedef std::string dirName_t;

/**
 * @brief Type Definition for file name.
 */
typedef std::string fileName_t;

/**
 * @brief Type Definition for files vector.
 */
typedef std::vector<fileName_t> filesVector;


/*-----=  Client Classes  =-----*/


/**
 * @brief The K1 object for the MapReduceFramework of the Search process.
 *        K1Search object represent a directory to search in a given substring.
 */
class k1Search : public k1Base
{
public:
    /**
     * @brief A Constructor for the K1 Class.
     * @param directoryName The current directory representing this K1 object.
     */
    k1Search(const dirName_t &directoryName) : _directoryName(directoryName) {};

    /**
     * @brief An implementation of comparison operation for the K1 objects.
     *        The comparison is done by lexicographic order of the
     *        directory name of the given K1 object.
     * @param other The other K1 object to compare.
     * @return true if this is lower then other, false otherwise.
     */
    bool operator<(const k1Base &other) const override
    {
        const k1Search &searchOther = dynamic_cast<const k1Search &>(other);
        return _directoryName.compare(searchOther.getDirectoryName()) < 0;
    };

    /**
     * @brief A Destructor for this K1 class.
     */
    ~k1Search() override {};

    /**
     * @brief Gets the directory name of this K1 object.
     * @return The directory name of this K1 object.
     */
    dirName_t getDirectoryName() const { return _directoryName; };

private:
    /**
     * @brief The directory name associated with this K1 object.
     */
    dirName_t _directoryName;
};

/**
 * @brief The V1 object for the MapReduceFramework of the Search process.
 *        V1Search object represent the given substring to search.
 */
class v1Search : public v1Base
{
public:
    /**
     * @brief A Constructor for the V1 Class.
     * @param substring The given substring to search.
     */
    v1Search(const std::string &substring) : _substring(substring) {};

    /**
     * @brief A Destructor for this V1 class.
     */
    ~v1Search() override {};

    /**
     * @brief Gets the substring to search that associated with this V1 object.
     * @return The substring to search that associated with this V1 object.
     */
    std::string getSubstring() const { return _substring; };

private:
    /**
     * @brief The substring to search associated with this V1 object.
     */
    std::string _substring;
};

/**
 * @brief The K2 object for the MapReduceFramework of the Search process.
 *        K2Search object represent a file name in a given directory from
 *        the Search program arguments.
 */
class k2Search : public k2Base
{
public:
    /**
     * @brief A Constructor for the K2 Class.
     * @param fileName The current file name representing this K2 object.
     */
    k2Search(fileName_t fileName) : _fileName(fileName) {};

    /**
     * @brief An implementation of comparison operation for the K2 objects.
     *        The comparison is done by lexicographic order of the
     *        file name of the given K2 object.
     * @param other The other K2 object to compare.
     * @return true if this is lower then other, false otherwise.
     */
    bool operator<(const k2Base &other) const override
    {
        const k2Search &searchOther = dynamic_cast<const k2Search &>(other);
        return _fileName.compare(searchOther.getFileName()) < 0;
    };

    /**
     * @brief A Destructor for this K2 class.
     */
    ~k2Search() override {};

    /**
     * @brief Gets the file name associated with this K2 object.
     * @return The file name associated with this K2 object.
     */
    fileName_t getFileName() const { return _fileName; };

private:
    /**
     * @brief The file name associated with this K2 object.
     */
    fileName_t _fileName;
};

/**
 * @brief The K3 object for the MapReduceFramework of the Search process.
 *        K3Search object represent a file name in a given directory from
 *        the Search program arguments which satisfies the search conditions.
 */
class k3Search : public k3Base
{
public:
    /**
     * @brief A Constructor for the K3 Class.
     * @param fileName The current file name representing this K3 object.
     */
    k3Search(fileName_t fileName) : _fileName(fileName) {};

    /**
     * @brief An implementation of comparison operation for the K3 objects.
     *        The comparison is done by lexicographic order of the
     *        file name of the given K3 object.
     * @param other The other K3 object to compare.
     * @return true if this is lower then other, false otherwise.
     */
    bool operator<(const k3Base &other) const override
    {
        const k3Search &searchOther = dynamic_cast<const k3Search &>(other);
        return _fileName.compare(searchOther.getFileName()) < 0;
    };

    /**
     * @brief A Destructor for this K3 class.
     */
    ~k3Search() override {};

    /**
     * @brief Gets the file name associated with this K3 object.
     * @return The file name associated with this K3 object.
     */
    fileName_t getFileName() const { return _fileName; };

private:
    /**
     * @brief The file name associated with this K3 object.
     */
    fileName_t _fileName;
};

/**
 * @brief The MapReduce Class for the Search program.
 */
class MapReduceSearch : public MapReduceBase
{
public:
    /**
     * @brief The Map function. It takes a key and value where key is the
     *        directory name and value is the substring to search.
     *        The Map function then iterates through all the files in the
     *        directory and Emit only files which contain the substring
     *        to search in their names.
     * @param key The K1 object which in this case is a directory name.
     * @param val The V1 object which in this case is a substring.
     */
    void Map(const k1Base *const key, const v1Base *const val) const override
    {
        // Perform Down-Casting for K1 and V1 in order to use Map with the
        // proper Search program objects.
        const k1Search *currentKey = dynamic_cast<const k1Search * const>(key);
        const v1Search *currentVal = dynamic_cast<const v1Search * const>(val);
        assert(currentKey != nullptr && currentVal != nullptr);

        // The vector which will contain all the files in this directory.
        filesVector files;

        const dirName_t currentDirectory = currentKey->getDirectoryName();
        const std::string substring = currentVal->getSubstring();

        // Setup the files vector with all the files in the given directory.
        _setupFiles(currentDirectory, files);

        // Emit files from this directory which contains the substring
        // to search in their names.
        for (auto i = files.begin(); i != files.end(); ++i)
        {
            fileName_t currentFileName = *i;
            if (currentFileName.find(substring) != std::string::npos)
            {
                // If the file name contains the substring to search.
                k2Search *currentK2 = new k2Search(currentFileName);  // TODO: new might fail.
                Emit2(currentK2, nullptr);
            }
        }
    }

    /**
     * @brief The Reduce function. It takes a key and a vector of values
     *        where key is the file name and values is a vector of size of
     *        the amount of appearances of the corresponding file name.
     *        The Reduce function then Emit the file name as much as
     *        the size of the vector of values.
     * @param key The K2 object which in this case is a file name.
     * @param vals The vector of V2 objects which in this case is a vector of
     *        null pointers and only it's size matters.
     */
    void Reduce(const k2Base *const key, const V2_VEC &vals) const override
    {
        // Perform Down-Casting for K2 to use Reduce with the
        // proper Search program objects.
        const k2Search *currentKey = dynamic_cast<const k2Search * const>(key);
        assert(currentKey != nullptr);

        fileName_t currentFileName = currentKey->getFileName();

        // For each filename, Emit each instance of it according to the size
        // of it's values vector, if the size is n then this file name appeared
        // n times during the Search process.
        for (unsigned int i = 0; i < vals.size(); ++i)
        {
            k3Search *currentK3 = new k3Search(currentFileName);  // TODO: new might fail.
            Emit3(currentK3, nullptr);
        }
    }

private:
    /**
     * @brief Setup the files vector with all the files in the given directory.
     *        if the given directory is empty or is not a directory, the vector
     *        will stay empty.
     * @param directory The given directory.
     * @param files The files vector to fill with file names.
     */
    void _setupFiles(dirName_t const directory, filesVector &files) const
    {
        DIR *pDir = NULL;
        struct dirent *pEnt = NULL;
        pDir = opendir(directory.c_str());  // Open Directory Stream.
        if (pDir != NULL)
        {
            // If this is a valid directory.
            while ((pEnt = readdir(pDir)) != NULL)
            {
                // Receive the next file name in this directory.
                fileName_t currentFileName = pEnt->d_name;

                // If current file name is equal to "." or ".." we skip it.
                if (!currentFileName.compare(SELF_DIRECTORY_NAME) ||
                    !currentFileName.compare(PARENT_DIRECTORY_NAME))
                {
                    continue;
                }

                // Insert this file name into the given files vector.
                files.push_back(currentFileName);
            }
        }
        closedir(pDir);  // Close Directory Stream.
    }
};


/*-----=  Search Implementation  =-----*/


/**
 * @brief Checks whether the program received the desired number of arguments.
 *        In case of invalid arguments count the function output an error
 *        message specifying the correct and exit.
 * @param argc The number of arguments given to the program.
 */
void checkArguments(int const argc)
{
    if (argc <= EMPTY_ARGUMENTS)
    {
        std::cerr << USAGE_MSG << std::endl;
        exit(EXIT_FAILURE);
    }
}

/**
 * @brief Comparator for sorting two pairs of OUT_ITEM in the Search program.
 *        The comparison is done by comparing the Key values of each
 *        pair, i.e. by comparing K3 values.
 * @param lhs The first pair to compare.
 * @param rhs The second pair to compare.
 * @return true if lhs is smaller then rhs, false otherwise.
 */
bool SearchPairSort(const OUT_ITEM &lhs, const OUT_ITEM &rhs)
{
    assert(lhs.first != nullptr && rhs.first != nullptr);
    return *(lhs.first) < *(rhs.first);
}

/**
 * @brief produce the output results of the Search process to the user.
 * @param output The output vector obtained by the MapReduce Framework.
 */
void outputProcedure(OUT_ITEMS_VEC &output)
{
    // Sort the output vector according to alphabet order.
    std::sort(output.begin(), output.end(), SearchPairSort);

    // Print all the files of the result output.
    for (auto i = output.begin(); i != output.end(); ++i)
    {
        k3Search *currentK3 = dynamic_cast<k3Search *>(i->first);

        std::cout << (i == output.begin() ? EMPTY_CHAR : WHITE_SPACE);

        assert(currentK3 != nullptr);
        std::cout << currentK3->getFileName();
    }
}

/**
 * @brief Free all resources allocated for input vector and for output vector.
 * @param input The input vector which contains pointers to allocated objects
 *        of K1 and V1 pairs.
 * @param output The output vector which contains pointers to allocated objects
 *        of K3 and V3 pairs.
 */
void freeResources(IN_ITEMS_VEC &input, OUT_ITEMS_VEC &output)
{
    // Free all resources for objects in the Input Vector.
    for (auto i = input.begin(); i != input.end(); ++i)
    {
        delete i->first;
        i->first = nullptr;
        delete i->second;
        i->second = nullptr;
    }

    // Free all resources for objects in the Output Vector.
    for (auto i = output.begin(); i != output.end(); ++i)
    {
        delete i->first;
        i->first = nullptr;
        delete i->second;
        i->second = nullptr;
    }
}

/**
 * @brief The Main function that runs the Search program.
 * @param argc The number of arguments received.
 * @param argv The given arguments for this program.
 */
int main(int argc, char* argv[])
{
    // Check for valid arguments count.
    checkArguments(argc);

    // Initialize the data for running this Search process.
    MapReduceSearch mapReduceSearch;
    IN_ITEMS_VEC input;
    OUT_ITEMS_VEC output;

    try
    {
        // Set the substring to be the value of the desired substring to search and
        // create a matching V1 object associated with this substring.
        assert(argc > SUBSTRING_ARGUMENT_INDEX);
        const std::string substring = argv[SUBSTRING_ARGUMENT_INDEX];
        v1Search *currentV1 = new v1Search(substring);

        // Iterate the given arguments which represent directories to search in.
        for (int i = DIRECTORY_START_INDEX; i < argc; ++i)
        {
            // Create a K1 object representing the current directory.
            dirName_t currentDirectory = argv[i];
            k1Search *currentK1 = new k1Search(argv[i]);

            // Create the Input Pair which holds a pointer to the current K1 of the
            // directory name and a pointer to V1 of the substring to search and
            // store it in the Input Vector.
            assert(currentV1 != nullptr && currentK1 != nullptr);
            input.push_back(std::make_pair(currentK1, currentV1));
        }

        // Perform the MapReduce Process.
        output = RunMapReduceFramework(mapReduceSearch, input,
                                       MULTI_THREAD_LEVEL, true);

        // Output to the Standard Output the results.
        outputProcedure(output);

        // Free all resources.
        freeResources(input, output);
        return EXIT_SUCCESS;
    }
    catch (std::bad_alloc &badAllocException)
    {
        // Free all resources.
        freeResources(input, output);
        return EXIT_FAILURE;
    }
}

