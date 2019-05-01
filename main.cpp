#include <boost/filesystem.hpp>
#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>
#include <pprint.hpp>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
namespace fs = boost::filesystem;

#define DEBUG false

/*
 *   Given a string value for the top level directory as input to your function
 *   and helper functions to get files/directories in a given directory (that
 *   you need to define, not code), write a function to return a list of all
 *   duplicate files in the file system. For example:
 *
 *   //Helper functions (need to be defined by you)
 *   List<String> getFiles(String directoryPath); //Returns all files directly
 *   under this directory
 *   List<String> getDirectories(String directoryPath); //Returns all…
 */

// Returns all files directly under this directory
std::vector<fs::path> getFiles(const std::string &directoryPath) {
  fs::path p(directoryPath);
  std::vector<fs::path> files;

  if (fs::is_directory(p)) {
    for (auto &&x : fs::directory_iterator(p)) {
      if (fs::is_regular_file(x))
        files.push_back(x.path());
    }

    std::sort(files.begin(), files.end());
  } else {
    std::ostringstream stringStream;
    stringStream << "Directory: " << directoryPath << " does not exist\n";
    std::string msg = stringStream.str();
    throw std::invalid_argument(msg);
  }

  return files;
}

// Returns all…
std::vector<fs::path> getDirectories(const std::string &directoryPath) {
  fs::path p(directoryPath);
  std::vector<fs::path> dirs;

  if (directoryPath == "") {
    return dirs;
  }

  if (!exists(p)) {
    std::ostringstream stringStream;
    stringStream << "Directory: " << directoryPath << " does not exist\n";
    std::string msg = stringStream.str();
    throw std::invalid_argument(msg);
  }

  if (fs::is_directory(p)) {
    for (auto &&x : fs::directory_iterator(p)) {
      if (fs::is_symlink(x)) {
        continue;
      }
      if (fs::is_directory(x))
        dirs.push_back(x.path());
    }

    // std::sort(dirs.begin(), dirs.end());
  }

  return dirs;
}

typedef uintmax_t filesize_t;
typedef std::size_t hash_t;

namespace std {
template <> struct hash<fs::path> {
  std::size_t operator()(const fs::path &k) const {
    using std::hash;
    using std::size_t;
    using std::string;

    return hash<string>()(k.string());
  }
};
} // namespace std

struct File {
  File(fs::path p) : path(p) {}
  fs::path path;
  bool operator<(const File &f) const {
    if (path < f.path) {
      return true;
    }
    return false;
  }
  bool operator==(const File &f) const {
    if (path == f.path) {
      return true;
    }
    return false;
  }
};

namespace std {
template <> struct hash<File> {
  std::size_t operator()(const File &k) const {
    using std::hash;
    using std::size_t;
    using std::string;

    std::ifstream file(k.path.string(), std::ios::binary);
    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());

    return hash<string>()(content);
  }
};
} // namespace std

static std::mutex screenLock;
static std::mutex dirSetMutex;
static std::mutex sizePathMapMutex;
static std::mutex dupesMutex;
static std::condition_variable cv;
class Crawler {
  std::vector<std::vector<fs::path>> _dupes;
  std::vector<std::thread> _threadPool;
  std::queue<fs::path> _dirSet;
  std::unordered_map<filesize_t, std::vector<fs::path>> _sizePathMap;
  // std::unordered_map<hash_t, std::vector<fs::path>> _hashPathMap;
  const int _max_workers;
  int _cur_workers;
  fs::path _rootDir;

public:
  Crawler(int num_workers, fs::path rootDir)
      : _max_workers(num_workers), _rootDir(rootDir) {
    _dirSet.push(rootDir);
  }

  void dirCrawler(const fs::path root) {
    if (DEBUG) {
      std::lock_guard sl(screenLock);
      std::cout << "Crawling: " << root << "\n";
    }

    // Push directories onto dirs
    std::vector<fs::path> dirs;
    for (const auto &d : getDirectories(root.string())) {
      if (DEBUG) {
        std::lock_guard sl(screenLock);
        std::cout << "d: " << d << "\n";
      }
      dirs.push_back(d);
    }

    dirSetMutex.lock();
    for (const auto &d : dirs) {
      _dirSet.push(d);
    }
    dirSetMutex.unlock();

    // Insert file sizes into sizePathmap
    std::vector<std::pair<filesize_t, fs::path>> filesizes;
    for (const auto &f : getFiles(root.string())) {
      auto fsz = fs::file_size(f);
      if (DEBUG) {
        std::lock_guard sl(screenLock);
        std::cout << "f: " << fsz << " : " << f << "\n";
      }
      filesizes.push_back({fs::file_size(f), f});
    }

    sizePathMapMutex.lock();
    for (const auto &d : filesizes) {
      _sizePathMap[std::get<0>(d)].push_back(std::get<1>(d));
    }
    sizePathMapMutex.unlock();
  }

  void Crawl() {
    // create array of dir crawlers
    while (true) {
      if (DEBUG) {
        std::lock_guard sl(screenLock);
        std::cout << _dirSet.front() << "\n";
      }

    threadcreate:
      while (!_dirSet.empty() && _cur_workers < _max_workers) {
        dirSetMutex.lock();
        auto dir = _dirSet.front();
        _dirSet.pop();
        dirSetMutex.unlock();
        _threadPool.push_back(std::thread(&Crawler::dirCrawler, this, dir));
        _cur_workers++;
      }

      // If all workers are being used, join a thread
      if (_cur_workers >= _max_workers) {
        for (auto &t : _threadPool) {
          if (t.joinable()) {
            t.join();
            _cur_workers--;
            goto threadcreate;
          }
        }
      }

      // If the dirset is empty wait for all threads to finish
      for (auto &t : _threadPool) {
        if (t.joinable()) {
          t.join();
          _cur_workers--;
        }
      }
      // If the dirset is still empty, we're done
      if (_dirSet.empty())
        return;
    }
  }

  void hashList(const std::vector<fs::path>& paths) {
    std::unordered_map<hash_t, std::vector<fs::path>> hashPathMap;
    // hash files into hashmap <hash, path>
    for (const auto &p : paths) {
      // get contents
      File f = File(p);
      hash_t hash = std::hash<File>()(f);
      hashPathMap[hash].push_back(p);
    }

    // for rows in hashmap that have more than one hash, push into dupes
    dupesMutex.lock();
    for (const auto&paths : hashPathMap) {
      if (paths.second.size() > 1) {
        _dupes.push_back(paths.second);
      }
    }
    dupesMutex.unlock();
  }

  void HashFiles() {
    // for rows in _sizePathMap
    for (const auto &paths : _sizePathMap) {
      // If row has more than one path
      if (paths.second.size() > 1) {
        // Spin off thread with it's own hashmap
        _threadPool.push_back(std::thread(&Crawler::hashList, this, paths.second));
      }
    }

    for (auto &t : _threadPool) {
      if (t.joinable()) {
        t.join();
      }
    }
  }

  void PrintStats() {
    // pprint::PrettyPrinter printer;
    std::cerr << "# Files: " << _sizePathMap.size() << '\n';
    // std::cerr << "# Possible Dupes: " << _hashPathMap.size() << '\n';
    std::cerr << "# Dupes: " << _dupes.size() << '\n';
    // printer.print(_sizePathMap);
    // printer.print(_hashPathMap);
    // printer.print(_dupes);
  }
};

// Producer Thread reads in directories into dirMap recursively
// Consumer Thread puts them into filesize_t, path multimap

int main([[maybe_unused]] int argc, char *argv[]) {
  const fs::path pwd(argv[1]);  // Input directory
  const int num_workers = 1000; // Max threads at any given time
  // Fan out workers per directory
  // Check Hash

  try {
    Crawler c = Crawler(num_workers, pwd);
    c.Crawl();
    c.HashFiles();
    c.PrintStats();
  } catch (const fs::filesystem_error &e) {
    std::cout << e.what() << '\n';
  } catch (const std::exception &e) {
    std::cout << e.what() << '\n';
  } catch (const std::out_of_range &e) {
    std::cerr << "Out of Range error: " << e.what() << '\n';
  } catch (...) {
    std::cout << "EX: " << '\n';
    throw;
  }

  return 0;
}
