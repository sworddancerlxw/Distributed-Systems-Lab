#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include <vector>


class yfs_client {
  extent_client* ec;
  lock_client* lc;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
  std::vector<dirent> parser(std::string&);
  
 public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);
  int create(inum, const char*, inum&);
  int lookup(inum, const char*, inum&);
  int readdir(inum, std::vector<dirent>&);
  int setattr(inum, struct stat, struct stat*);
  int read(inum, size_t, struct stat, off_t, std::string&);
  int write(inum, const char*, struct stat, size_t, off_t);
  int mkdir(inum, const char*, inum&);
  int unlink(inum, const char*);
};

#endif 
