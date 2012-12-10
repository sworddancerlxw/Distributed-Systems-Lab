// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"
#include "slock.h"


class extent_server {

 public:
  extent_server();

  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);

 private:
  
  typedef std::map<extent_protocol::extentid_t, std::string> StringMap;
  typedef std::map<extent_protocol::extentid_t, extent_protocol::attr> AttrMap;
  
  StringMap contents_;
  AttrMap attrs_;
  Mutex m_;
};

#endif 







