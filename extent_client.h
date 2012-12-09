// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include <map>
#include "extent_protocol.h"
#include "rpc.h"


class extent_client {
 private:
  rpcc *cl;
  Mutex m_;
  struct extent_attr {
    bool modified;
    std::string extent;
    extent_protocol::attr attr;
    extent_attr() : modified(false) {}
  }; 
  std::map<extent_protocol::extentid_t, extent_attr> extent_attr_cache; 
 
 public:
  extent_client(std::string dst);

  extent_protocol::status get(extent_protocol::extentid_t eid, 
			      std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				  extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  extent_protocol::status flush(extent_protocol::extentid_t eid);
};

#endif 

