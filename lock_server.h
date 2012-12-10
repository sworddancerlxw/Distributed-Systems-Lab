// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include <tr1/unordered_map>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include "slock.h"

using std::tr1::unordered_map;

class lock_server {

 protected:
  int nacquire;

 private:
   Mutex m_;
   ConditionVar cv_;   

   enum lock_status { locked, free };
   unordered_map<lock_protocol::lockid_t, lock_status> lock_map_;
   
   
 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int&);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int&);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int&);
};

#endif 







