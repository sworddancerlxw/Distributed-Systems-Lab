// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include <queue>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.

class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {}
};

class lock_extent_releaser : public lock_release_user {
private:
  class extent_client* ec;
public:
  lock_extent_releaser(class extent_client* _ec);
  virtual void dorelease(lock_protocol::lockid_t);
};
   

class lock_client_cache : public lock_client {
 private:
  enum lock_status { none, free, locked, acquiring, releasing };
  struct lock_state {
    lock_status stat;
    bool flag_revoke;
    bool flag_retry;
    bool ready_release;
    size_t thread_counter;
    lock_state() : stat(none), flag_revoke(false), flag_retry(false), ready_release(false), thread_counter(0) {}
  };
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  bool beStop;
 
  std::map<lock_protocol::lockid_t, struct lock_state> lock_map;
  std::queue<lock_protocol::lockid_t> releasing_locks;
  Mutex m_lock_map, m_lock_queue;
  ConditionVar cv1_, cv2_, cv3_, cv_queue, cv_release;
  pthread_t Releaser;
  
  void stop();
  void* Releaser_Function();
  static void* Releaser_Helper(void* arg);

 public:
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() { stop(); }
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
                                        int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
                                       int &);
};


#endif
