#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <map>
#include <queue>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

class lock_server_cache {
 private:
  enum lock_status { lock, unlock };
  struct lock_state {
    std::string client_id;
    bool flag_revoke;
    lock_status stat;
    lock_state() : flag_revoke(false), stat(unlock) {}
  };

  struct lock_request {
    std::string client_id;
    lock_protocol::lockid_t lid;
    lock_request(std::string id, lock_protocol::lockid_t lock_id) : client_id(id), lid(lock_id) {}
  };
  int nacquire;

  std::map<lock_protocol::lockid_t, lock_state> lock_client_map;
  std::queue<lock_request> requests;

  pthread_t revoker, retrier;

  Mutex m_req_queue, m_lock_map;
  ConditionVar cv_revoker, cv_retrier, cv_lock_map;

  void* Revoker_Function();
  void* Retrier_Function();
  static void* Revoker_Helper(void*);
  static void* Retrier_Helper(void*);
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
