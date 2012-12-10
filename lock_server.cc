// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int& r)
{
  m_.lock();
  unordered_map<lock_protocol::lockid_t, lock_status>::iterator it =
      lock_map_.find(lid);

  if (it == lock_map_.end()) {
    std::pair<lock_protocol::lockid_t, lock_status> new_lock(lid, locked);
    lock_map_.insert(new_lock);
    m_.unlock();
    return lock_protocol::OK;
  }
  else {
    if (it->second == free) {
      it->second = locked;
      m_.unlock();
      return lock_protocol::OK;
    }
    else {
      while (it->second == locked) {
        cv_.wait(&m_);
      }
      it->second = locked;
      m_.unlock();
      return lock_protocol::OK;
    }
  }
}


lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int& r) {
  m_.lock(); 
  unordered_map<lock_protocol::lockid_t, lock_status>::iterator it =
      lock_map_.find(lid);
  if (it == lock_map_.end()) {
    m_.unlock();
    return lock_protocol::NOENT;
  }
  else {
    if (it->second == free) {
      m_.unlock();
      return lock_protocol::NOENT;
    }
    else {
      it->second = free;
      cv_.signal();
      m_.unlock();
      return lock_protocol::OK;
    }
  }
}
