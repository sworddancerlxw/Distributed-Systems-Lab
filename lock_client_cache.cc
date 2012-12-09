// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "extent_client.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


lock_extent_releaser::lock_extent_releaser(extent_client* _ec)
    : ec(_ec) {}

void
lock_extent_releaser::dorelease(lock_protocol::lockid_t lid) 
{
  tprintf("call flush\n");
  ec->flush(lid);
}

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu), beStop(false)
{
  rpcs *rlsrpc = new rpcs(0);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  const char *hname;
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlsrpc->port();
  id = host.str();
  pthread_create(&Releaser, NULL, &lock_client_cache::Releaser_Helper, this);
}


lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int r = 0;
  
  //tprintf("waiting for m_lock_map\n");
  m_lock_map.lock();
  if (lock_map[lid].flag_revoke)
  { 
    while(!lock_map[lid].flag_revoke) 
    {
      cv2_.wait(&m_lock_map);
    }
    if (lock_map[lid].stat == none)
    {
      goto none_handler;
    }
    else if(lock_map[lid].stat == acquiring || lock_map[lid].stat == locked)
    {
      goto locked_handler;
    }
    else
    {
      lock_map[lid].stat = locked;
      lock_map[lid].ready_release = false;
      m_lock_map.unlock();
      //tprintf("client %d got lock\n", id.c_str());
      return lock_protocol::OK;
    }
  }
  else {
    if (lock_map[lid].stat == none)
    {
none_handler:
      lock_map[lid].stat = acquiring;
      m_lock_map.unlock();
      //tprintf("client %s acquire a lock from server\n", id.c_str());
      lock_protocol::status ret = cl->call(lock_protocol::acquire,  lid, id, r);
      if (ret == lock_protocol::RETRY) 
      {
        m_lock_map.lock();
        ++lock_map[lid].thread_counter;
        while(!lock_map[lid].flag_retry) 
        {
          cv1_.wait(&m_lock_map); 
        }
        lock_map[lid].ready_release = false;
        lock_map[lid].flag_retry = false;
        lock_map[lid].stat = locked;
        --lock_map[lid].thread_counter;
        m_lock_map.unlock();
        //tprintf("client %s got lock, when lock was none\n", id.c_str());
        return lock_protocol::OK;
      }
      else if (ret == lock_protocol::OK) 
      {
        m_lock_map.lock();
        lock_map[lid].ready_release = false;
        lock_map[lid].stat = locked;
        m_lock_map.unlock();
        //tprintf("client %s got lock, when lock was none\n", id.c_str());
        return lock_protocol::OK;
      }
      else 
      {
        return ret;
      }
    }
    else if (lock_map[lid].stat == free)
    {
      //tprintf("client %s acquire a local lock\n", id.c_str());
      lock_map[lid].stat = locked;
      lock_map[lid].ready_release = false;
      m_lock_map.unlock();
      //tprintf("client %s got lock, when lock was free\n", id.c_str());
      return lock_protocol::OK;
    }
    else // lock_map[lid] == acquiring or locked;
    {
locked_handler:
      //tprintf("client %s acquire a local lock\n", id.c_str());
      ++lock_map[lid].thread_counter;
      while (lock_map[lid].stat != free) 
      {
        cv3_.wait(&m_lock_map);
      }  
      lock_map[lid].stat = locked;
      lock_map[lid].ready_release = false;
      //tprintf("client %s got lock, when lock was locked\n", id.c_str());
      --lock_map[lid].thread_counter;
      m_lock_map.unlock();
      return lock_protocol::OK;
    }
  }
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{

  m_lock_map.lock();

  if (lock_map[lid].flag_revoke && lock_map[lid].thread_counter == 0) 
  {
    lock_map[lid].stat = releasing;
    lock_map[lid].ready_release = true;
    m_lock_map.unlock();
    //tprintf("call do release\n");
    //lu->dorelease(lid);
    cv_release.signal();
    return lock_protocol::OK;
  }
  else 
  {
    if (lock_map[lid].thread_counter == 0)
    {
      lock_map[lid].ready_release = true;
      lock_map[lid].stat = free;
      m_lock_map.unlock();
      return lock_protocol::OK;
    }
    else 
    {
      lock_map[lid].stat = free;
      m_lock_map.unlock();
      cv3_.signal();
      return lock_protocol::OK;
    }
  }
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  m_lock_map.lock();
  lock_map[lid].flag_revoke = true;
  m_lock_map.unlock();
  
  m_lock_queue.lock();
  releasing_locks.push(lid);
  m_lock_queue.unlock();
  cv_queue.signal();
  int ret = rlock_protocol::OK;
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  m_lock_map.lock();
  lock_map[lid].flag_retry = true;
  m_lock_map.unlock();  
  cv1_.signal();
  int ret = rlock_protocol::OK;
  return ret;
}



void*
lock_client_cache::Releaser_Function()
{
  while(1)
  { 
    m_lock_queue.lock();
    while(releasing_locks.empty())
    {
      cv_queue.wait(&m_lock_queue);
      if (beStop) 
      {
        m_lock_queue.unlock();
        break;
      }
    }
    lock_protocol::lockid_t lid = releasing_locks.front();
    //tprintf("ready to release lock %d\n", lid);
    releasing_locks.pop();
    m_lock_queue.unlock();

    m_lock_map.lock();
    VERIFY(lock_map[lid].stat != releasing || lock_map[lid].stat != free);
    while(!lock_map[lid].ready_release)
    {
      cv_release.wait(&m_lock_map);
    }
    /*if (lock_map[lid].stat == free )
    tprintf("%s release lock %d, lock status is free\n", id.c_str(), lid);
    if (lock_map[lid].stat == releasing )
    tprintf("%s release lock %d, lock status is releasing\n", id.c_str(), lid);
    */
    lock_map[lid].flag_revoke = false;
    lock_map[lid].ready_release = false;
    lock_map[lid].stat = none;
    cv2_.signal();
    m_lock_map.unlock();
    
    int r;
    lu->dorelease(lid);
    lock_protocol::status ret = cl->call(lock_protocol::release, lid, id, r); 
    VERIFY(ret == lock_protocol::OK);
  }
}

void 
lock_client_cache::stop()
{
  m_lock_queue.lock();
  beStop = true;
  cv_queue.signal();
  m_lock_queue.unlock();
  pthread_join(Releaser, NULL);
}

void* 
lock_client_cache::Releaser_Helper(void* arg)
{
  return reinterpret_cast<lock_client_cache*>(arg)->Releaser_Function();
}
