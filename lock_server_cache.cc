// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
  pthread_create(&revoker, NULL, &lock_server_cache::Revoker_Helper, this);
  //pthread_create(&retrier, NULL, &lock_server_cache::Retrier_Helper, this);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  m_lock_map.lock();
  if (lock_client_map[lid].stat == unlock && !lock_client_map[lid].flag_revoke)
  {
    lock_client_map[lid].stat = lock;
    lock_client_map[lid].client_id = id;
    //tprintf("server grant lock to client %s\n", id.c_str());
    m_lock_map.unlock();
    return lock_protocol::OK;
  }    
  else 
  {
    m_lock_map.unlock();
    
    m_req_queue.lock();
    requests.push(lock_request(id, lid));
    //tprintf("lock in a client, send revoker\n");
    m_req_queue.unlock();
    cv_revoker.signal();
    return lock_protocol::RETRY;  
  }
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  m_lock_map.lock();
  lock_client_map[lid].stat = unlock;
  //tprintf("release:lock status is %d\n", lock_client_map[lid].stat)
  m_lock_map.unlock();
  cv_lock_map.signal();
  lock_protocol::status ret = lock_protocol::OK;
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

void*
lock_server_cache::Revoker_Function()
{
  while(1) 
  {
    m_req_queue.lock();
    while(requests.empty())
    {
      cv_revoker.wait(&m_req_queue); 
    }
    lock_protocol::lockid_t lid = requests.front().lid;
    std::string waiting_clt = requests.front().client_id;
    requests.pop();
    m_req_queue.unlock();
    
    m_lock_map.lock();
    std::string holding_clt = lock_client_map[lid].client_id;
    lock_client_map[lid].flag_revoke = true;
    m_lock_map.unlock();
    
        
    //tprintf("ready to send revoke RPC to client %s\n", holding_clt.c_str());
    handle h1(holding_clt);
    rpcc* cl_revoke = h1.safebind();
    if (cl_revoke) 
    {
      int r;
      //tprintf("send revoke RPC to client %s\n", holding_clt.c_str());
      rlock_protocol::status ret = cl_revoke->call(rlock_protocol::revoke,  lid, r);
      if (ret == rlock_protocol::RPCERR) 
      {
        tprintf("Revoke RPC failed.");
      }
      else 
      {
        m_lock_map.lock();
        //tprintf("revoke thread:lock status is %d\n", lock_client_map[lid].stat);
        while(lock_client_map[lid].stat == lock)
        {
          cv_lock_map.wait(&m_lock_map);
        } 
        lock_client_map[lid].stat = lock;
        lock_client_map[lid].client_id = waiting_clt;
        lock_client_map[lid].flag_revoke = false;
        //tprintf("revoke thread:lock status is %d\n", lock_client_map[lid].stat);
        m_lock_map.unlock();
      }
    }
    else 
    {
      tprintf("bind() failed");
    }
   
    handle h2(waiting_clt);
    rpcc* cl_retry = h2.safebind();
    if (cl_retry)
    { 
      int r;   
      //tprintf("send retry RPC to client %s\n", waiting_clt.c_str());
      rlock_protocol::status ret = cl_retry->call(rlock_protocol::retry,  lid, r);

      if (ret == rlock_protocol::RPCERR) 
      {
        tprintf("Revoke RPC failed.");
      }
    }
    else 
    {
      tprintf("bind() failed");
    }
  }
  return 0;
}

/*void*
lock_server_cache::Retrier_Function()
{
  while(1) 
  {
    m_req_queue.lock();
    while(requests.empty())
    {
      cv_retrier.wait(&m_req_queue); 
    }
    lock_protocol::lockid_t lid = requests.front().lid;
    std::string clt_id = requests.front().client_id;
    requests.pop();
    m_req_queue.unlock();
    
    tprintf("retrier thread send RPC to client %s", clt_id.c_str());
    handle h(clt_id);
    rpcc* cl = h.safebind();
    if (cl) 
    {
      int r;
      rlock_protocol::status ret = cl->call(rlock_protocol::retry,  lid, r);
      if (ret == rlock_protocol::RPCERR) 
      {
        tprintf("RPC failed.");
      }
    }
    else 
    {
      tprintf("bind() failed");
    }     
  }
  return 0;
}*/

void* 
lock_server_cache::Revoker_Helper(void* arg) 
{
  return reinterpret_cast<lock_server_cache*>(arg)->Revoker_Function();
}

/*void* 
lock_server_cache::Retrier_Helper(void* arg) 
{
  return reinterpret_cast<lock_server_cache*>(arg)->Retrier_Function();
}*/
