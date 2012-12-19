// the caching lock server implementation

#include "lock_server_cache_rsm.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"



lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm) 
  : rsm (_rsm), server_xid(0), duplicate_flag(false)
{
  int r =  pthread_create(&revoker, NULL, &lock_server_cache_rsm::Revoker_Helper, this);
  VERIFY (r == 0);
}

void
lock_server_cache_rsm::Revoker_Function()
{

  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock
  while(1) 
  {
    m_lock_map.lock();
    /*lock_protocol::lockid_t lid;
    std::string next_clt;*/
    tprintf("revoke thread waits to be waken up\n");
    while(requests.empty())
    {
      cv_revoker.wait(&m_lock_map); 
    }
    tprintf("\n\n\trevoke thread was waken up\n");
    lock_protocol::lockid_t lid = requests.front().lid;
    std::string next_clt = requests.front().client_id;
    tprintf("client %s is poped out\n", next_clt.c_str());
    requests.pop();
    std::string holding_clt = lock_client_map[lid].client_id;
    lock_client_map[lid].flag_revoke = true;
    m_lock_map.unlock();
        
    if (duplicate_flag) {
      if (rsm->amiprimary()) {
        int r;
        handle h(last_clt);
        rpcc* cl = h.safebind();
        //tprintf("duplicated:send revoke of %d to %s\n", last_lid, holding_clt.c_str());
        rlock_protocol::status ret = cl->call(rlock_protocol::revoke, last_lid, server_xid, r);
        VERIFY(ret == rlock_protocol::OK);
        server_xid++;
      }
      duplicate_flag = false;
    } 
    else {
      /*tprintf("revoke thread waits to be waken up\n");
      while(requests.empty())
      {
        cv_revoker.wait(&m_lock_map); 
      }
      tprintf("\n\n\trevoke thread was waken up\n");
      lid = requests.front().lid;
      next_clt = requests.front().client_id;
      tprintf("client %s is poped out\n", next_clt.c_str());
      requests.pop();
      std::string holding_clt = lock_client_map[lid].client_id;
      lock_client_map[lid].flag_revoke = true;
      m_lock_map.unlock();*/
      tprintf("ready to send revoke RPC to client %s\n", holding_clt.c_str());
      handle h_revoke(holding_clt);
      rpcc* cl_revoke = h_revoke.safebind();
      if (cl_revoke) 
      {
        int r;
        rlock_protocol::status ret = rlock_protocol::OK;
        last_clt = holding_clt;
        if (rsm->amiprimary()) {
          tprintf("send revoke RPC to client %s\n", holding_clt.c_str());
          ret = cl_revoke->call(rlock_protocol::revoke, lid, server_xid, r);
        }
        VERIFY (ret == rlock_protocol::OK);
        //server_xid++;
      } 
      else {
        tprintf("bind() failed!\n");
      }
    } 
        
    //tprintf("Try to acquire m_lock_map lock\n");
    m_lock_map.lock();
    //tprintf("revoke thread:lock status is %d\n", lock_client_map[lid].stat);
restart_point:
    tprintf("retry waits to be waken up\n");
    while(lock_client_map[lid].stat == lock)
    {
      cv_lock_map.wait(&m_lock_map);
    }
    tprintf("\n\n\tretry thread was waken up\n");
    if (duplicate_flag) {
      lock_client_map[lid].stat = lock;
      m_lock_map.unlock();
      if (rsm->amiprimary()) {
        int r;
        handle h1(last_clt);
        std::cout << last_clt << "testtest"<< std::endl;
        rpcc* cl = h1.safebind();
        tprintf("duplicated: send retry of to %s\n", last_clt.c_str());
        rlock_protocol::status ret = cl->call(rlock_protocol::retry, last_lid, server_xid, r);
        tprintf("\n\n\ttest\n");
        VERIFY(ret == rlock_protocol::OK);
        server_xid++;
         
        handle h2(last_clt);
        rpcc* c2 = h2.safebind();
        tprintf("duplicated: send revoke of to %s\n", last_clt.c_str());
        ret = c2->call(rlock_protocol::revoke, last_lid, server_xid, r);
        VERIFY(ret == rlock_protocol::OK);
        server_xid++;
      }
      duplicate_flag = false;  
      goto restart_point;
    } 
    else {
      tprintf("ready to send retry RPC to client %s\n", next_clt.c_str());
      lock_client_map[lid].flag_revoke = false;
      lock_client_map[lid].client_id = next_clt;
      lock_client_map[lid].stat = lock;
      last_clt = next_clt;
      //requests.pop();
      m_lock_map.unlock();
 

      handle h_retry(next_clt);
      rpcc* cl_retry = h_retry.safebind(); 
      if (cl_retry)
      { 
        int r;   
        rlock_protocol::status ret = rlock_protocol::OK;
        if (rsm->amiprimary()) {
          tprintf("send retry RPC to client %s\n", next_clt.c_str());
          ret = cl_retry->call(rlock_protocol::retry, lid, server_xid, r);
        }
        VERIFY( ret == rlock_protocol::OK );
      }
      else 
      {
        tprintf("bind() failed");
      }
    }
  }
}


/*void
lock_server_cache_rsm::retryer()
{

  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.
}
*/

int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id, 
             lock_protocol::xid_t xid, int &)
{
  m_lock_map.lock();
  /*if (lock_client_map[lid].xid_map[id] == xid) {
    tprintf("\n\n\tduplicate found, client xid is %llu %llu\n client is %s\n\n", 
        xid, lock_client_map[lid].xid_map[id], id.c_str());
    last_lid = lid;
    if (lock_client_map[lid].client_id == id) {
      tprintf("duplicate acquire request\n");
      m_lock_map.unlock();
      return lock_protocol::OK;
    }
    else {
      duplicate_flag = true;
      cv_revoker.signal();
      m_lock_map.unlock();
      return lock_protocol::RETRY;
    }
  }*/

  if (lock_client_map[lid].stat == unlock && !lock_client_map[lid].flag_revoke)
  {
    lock_client_map[lid].stat = lock;
    lock_client_map[lid].client_id = id;
    lock_client_map[lid].xid_map[id] = xid;
    tprintf("server grant lock to client %s\n", id.c_str());
    m_lock_map.unlock();
    return lock_protocol::OK;
  }    
  else 
  {
    lock_client_map[lid].xid_map[id] = xid;
    
    requests.push(lock_request(id, lid));
    tprintf("lock in a client, send revoker\n");
    m_lock_map.unlock();
    cv_revoker.signal();
    return lock_protocol::RETRY;  
  }
}

int 
lock_server_cache_rsm::release(lock_protocol::lockid_t lid, std::string id, 
         lock_protocol::xid_t xid, int &r)
{
  m_lock_map.lock();
  if (lock_client_map[lid].xid_map[id] == xid) {
    last_lid = lid;
    tprintf("duplicate release request\n");
    tprintf("lock status is %d\n", lock_client_map[lid].stat);
    duplicate_flag = true;
    lock_client_map[lid].stat = unlock;
  }
  else {
    lock_client_map[lid].stat = unlock;
    lock_client_map[lid].xid_map[id] = xid;
  }
  //tprintf("release:lock id is %d\n", lid)
  m_lock_map.unlock();
  cv_lock_map.signal();
  tprintf("retry thread was signal\n");
  lock_protocol::status ret = lock_protocol::OK;
  return ret;
}

std::string
lock_server_cache_rsm::marshal_state()
{
  tprintf("\n\n\tstart marshall\n");
  marshall rep;
  m_lock_map.lock();
  rep << (unsigned long long) last_lid;
  rep << (unsigned long long) server_xid;
  rep << duplicate_flag;
  rep << last_clt;
  rep << (unsigned int) lock_client_map.size();
  std::map<lock_protocol::lockid_t, lock_state>::iterator it_map;
  std::map<std::string, lock_protocol::xid_t>::iterator it_xidmap;
  struct lock_state state;
  for (it_map = lock_client_map.begin(); it_map != lock_client_map.end();
      it_map++) {
    rep << (unsigned long long)it_map->first;
    state = lock_client_map[it_map->first];
    rep << state.client_id;
    rep << state.flag_revoke;
    rep << (int)state.stat;
    rep << (unsigned int) state.xid_map.size();
    for (it_xidmap = state.xid_map.begin(); it_xidmap != state.xid_map.end();
        it_xidmap++) {
      rep << it_xidmap->first;
      rep << (unsigned long long)(it_xidmap->second);
    }
  }
  
  rep << (unsigned int) requests.size();
  struct lock_request req("", 0);
  std::queue<lock_request> tmp;
  while (!requests.empty()) {
    req = requests.front();
    rep << req.client_id;
    rep << (unsigned long long) req.lid;
    tmp.push(req);
    requests.pop();
  }
  while(!tmp.empty()) {
    requests.push(tmp.front());
    tmp.pop();
  }
  m_lock_map.unlock();

  return rep.str();
}

void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
  tprintf("\n\n\tstart unmarshall\n");
  unmarshall rep(state);
  rep >> last_lid;
  rep >> server_xid;
  rep >> duplicate_flag;
  rep >> last_clt;
  unsigned int map_size;
  rep >> map_size;
  unsigned long long lid;
  unsigned long long xid;
  int stat;
  std::string client_id;
  struct lock_state lstate;
  
  m_lock_map.lock();
  for ( size_t i=0; i<map_size; i++) {
    rep >> lid;   
    rep >> lstate.client_id;
    rep >> lstate.flag_revoke;
    rep >> stat;
    lstate.stat = (lock_server_cache_rsm::lock_status) stat;
    unsigned int xidmap_size;
    rep >> xidmap_size;
    std::map<std::string, lock_protocol::xid_t> tmp;
    for ( size_t j=0; j<xidmap_size; j++) {
      rep >> client_id;
      rep >> xid;
      tmp[client_id] = xid;
    }
    lstate.xid_map = tmp;
    lock_client_map[lid] = lstate;
  }
  
  unsigned int queue_size;
  rep >> queue_size;
  std::string clt_id;
  while (queue_size-- > 0) {
    rep >> clt_id;
    rep >> lid;
    requests.push(lock_request(clt_id, lid));
  }
  m_lock_map.unlock();
}

lock_protocol::status
lock_server_cache_rsm::stat(lock_protocol::lockid_t lid, int &r)
{
  printf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

void* 
lock_server_cache_rsm::Revoker_Helper(void* arg) 
{
  reinterpret_cast<lock_server_cache_rsm*>(arg)->Revoker_Function();
  return 0;
}

