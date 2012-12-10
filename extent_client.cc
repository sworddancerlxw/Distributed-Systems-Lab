// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include "tprintf.h"

// The calls assume that the caller holds a lock on the extent

using std::string;

extent_client::extent_client(string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  m_.lock();  
  if(extent_attr_cache.find(eid) != extent_attr_cache.end()) 
  {
    buf = extent_attr_cache[eid].extent;
    extent_attr_cache[eid].attr.atime = time(NULL);
    m_.unlock();
    return ret;
  }
  else 
  {
    m_.unlock();
    extent_protocol::attr attr;
    ret = cl->call(extent_protocol::getattr, eid, attr);
    if ( ret != extent_protocol::OK) 
    {
      tprintf("get attr from server at extent_client::get failed\n");
      return ret;
    }
    ret = cl->call(extent_protocol::get, eid, buf);
    //tprintf("get extent %d and %s from server in get\n", eid, buf.c_str());
    if ( ret != extent_protocol::OK) 
    {
      tprintf("get extent from server at extent_client::get failed\n");
      return ret;
    }
    m_.lock();
    extent_attr_cache[eid].extent = buf;
    extent_attr_cache[eid].attr = attr;
    m_.unlock();
    return ret;
  }
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  string buf;
  extent_protocol::status ret = extent_protocol::OK;
  m_.lock();  
  if(extent_attr_cache.find(eid) != extent_attr_cache.end()) 
  {
    attr = extent_attr_cache[eid].attr;
    m_.unlock();
    return ret;
  }
  else 
  {
    m_.unlock();
    ret = cl->call(extent_protocol::getattr, eid, attr);
    if ( ret != extent_protocol::OK) 
    {
      tprintf("get attr from server at extent_client::get failed\n");
      return ret;
    }
    ret = cl->call(extent_protocol::get, eid, buf);
    //tprintf("get extent %s from server in getattr\n", buf.c_str());
    if ( ret != extent_protocol::OK) 
    {
      tprintf("get extent from server at extent_client::get failed\n");
      return ret;
    }
    m_.lock();
    extent_attr_cache[eid].extent = buf;
    extent_attr_cache[eid].attr = attr;
    //std::cout << "buf is " << buf << " size is " << attr.size << std::endl;
    m_.unlock();
    return ret;
  }
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  //tprintf("created a file or directory %d\n", eid);
  //int r;
  //ret = cl->call(extent_protocol::put, eid, buf, r);
  m_.lock();
  extent_attr_cache[eid].extent = buf;
  extent_attr_cache[eid].attr.atime = time(NULL);
  extent_attr_cache[eid].attr.mtime = time(NULL);
  extent_attr_cache[eid].attr.ctime = time(NULL);
  extent_attr_cache[eid].attr.size = buf.size();
  extent_attr_cache[eid].modified = true;
  m_.unlock();
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  m_.lock();
  extent_attr_cache.erase(eid);
  m_.unlock();
  //int r;
  //ret = cl->call(extent_protocol::remove, eid, r);
  return ret;
}

extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid)
{
  tprintf("flush started\n");
  extent_protocol::status ret = extent_protocol::OK;
  m_.lock();
  if (extent_attr_cache.find(eid) != extent_attr_cache.end())
  {
    //tprintf("found this directory or file %d\n", eid);
    if (extent_attr_cache[eid].modified)
    {
      int r;
      string buf = extent_attr_cache[eid].extent;
      extent_attr_cache.erase(eid);
      m_.unlock();
      ret = cl->call(extent_protocol::put, eid, buf, r);
      //tprintf("%s is flushed to %d\n", buf.c_str(), eid);
      return ret;
    } 
    else 
    {
      extent_attr_cache.erase(eid);
      m_.unlock();
      return ret;
    }
  }
  else 
  {
    m_.unlock();
    int r;
    ret = cl->call(extent_protocol::remove, eid, r);
    return ret;
  }
  
}
