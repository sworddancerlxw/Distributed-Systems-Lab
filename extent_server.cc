// the extent server implementation

#include "extent_server.h"
#include "tprintf.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using std::string;
using std::pair;

extent_server::extent_server() 
{
	contents_.insert(pair<extent_protocol::extentid_t, string>(0x00000001, "") );
	extent_protocol::attr attr;
	attr.atime = time(NULL);
	attr.mtime = time(NULL);
	attr.ctime = time(NULL);
	attrs_.insert(pair<extent_protocol::extentid_t, extent_protocol::attr>(0x00000001, attr ));
}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
	m_.lock();
	pair<StringMap::iterator, bool> ret1;
	ret1 = contents_.insert(pair<extent_protocol::extentid_t, string>(id, buf) );
	pair<AttrMap::iterator, bool> ret2;
	ret2 = attrs_.insert(pair<extent_protocol::extentid_t, extent_protocol::attr>(id, extent_protocol::attr()) );

	if (!ret1.second)
	{
		if (!ret2.second)
		{
		 	//tprintf("put %s in %d successfully\n", buf.c_str(), id);
                        ret1.first->second = buf;
			ret2.first->second.ctime = time(NULL);
			ret2.first->second.mtime = time(NULL);
			ret2.first->second.atime = time(NULL);
			ret2.first->second.size  = buf.size();	
			m_.unlock();
			return extent_protocol::OK;
		}
		else 
		{
			m_.unlock();
			return extent_protocol::IOERR;
		}
	}
	else 
	{
		if (!ret2.second)
		{
			m_.unlock();
			return extent_protocol::IOERR;
		}
		else
		{
		 	//tprintf("put %s in %d successfully\n", buf.c_str(), id);
			ret2.first->second.ctime = time(NULL);
			ret2.first->second.mtime = time(NULL);
			ret2.first->second.atime = time(NULL);
                        ret2.first->second.size = buf.size();
			m_.unlock();
			return extent_protocol::OK;
		}
				
	}	
  // You fill this in for Lab 2.
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
	m_.lock();
	StringMap::iterator it_con = contents_.find(id);
	if (it_con == contents_.end())
	{
		m_.unlock();
		return extent_protocol::NOENT;
	}
	else
	{
		AttrMap::iterator it_attr = attrs_.find(id);
		if (it_attr == attrs_.end())	
		{
			m_.unlock();
			return extent_protocol::NOENT;
		}
		else
		{
			buf = it_con->second;
			it_attr->second.atime = time(NULL);
			m_.unlock();
			return extent_protocol::OK;
		}
	}
  // You fill this in for Lab 2.
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{	
	m_.lock();
	StringMap::iterator it_con = contents_.find(id);
	if (it_con == contents_.end())
	{       
		m_.unlock();
		return extent_protocol::NOENT;
	}
	else
	{
		AttrMap::iterator it_attr = attrs_.find(id);
		if (it_attr == attrs_.end())	
		{
			m_.unlock();
			return extent_protocol::NOENT;
		}
		else
		{
			a.atime = it_attr->second.atime;
			a.ctime = it_attr->second.ctime;
			a.mtime = it_attr->second.mtime;
			a.size = it_attr->second.size;
			m_.unlock();
			return extent_protocol::OK;
		}
	}
  // You fill this in for Lab 2.
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
	m_.lock();
	StringMap::iterator it_con = contents_.find(id);
	if (it_con == contents_.end())
	{
		m_.unlock();
		return extent_protocol::NOENT;
	}
	else
	{
		AttrMap::iterator it_attr = attrs_.find(id);
		if (it_attr == attrs_.end())	
		{
			m_.unlock();
			return extent_protocol::NOENT;
		}
		else
		{
			contents_.erase(it_con);
			attrs_.erase(it_attr);
			m_.unlock();
			return extent_protocol::OK;
		}
	}
  // You fill this in for Lab 2.
}

