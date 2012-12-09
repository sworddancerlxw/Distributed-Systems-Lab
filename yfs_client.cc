// yfs client.  implements FS operations using extent and lock server
// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using std::vector;

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lock_extent_releaser* lu = new lock_extent_releaser(ec);
  lc = new lock_client_cache(lock_dst, dynamic_cast<lock_release_user*>(lu));
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  lc->acquire(inum);
  int r = OK;

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:
  
  lc->release(inum);
  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  lc->acquire(inum);
  int r = OK;

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  lc->release(inum);
  return r;
}

int
yfs_client::create(inum dir_inum, const char* name, inum& file_inum)
{
  lc->acquire(dir_inum);
 //srand(time(NULL));
  file_inum = rand() | (0x80000000);
  
  std::string dir_contents;
  std::string namewithcomma(name);
  namewithcomma.append(",");
  
  if (ec->get(dir_inum, dir_contents) != extent_protocol::OK) 
  {
    lc->release(dir_inum);
    return IOERR;
  }

  if (dir_contents.find(namewithcomma) != std::string::npos) 
  {
    lc->release(dir_inum);
    return EXIST;
  }

  std::ostringstream new_entry;
  new_entry << "<" << name << ", "<< file_inum << ">";
  if (ec->put(dir_inum, dir_contents.append(new_entry.str())) != extent_protocol::OK) 
  {
    lc->release(dir_inum);
    return IOERR;
  }
  
  lc->release(dir_inum);
  lc->acquire(file_inum);
  if (ec->put(file_inum, "") != extent_protocol::OK)
  {
    lc->release(file_inum);
    return IOERR;
  }
  lc->release(file_inum);
  return OK; 
}

int
yfs_client::lookup(inum dir_inum, const char* name, inum& file_inum)
{ 
  lc->acquire(dir_inum);
  std::string dir_contents;
  std::string namewithcomma(name);
  namewithcomma.append(",");

  if (ec->get(dir_inum, dir_contents) != extent_protocol::OK) 
  {
    lc->release(dir_inum);
    std::cout << "get dir error" << std::endl;
    return IOERR;
  }
  lc->release(dir_inum);
  //std::cout << dir_contents << std::endl;
  //std::cout << name << std::endl;
  if (dir_contents.find(namewithcomma) != std::string::npos)
  {
    vector<yfs_client::dirent> contents = parser(dir_contents);
  
    for(size_t i=0; i<contents.size(); i++)
    {
      if (contents[i].name.compare(name) == 0) 
      {
        file_inum = contents[i].inum;
        std::cout << "file_inum is " << std::hex << file_inum << std::endl;
        return OK;
      }
    }
    return NOENT;
  }
  std::cout << "the file name is not found" << std::endl;
  return NOENT;
  
}

int
yfs_client::readdir(inum dir_inum, std::vector<yfs_client::dirent>& dir_contents)
{
  lc->acquire(dir_inum);
  std::string contents;
  if (ec->get(dir_inum, contents) != extent_protocol::OK) 
  {
    lc->release(dir_inum);
    std::cout << "get dir error" << std::endl;
    return IOERR;
  }
  
  lc->release(dir_inum);
  vector<yfs_client::dirent> tmp = parser(contents);
  dir_contents.resize(tmp.size());
  for (size_t i=0; i<tmp.size(); i++)
  {
    dir_contents[i] = tmp[i];
  }
  return OK;
}

int
yfs_client::setattr(inum file_inum, struct stat st, struct stat* attr)
{
  lc->acquire(file_inum);
  std::string buf = "";
  if (ec->get(file_inum, buf) != extent_protocol::OK)
  {
    lc->release(file_inum);
    return NOENT;
  }

  if (st.st_size <= attr->st_size)
  {
    buf.append(attr->st_size-st.st_size, '\0');
  }
  else 
  {
    buf = buf.substr(0, attr->st_size);
  }
  
  if (ec->put(file_inum, buf) != extent_protocol::OK)
  {
    lc->release(file_inum);
    return IOERR;
  }
  
  lc->release(file_inum);
  return OK;
}

int
yfs_client::read(inum file_inum, size_t size, struct stat st, off_t off, std::string& buf)
{ 
  lc->acquire(file_inum);
  std::string file_buf;
  
  buf = "";
  if (ec->get(file_inum, file_buf) != extent_protocol::OK)
  {
    lc->release(file_inum);
    return NOENT;
  }
  lc->release(file_inum);

  if (off >= st.st_size)
  {
    return OK;
  }
  else 
  {
    if (size + off > st.st_size)
    {
      buf.append(file_buf.substr(off));
    }
    else 
    {
      buf.append(file_buf.substr(off, size));
    }
    return OK;
  }
}

int 
yfs_client::write(inum file_inum, const char* buf, struct stat st, size_t size, off_t off)
{
  lc->acquire(file_inum);
  std::string file_buf = "";
  if(ec->get(file_inum, file_buf) != extent_protocol::OK)
  {
    lc->release(file_inum);
    return IOERR;
  }
  if (off > st.st_size)
  {
    file_buf.append(off - st.st_size, '\0');
    file_buf.append(buf, size);
  } 
  else 
  {
    if (off + size > st.st_size)
    {
      file_buf = file_buf.substr(0, off);
      file_buf.append(buf, size);
    }
    else 
    {
      std::string padding = file_buf.substr(off+size, std::string::npos);
      file_buf = file_buf.substr(0, off);
      file_buf.append(buf, size);
      file_buf.append(padding);
    }
  }
  if (ec->put(file_inum, file_buf) != yfs_client::OK)
  {
    lc->release(file_inum);
    return IOERR;
  }
  lc->release(file_inum);
  return OK;
}

int 
yfs_client::mkdir(inum parent_inum, const char* dir_name, inum& dir_inum) 
{
  lc->acquire(parent_inum);
  std::string dir_contents;
  std::string namewithcomma(dir_name);
  namewithcomma.append(",");

  if (ec->get(parent_inum, dir_contents) != extent_protocol::OK)
  {
    lc->release(parent_inum);
    return IOERR;
  }
  if (dir_contents.find(namewithcomma) != std::string::npos)
  {
    lc->release(parent_inum);
    return EXIST;
  }

  std::string new_contents(dir_contents);
  dir_inum = rand() & 0x7FFFFFFF;
  new_contents.append("<").append(dir_name).append(", ").append(filename(dir_inum)).append(">");
  
  if (ec->put(parent_inum, new_contents) != extent_protocol::OK)
  {
    lc->release(parent_inum);
    return IOERR;
  }     
  lc->release(parent_inum);
  lc->acquire(dir_inum);
  if (ec->put(dir_inum, "") != extent_protocol::OK)
  {
    lc->release(dir_inum);
    return IOERR;
  }
  lc->release(dir_inum);
  return OK;
}

int 
yfs_client::unlink(inum parent_inum, const char* name)
{
  lc->acquire(parent_inum);
  std::string dir_contents;
  if (ec->get(parent_inum, dir_contents) != extent_protocol::OK)
  {
    lc->release(parent_inum);
    return IOERR;
  }

  vector<yfs_client::dirent> contents = parser(dir_contents);
  bool found = false;
  std::ostringstream oss;
  inum file_inum;
  for (size_t i=0; i<contents.size(); i++)
  {
    if (contents[i].name.compare(name) == 0 && isfile(contents[i].inum))
    {
       found = true;
       file_inum = contents[i].inum;
       contents.erase(contents.begin()+i);
       break; 
    }
  }
  
  if (!found)
  {
    lc->release(parent_inum);
    return NOENT;
  }

  for (size_t i=0; i<contents.size(); i++)
  {
    oss << "<" << contents[i].name << ", "<< contents[i].inum << ">";
  }

  if (ec->put(parent_inum, oss.str()) != extent_protocol::OK)
  {
    lc->release(parent_inum);
    return IOERR;
  }

  lc->release(parent_inum);
  lc->acquire(file_inum);
  if (ec->remove(file_inum) != extent_protocol::OK)
  {
    lc->release(file_inum);
    return IOERR;
  }
  
  lc->release(file_inum);
  return OK;
} 


vector<yfs_client::dirent> yfs_client::parser(std::string& contents)
{
  vector<yfs_client::dirent> results;
  size_t num_s, num_e, name_s, name_e;
  yfs_client::inum num;
  std::string name;
  for (size_t i=0; i<contents.length(); i++)
  {
    if (contents[i] == '<') { name_s = i; }
    if (contents[i] == ',') 
    { 
      name_e = i;
      num_s = i;
      name = contents.substr(name_s+1, name_e-name_s-1);
    }
    if (contents[i] == '>')
    {
      num_e = i;
      std::stringstream ss(contents.substr(num_s+2, num_e-num_s-2));
      results.push_back(dirent());
      ss >> num;
      results.back().name = name;
      results.back().inum = num;
    }
    
  }
  return results;
}

