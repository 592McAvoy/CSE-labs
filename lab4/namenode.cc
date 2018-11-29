#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"

using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst) {
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  yfs = new yfs_client(extent_dst, lock_dst);

  /* Add your init logic here */
}

unsigned int
NameNode::GetFileSize(yfs_client::inum ino){
  extent_protocol::attr a;
  ret = ec->getattr(ino, a);
  return a.size;  
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
  list<blockid_t> block_ids;
  list<LocatedBlock> block_locs;
  extent_protocol::status ret;
  
  ino = ino - INODE_NUM;

  lc->acquire(ino);

  unsigned int filesize = GetFileSize(ino);  

  ret = ec->get_block_ids(ino, block_ids);
  if(ret != extent_protocol::OK){
    lc->release(ino);
    return block_locs;
  }

  int len = block_ids.size();
  for(int i=0;i<len;i++){
    blockid_t id = block_ids.front();
    uint64_t size = (i==len-1) ? filesize%BLOCKSIZE : BLOCKSIZE;    
    block_locs.push_back(LocatedBlock(id, i, size, master_datanode));
    block_ids.pop_front();
  }

  lc->release(ino);
  return block_locs;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
  extent_protocol::status ret;

  ino = ino - INODE_NUM;
  ret = ec->complete(ino, new_size);
  lc->release(ino);
  if(ret != extent_protocol::OK){    
    return false;
  }
  return true;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
  //throw HdfsException("Not implemented");
  blockid_t bid;
  extent_protocol::status ret;
  ino = ino - INODE_NUM;  

  unsigned int filesize = GetFileSize(ino);  
  int off = filesize/BLOCKSIZE + (filesize%BLOCKSIZE>0);
  ret = ec->append_block(ino, bid);
  return LocatedBlock(bid,off,BLOCK_SIZE,master_datanode);
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
  bool found;
  int r;
  r = yfs->rename(src_dir_ino, src_name, dst_dir_ino, dst_name);
  if(r != OK){
    return false;
  }  
  return true;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  int r;
  r = yfs->mkdir(parent, name.c_str(),mode, ino_out);
  if(r != OK){
    return false;
  }  
  return true;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  extent_protocol::status ret; 
  int r; 
  inum fid;
  lc->acquire(create_mark);
  ret = ec->create(extent_protocol::T_FILE fid);
  lc->release(create_mark);
  if(ret != extent_protocol::OK){
    return false;
  }

  ino_out = fid + INODE_NUM;
  r = yfs->addEntry(parent, ino_out, name);
  if(r != OK){
    return false;
  }  
  lc->acquire(ino_out);
  return true;
}

bool NameNode::Isfile(yfs_client::inum ino) {
  fflush(stdout);
  extent_protocol::attr a;
  inum i = ino - INODE_NUM;

  if (ec->getattr(i, a) != extent_protocol::OK) {
    printf("error getting attr\n");
    return false;
  }

  if(a.type == extent_protocol::T_FILE) {
    printf("isfile: %lld is a file\n", inum);
    return true;
  } 
  printf("isfile: %lld is not a file\n", inum);
  return false;
}

bool NameNode::Isdir(yfs_client::inum ino) {
  int r;
  r = yfs->isdir(ino);
  if(r != OK){
    return false;
  }  
  return true;
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
  printf("getfile %016llx\n", ino);
  inum i = ino - INODE_NUM;
    extent_protocol::attr a;
    if (ec->getattr(i, a) != extent_protocol::OK) {
        return false;
    }
    info.atime = a.atime;
    info.mtime = a.mtime;
    info.ctime = a.ctime;
    info.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);
    return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
  int r;
  r = yfs->getdir(ino, info);
  if(r != OK){
    return false;
  }  
  return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
  int r;
  r = yfs->readdir(ino, dir);
  if(r != OK){
    return false;
  }  
  return true;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
  int r;
  extent_protocol::status ret;
  inum i;
  r = yfs->delEntry(parent, i, name);
  if(r != OK){
    return false;
  }
  ret = ec->remove(ino - INODE_NUM);
  if(ret != extent_protocol::OK){
    return false;
  }
  return true;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
  return list<DatanodeIDProto>();
}
