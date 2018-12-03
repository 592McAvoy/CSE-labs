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
  ec->getattr(ino, a);
  return a.size;  
}

void 
NameNode::RecordDirtyInode(yfs_client::inum ino){
  list<blockid_t> l;
  ec->get_block_ids(ino, l);
  list<blockid_t>::iterator it;
  for(it=l.begin();it!=l.end();it++){
    dirtyBlocks.push_back(*it);
  }
}
list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
  printf("namenode\tGetBlockLocations\tino:%d\n",ino);fflush(stdout);
  list<blockid_t> block_ids;
  list<LocatedBlock> block_locs;
  extent_protocol::status ret;

  printf("namenode\tacquire lock:%d\n",ino);fflush(stdout);
  lc->acquire(ino);

  unsigned int filesize = GetFileSize(ino);  

  ret = ec->get_block_ids(ino, block_ids);
  if(ret != extent_protocol::OK){
    lc->release(ino);
    printf("namenode\trelease lock:%d\n",ino);fflush(stdout);
    return block_locs;
  }

  int len = block_ids.size();
  for(int i=0;i<len;i++){
    blockid_t id = block_ids.front();
    uint64_t size = (i==len-1) ? filesize%BLOCK_SIZE : BLOCK_SIZE;    
    block_locs.push_back(LocatedBlock(id, i*BLOCK_SIZE, size, GetDatanodes()));
    block_ids.pop_front();
  }

  lc->release(ino);
  printf("namenode\trelease lock:%d\n",ino);fflush(stdout);
  return block_locs;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
  printf("namenode\tComplete\tino:%d\n",ino);fflush(stdout);
  extent_protocol::status ret;

  ret = ec->complete(ino, new_size);
  RecordDirtyInode(ino);
  lc->release(ino);
  printf("namenode\trelease lock:%d\n",ino);fflush(stdout);
  if(ret != extent_protocol::OK){    
    return false;
  }
  return true;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
  //throw HdfsException("Not implemented");
  printf("namenode\tAppendBlock\tino:%d\n",ino);fflush(stdout);
  blockid_t bid;
  extent_protocol::status ret;

  unsigned int filesize = GetFileSize(ino);  
  int off = filesize/BLOCK_SIZE + (filesize%BLOCK_SIZE>0);
  ret = ec->append_block(ino, bid);
  
  return LocatedBlock(bid,off,BLOCK_SIZE, GetDatanodes());
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
  printf("namenode\tRename\told:%s\tnew:%s\n",src_name.c_str(),dst_name.c_str());fflush(stdout);
  bool found;
  int r;
  r = yfs->rename(src_dir_ino, src_name, dst_dir_ino, dst_name);
  if(r != yfs_client::OK){
    return false;
  }  
  return true;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  printf("namenode\tMkdir\tparent:%d\tname:%s\n",parent,name.c_str());fflush(stdout);
  int r;
  r = yfs->mkdir(parent, name.c_str(),mode, ino_out);
  printf("\tnew dir ino:%d\n",ino_out);
  if(r != yfs_client::OK){
    return false;
  }  
  return true;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  printf("namenode\tCreate\tparent:%d\tname:%s\n",parent,name.c_str());fflush(stdout);
  extent_protocol::status ret; 
  int r; 
  printf("namenode\tacquire lock:%d\n",create_mark);fflush(stdout);
  lc->acquire(create_mark);
  ret = ec->create(extent_protocol::T_FILE, ino_out);
  lc->release(create_mark);
  printf("namenode\trelease lock:%d\n",create_mark);fflush(stdout);
  
  printf("\t\tnew ino:%d\n",ino_out);
  if(ret != extent_protocol::OK){
    return false;
  }

  r = yfs->addEntry(parent, ino_out, name);

  lc->acquire(parent);
  RecordDirtyInode(parent);
  lc->release(parent);

  if(r != yfs_client::OK){
    return false;
  }  
  printf("namenode\tacquire lock:%d\n",ino_out);fflush(stdout);
  lc->acquire(ino_out);
  return true;
}

bool NameNode::Isfile(yfs_client::inum ino) {
  printf("namenode\tIsfile\tino:%d\n",ino);fflush(stdout);
  extent_protocol::attr a;
 
  if (ec->getattr(ino, a) != extent_protocol::OK) {
    printf("error getting attr\n");
    return false;
  }

  if(a.type == extent_protocol::T_FILE) {
    printf("isfile: %lld is a file\n", ino);
    return true;
  } 
  printf("isfile: %lld is not a file\n", ino);
  return false;
}

bool NameNode::Isdir(yfs_client::inum ino) {
  printf("namenode\tIsdir\tino:%d\n",ino);fflush(stdout);
  if(yfs->isdir(ino)){
    return true;
  }  
  return false;
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
  printf("namenode\tGetfile\tino:%d\n",ino);fflush(stdout);
  printf("getfile %016llx\n", ino);
    extent_protocol::attr a;
    if (ec->getattr(ino, a) != extent_protocol::OK) {
        return false;
    }
    info.atime = a.atime;
    info.mtime = a.mtime;
    info.ctime = a.ctime;
    info.size = a.size;
    printf("getfile %016llx -> sz %llu\n", ino, info.size);
    return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
  printf("namenode\tGetdir\tino:%d\n",ino);fflush(stdout);
  int r;
  r = yfs->getdir(ino, info);
  if(r != yfs_client::OK){
    return false;
  }  
  return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
  printf("namenode\tReaddir\tino:%d\n",ino);fflush(stdout);
  int r;
  lc->release(ino);
  r = yfs->readdir(ino, dir);
  lc->acquire(ino);
  if(r != yfs_client::OK){
    return false;
  }  
  return true;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
  printf("namenode\tUnlink\tino:%d\n",ino);fflush(stdout);
  int r;
  extent_protocol::status ret;
  yfs_client::inum i;
  lc->release(parent);
  r = yfs->delEntry(parent, i, name);
  lc->acquire(parent);
  if(r != yfs_client::OK){
    printf("namenode\tUnlink\tino:%d\tfailed1!!\n",ino);fflush(stdout);
    return false;
  }
  
  ret = ec->remove(ino);  
  if(ret != extent_protocol::OK){
    printf("namenode\tUnlink\tino:%d\tfailed2!!\n",ino);fflush(stdout);
    return false;
  }
  
  printf("namenode\tUnlink\tino:%d\tsuccess!!\n",ino);fflush(stdout);
  return true;
}


void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
  printf("namenode\tDatanodeHeartbeat\n");fflush(stdout);

  State s = stateMap.find(id)->second;
  if(s.stat == d_DEAD){
    //lc->acquire(maplock);
    stateMap.find(id)->second.stat = d_RECOVER; 
    stateMap.find(id)->second.lastTime = time(NULL);   
    //lc->release(maplock);
    //replicate
    NewThread(this, &NameNode::ReplicateDatanode, id);
    NewThread(this, &NameNode::CheckAlive, id);
  }
  else{
    stateMap.find(id)->second.lastTime = time(NULL);
  }
  //stateMap[id] = State(s.lastTime, s.live);
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
  printf("namenode\tRegisterDatanode\n");fflush(stdout);
  
  datanodes.push_back(id);
  //lc->acquire(maplock);
  stateMap.insert(std::pair<DatanodeIDProto, State>(id, State(time(NULL), d_RECOVER)));
  //lc->release(maplock);
  //replicate
  NewThread(this, &NameNode::ReplicateDatanode, id);
  //create a new thread to check liveness
  NewThread(this, &NameNode::CheckAlive, id);
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
  printf("namenode\tGetDatanodes\n");fflush(stdout);
  list<DatanodeIDProto> result;
  list<DatanodeIDProto>::iterator it;
  //lc->acquire(maplock);
  for(it=datanodes.begin();it!=datanodes.end();it++)
  {
    DatanodeIDProto id = *it;
    State s = stateMap.find(id)->second;
    if(s.stat == d_NORMAL){
      result.push_back(id);
    }
  }
  //lc->release(maplock);
  return result;
}

void  
NameNode::CheckAlive(DatanodeIDProto id){
  time_t now;
  while(1){
    now = time(NULL);
    State s = stateMap.find(id)->second;
    if(difftime(now, s.lastTime)>=5){
      //lc->acquire(maplock);
      stateMap.find(id)->second.stat = d_DEAD;
      //lc->release(maplock);

      printf("namenode\tCheckAlive\tDead\n");fflush(stdout);
      ReallocDatanode(id);
      return;
    }
    printf("namenode\tCheckAlive\tLive\n");fflush(stdout);
    sleep(1);//check once per socend;    
  }
}

void 
NameNode::ReplicateDatanode(DatanodeIDProto id){
  printf("namenode\tReplicateDatanode\n");fflush(stdout);
  
  if(datanodes.empty() || dirtyBlocks.empty()){
    stateMap.find(id)->second.stat = d_NORMAL;
    return;
  }
  
  list<blockid_t>::iterator it;
  for(it=dirtyBlocks.begin();it!=dirtyBlocks.end();it++){
    blockid_t bid = *it;
    ReplicateBlock(bid, master_datanode, id);
  }
  //lc->acquire(maplock);
  stateMap.find(id)->second.stat = d_NORMAL;
  //lc->release(maplock);
 }

void 
NameNode::ReallocDatanode(DatanodeIDProto broken){
  printf("namenode\tReallocDatanode\n");fflush(stdout);
  if(dirtyBlocks.empty()){
    return;
  }
  list<DatanodeIDProto> health = GetDatanodes();
  if(health.empty()){
    return;
  }
  list<DatanodeIDProto> ::iterator datait;
  for(datait=health.begin();datait!=health.end();datait++){
    DatanodeIDProto tt = *datait;
    list<blockid_t>::iterator it;
    for(it=dirtyBlocks.begin();it!=dirtyBlocks.end();it++){
      blockid_t bid = *it;
      //ReplicateBlock(bid, broken, tt);
      ReplicateBlock(bid, master_datanode, tt);
  }
  }
  
}