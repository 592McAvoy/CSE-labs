// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

pthread_mutex_t mutex;

lock_server_cache::lock_server_cache()
{
  pthread_mutex_init(&mutex, NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  lock_protocol::status ret = lock_protocol::OK;
  int r;
  
  pthread_mutex_lock(&mutex);
  tprintf("lock-server\tid:%s\tacquire lock:%ld\n",id.c_str(),lid);
  if(lockmap.find(lid) == lockmap.end()){
    //a new lock never appear
    tprintf("lock-server\tid:%s\tacquire lock:%ld\tnew lock\n",id.c_str(),lid);
    lock_info info;
    info.owner = id;
    info.state = LOCKED;
    lockmap[lid] = info;
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  else{
    switch(lockmap[lid].state){
      case FREE:{
        tprintf("lock-server\tid:%s\tacquire lock:%ld\tfree lock\n",id.c_str(),lid);
        lockmap[lid].state = LOCKED;
        lockmap[lid].owner = id;
        pthread_mutex_unlock(&mutex);
        return ret;
      }
      case ASSIGNED:{
        tprintf("lock-server\tid:%s\tacquire lock:%ld\tassigned lock\n",id.c_str(),lid);
        if(lockmap[lid].owner == id){
          tprintf("lock-server\tid:%s\tacquire lock:%ld\tassigned to:%s\n",id.c_str(),lid,id.c_str());
          lockmap[lid].state = LOCKED;
          if(!lockmap[lid].wait_queue.empty()){
            pthread_mutex_unlock(&mutex);
            handle h(id);
            rpcc *cl = h.safebind();
            if(cl){
              cl->call(rlock_protocol::revoke, lid, r);
            }
            return ret;
          }
          pthread_mutex_unlock(&mutex);
          return ret;
        }
      }
      case LOCKED:{
        tprintf("lock-server\tid:%s\tacquire lock:%ld\tlocked lock\n",id.c_str(),lid);
            
        string lock_owner = lockmap[lid].owner;
        if(lock_owner == id){
          tprintf("lock-server\tid:%s\tacquire lock:%ld\tA to A\n",id.c_str(),lid);
          pthread_mutex_unlock(&mutex);
          return lock_protocol::GRANTED;
        }

        lockmap[lid].wait_queue.push(id);
        int size = lockmap[lid].wait_queue.size();
        pthread_mutex_unlock(&mutex);

        
        tprintf("lock-server\tid:%s\tacquire lock:%ld\trevoke owner:%s\n",id.c_str(),lid,lock_owner.c_str());
        handle h(lock_owner);
        rpcc *cl = h.safebind();
        if(cl){
          ret = cl->call(rlock_protocol::revoke, lid, size);
        }
        return lock_protocol::RETRY;

        /*if(ret == rlock_protocol::OK){
          return lock_protocol::RETRY;
        }else{
          tprintf("lock-server\tid:%s\tacquire lock:%ld\trevoke error\n",id.c_str(),lid);
          return lock_protocol::RPCERR;
        }*/
        
      }
      default:{
        tprintf("lock-server\tid:%s\tacquire lock:%ld\tunexpected state:%d\n",id.c_str(),lid,lockmap[lid].state);
        pthread_mutex_unlock(&mutex);
        }
    }
  }
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &)
{
  lock_protocol::status ret = lock_protocol::OK;
  int r = -1;

  pthread_mutex_lock(&mutex);
  tprintf("lock-server\tid:%s\trelease lock:%ld\n",id.c_str(),lid);
  //check ownership
  if(lockmap[lid].owner != id){
    tprintf("lock-server\tid:%s\trelease lock:%ld\twrong owner\n",id.c_str(),lid);
    pthread_mutex_unlock(&mutex);
    return lock_protocol::RPCERR;
  }
  
  /*//check state
  pthread_mutex_unlock(&mutex);
  handle h(id);
  rpcc *cl = h.safebind();
  if(cl){
    ret = cl->call(rlock_protocol::stat, lid, r);
  }    
  if(ret == rlock_protocol::OK){
    tprintf("lock-server\tid:%s\trelease lock:%ld\towner still holds lock\n",id.c_str(),lid);
    return lock_protocol::RPCERR;
  }
 
  pthread_mutex_lock(&mutex);

  //check ownership
  if(lockmap[lid].owner != id){
    tprintf("lock-server\tid:%s\trelease lock:%ld\twrong owner\n",id.c_str(),lid);
    pthread_mutex_unlock(&mutex);
    return lock_protocol::RPCERR;
  }*/

  if(lockmap[lid].wait_queue.empty()){
    tprintf("lock-server\tid:%s\trelease lock:%ld\tnobody waits\n",id.c_str(),lid);
    lockmap[lid].state = FREE;
    lockmap[lid].owner = -1;
    pthread_mutex_unlock(&mutex);
  }
  else{
    string next = lockmap[lid].wait_queue.front();
    lockmap[lid].wait_queue.pop();
    lockmap[lid].owner = next;
    lockmap[lid].state = ASSIGNED;
    int size = lockmap[lid].wait_queue.size();
    pthread_mutex_unlock(&mutex);

    tprintf("lock-server\tid:%s\trelease lock:%ld\tcall next owner:%s\n",id.c_str(),lid,next.c_str());
    handle h(next);
    rpcc *cl = h.safebind();
    if(cl){
      ret = cl->call(rlock_protocol::retry, lid, size);
    }    
    if(ret == rlock_protocol::OK){
      ret = lock_protocol::OK;
    }
    else{
      tprintf("lock-server\tid:%s\trelease lock:%ld\trecursive call\n",id.c_str(),lid);
      return release(lid, next, r);
    }
  }
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, std::string id, int &r)
{
  pthread_mutex_lock(&mutex);
  //check ownership
  if(lockmap[lid].owner != id){
    tprintf("lock-server\tid:%s\tstat lock:%ld\twrong owner\n",id.c_str(),lid);
    pthread_mutex_unlock(&mutex);
    return lock_protocol::RPCERR;
  }
  pthread_mutex_unlock(&mutex);
  return lock_protocol::OK;

}

