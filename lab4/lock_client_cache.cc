// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"
#include <unistd.h>

pthread_mutex_t mymutex;
pthread_cond_t cv;

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  char hname[100];
  VERIFY(gethostname(hname, sizeof(hname)) == 0);
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();

  pthread_mutex_init(&mymutex, NULL);
  pthread_cond_init(&cv, NULL);
  
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
  rlsrpc->reg(rlock_protocol::stat, this, &lock_client_cache::stat);

  
}

rlock_protocol::status
lock_client_cache::stat(lock_protocol::lockid_t lid, int &){
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&mymutex);
  tprintf("lock-client\tid:%s\tstat lock:%ld\tstate:%d\n",id.c_str(),lid,lockmap[lid].state);
  switch(lockmap[lid].state){
    case FREE:
    case LOCKED:
    case RELEASING: break;      
    default:ret = rlock_protocol::RPCERR;
  }
  pthread_mutex_unlock(&mymutex);
  return ret;
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  
  int ret = lock_protocol::OK;
  int r;

  pthread_mutex_lock(&mymutex);
  tid thread = pthread_self();
  tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\n",id.c_str(),thread,lid);
  if(lockmap.find(lid) == lockmap.end()){
    //a new lock 
    tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\tnew lock\n",id.c_str(),thread,lid);
    lock_info info;
    info.owner = -1;
    info.revoke_call = false;
    info.state = ACQUIRING;
    info.wait_queue.push(thread);
    lockmap[lid] = info;
    pthread_mutex_unlock(&mymutex); 

    ret = cl->call(lock_protocol::acquire, lid, id, r);
    if(ret == lock_protocol::GRANTED){
      ret = lock_protocol::OK;
    }
  }
  else{
    switch(lockmap[lid].state){
      case FREE:{
        tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\tfree lock\n",id.c_str(),thread,lid);
        lockmap[lid].state = LOCKED;
        lockmap[lid].owner = thread;
        pthread_mutex_unlock(&mymutex);
        return ret;
        }
      case LOCKED:{
        tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\tlocked lock\n",id.c_str(),thread,lid);
        lockmap[lid].wait_queue.push(thread);
        //wait until owner is this thread
        while(lockmap[lid].owner != thread){
          pthread_cond_wait(&cv,&mymutex);
        }
        tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\tlocked loop over\n",id.c_str(),thread,lid);
        pthread_mutex_unlock(&mymutex);
        return ret;
      }
      case NONE:{
        tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\tnone lock\n",id.c_str(),thread,lid);
        lockmap[lid].wait_queue.push(thread);
        lockmap[lid].state = ACQUIRING;
        pthread_mutex_unlock(&mymutex);

        ret = cl->call(lock_protocol::acquire, lid, id, r);
        if(ret == lock_protocol::GRANTED){
          ret = lock_protocol::OK;
        } 
        break;
      }
      case ACQUIRING:{
        tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\tacquirng lock\n",id.c_str(),thread,lid);
        lockmap[lid].wait_queue.push(thread);
        //wait until owner is this thread
        while(lockmap[lid].owner != thread){
          pthread_cond_wait(&cv,&mymutex);
        }
        tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\tacquiring loop over\n",id.c_str(),thread,lid);
        pthread_mutex_unlock(&mymutex);
        return ret;
      }
      default:{
        tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\treleasing lock\n",id.c_str(),thread,lid);
        lockmap[lid].wait_queue.push(thread);
        //wait until owner is this thread
        while(lockmap[lid].owner != thread){
          pthread_cond_wait(&cv,&mymutex);
        }
        tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\treleasing loop over\n",id.c_str(),thread,lid);
        pthread_mutex_unlock(&mymutex);
        return ret;
      }
    }
  }

  pthread_mutex_lock(&mymutex);
  if(ret == lock_protocol::OK){
    tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\tget lock\n",id.c_str(),thread,lid);
    if(lockmap[lid].wait_queue.empty()){
      lockmap[lid].owner = thread;
    }
    else{
      tid next = lockmap[lid].wait_queue.front();
      lockmap[lid].wait_queue.pop();
      lockmap[lid].owner = next;
    }

    if(lockmap[lid].revoke_call || lockmap[lid].state == RELEASING){
      lockmap[lid].revoke_call = false;
      lockmap[lid].state = RELEASING;
    }
    else{
      lockmap[lid].state = LOCKED;
    }
    pthread_mutex_unlock(&mymutex);
  }

  else if(ret == lock_protocol::RETRY || ret == lock_protocol::GRANTED){    
    //wait until owner is this thread
    tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\tRETRY lock\n",id.c_str(),thread,lid);
    while(lockmap[lid].owner != thread){
      pthread_cond_wait(&cv,&mymutex);
    }
    tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\tretry loop over, get lock!\n",id.c_str(),thread,lid);
    pthread_mutex_unlock(&mymutex);
    ret = lock_protocol::OK;
  }

  else{
    tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\terror\n",id.c_str(),thread,lid);
  }
  
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret = rlock_protocol::OK;
  int r;
  
  pthread_mutex_lock(&mymutex);

  tid thread = pthread_self();  
  tprintf("lock-client\tid:%s\tthread:%ld\trelease lock:%ld\n",id.c_str(),thread,lid);
  if(lockmap[lid].revoke_call){
    lockmap[lid].state = RELEASING;
    lockmap[lid].revoke_call = false;
  }

  switch(lockmap[lid].state){
    case LOCKED:{
      tprintf("lock-client\tid:%s\tthread:%ld\trelease lock:%ld\tlocked lock\n",id.c_str(),thread,lid);
      if(lockmap[lid].wait_queue.empty()){
        tprintf("lock-client\tid:%s\tthread:%ld\trelease lock:%ld\tnobody waits\n",id.c_str(),thread,lid);
        lockmap[lid].owner = -1;
        lockmap[lid].state = FREE;
        pthread_mutex_unlock(&mymutex);
      }
      else{
        tid next = lockmap[lid].wait_queue.front();
        if(next <= 0){
          pthread_mutex_unlock(&mymutex);
          cl->call(lock_protocol::release, lid, id, r);
          return rlock_protocol::OK;
        }
        lockmap[lid].wait_queue.pop();
        lockmap[lid].owner = next;
        tprintf("lock-client\tid:%s\tthread:%ld\trelease lock:%ld\tnext owner:%ld\n",id.c_str(),thread,lid,next);
        pthread_cond_broadcast(&cv);
        pthread_mutex_unlock(&mymutex);
      }
      return ret;
    }
    case RELEASING:{
      tprintf("lock-client\tid:%s\tthread:%ld\trelease lock:%ld\treleasing lock\n",id.c_str(),thread,lid);
      
      if(!lockmap[lid].wait_queue.empty()){
        lockmap[lid].state = ACQUIRING;
      }else{
        lockmap[lid].state = NONE;
      }

      lockmap[lid].owner = -1;
      pthread_mutex_unlock(&mymutex);

      ret = cl->call(lock_protocol::release, lid, id, r);

      pthread_mutex_lock(&mymutex);
      if(!lockmap[lid].wait_queue.empty()){
        tprintf("lock-client\tid:%s\tthread:%ld\trelease lock:%ld\tcall acquire for remaining lock\n",id.c_str(),thread,lid);
        pthread_mutex_unlock(&mymutex);
        int rt = cl->call(lock_protocol::acquire, lid, id, r);

        if(rt == lock_protocol::OK || rt == lock_protocol::GRANTED){
          pthread_mutex_lock(&mymutex);
          if(lockmap[lid].owner != -1){
            pthread_mutex_unlock(&mymutex);
            return ret;
          }
          tid next = lockmap[lid].wait_queue.front();
          if(next <= 0){
            pthread_mutex_unlock(&mymutex);
            cl->call(lock_protocol::release, lid, id, r);
            return rlock_protocol::OK;
          }
          lockmap[lid].wait_queue.pop();
          lockmap[lid].owner = next;
          lockmap[lid].state = LOCKED;
          tprintf("lock-client\tid:%s\tthread:%ld\trelease lock:%ld\tafter call next owner:%ld\n",id.c_str(),thread,lid,next);
          pthread_cond_broadcast(&cv);
          pthread_mutex_unlock(&mymutex);
        }
        return ret;
      }
      pthread_mutex_unlock(&mymutex);
      return ret;
    }
    default:{
      tprintf("lock-client\tid:%s\tthread:%ld\trelease lock:%ld\tunexpected state:%d\n",id.c_str(),thread,lid,lockmap[lid].state);
      pthread_mutex_unlock(&mymutex);
      /* error */
      }
  }

  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;
  int r;
 
  pthread_mutex_lock(&mymutex);
  tprintf("lock-client\tid:%s\trevoke lock:%ld\n",id.c_str(),lid);
  switch(lockmap[lid].state){
    case FREE:{
      tprintf("lock-client\tid:%s\trevoke lock:%ld\trelease free lock\n",id.c_str(),lid);
      lockmap[lid].state = NONE;
      pthread_mutex_unlock(&mymutex);
      cl->call(lock_protocol::release, lid, id, r);
      return ret;
    }
    case LOCKED:{
      tprintf("lock-client\tid:%s\trevoke lock:%ld\tchange state to releasing\n",id.c_str(),lid);
      lockmap[lid].state = RELEASING;
      pthread_mutex_unlock(&mymutex);
      return ret;
    }
    default:{
      tprintf("lock-client\tid:%s\trevoke lock:%ld\tunexpected state:%d\n",id.c_str(),lid,lockmap[lid].state);
      lockmap[lid].revoke_call = true;
      pthread_mutex_unlock(&mymutex);
      return rlock_protocol::RPCERR;
      }
  }
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &size)
{
  int ret = rlock_protocol::OK;
  int r;
  tprintf("lock-client\tid:%s\tretry lock:%ld\tr:%d\n",id.c_str(),lid,size);

  pthread_mutex_lock(&mymutex);
  if( lockmap[lid].wait_queue.empty() && lockmap[lid].state==NONE){
    //mutiple retry error
    tprintf("lock-client\tid:%s\tretry lock:%ld\tdo not need a lock now\n",id.c_str(),lid);
    pthread_mutex_unlock(&mymutex);
    return rlock_protocol::RPCERR;
  }
  pthread_mutex_unlock(&mymutex);

  tprintf("lock-client\tid:%s\tretry lock:%ld\tdebug1\n",id.c_str(),lid);
  r = cl->call(lock_protocol::acquire, lid, id, r);
  tprintf("lock-client\tid:%s\tretry lock:%ld\tdebug2\n",id.c_str(),lid);

  pthread_mutex_lock(&mymutex);

  //check state
  if(r ==lock_protocol::OK && (lockmap[lid].state==LOCKED || lockmap[lid].state==RELEASING)){
    //mutiple retry error
    tprintf("lock-client\tid:%s\tretry lock:%ld\tretry error on holding lock state:%d\n",id.c_str(),lid,lockmap[lid].state);
    pthread_mutex_unlock(&mymutex);
    return ret;
  }
  if(r == lock_protocol::OK && lockmap[lid].state==NONE){
    //mutiple retry error
    tprintf("lock-client\tid:%s\tretry lock:%ld\tretry error on not wanting lock\n",id.c_str(),lid);
    pthread_mutex_unlock(&mymutex);
    cl->call(lock_protocol::release, lid, id, r);
    return ret;
  }
  //check queue
  if(r == lock_protocol::OK && lockmap[lid].wait_queue.empty()){
    //mutiple retry error
    tprintf("lock-client\tid:%s\tretry lock:%ld\tretry error on nobody waiting\n",id.c_str(),lid);
    lockmap[lid].state == NONE;
    pthread_mutex_unlock(&mymutex);
    cl->call(lock_protocol::release, lid, id, r);
    return ret;
  }

  if(r != lock_protocol::OK){
    tprintf("lock-client\tid:%s\tretry lock:%ld\tretry error state:%d\tclient state:%d\n",id.c_str(),lid,r,lockmap[lid].state);
    
    pthread_mutex_unlock(&mymutex);
    r = cl->call(lock_protocol::stat, lid, id, r);
    if(r != lock_protocol::OK){
      tprintf("lock-client\tid:%s\tretry lock:%ld\tnot lock owner\n",id.c_str(),lid);
      return ret;
    }
    pthread_mutex_lock(&mymutex);

    if((lockmap[lid].state==LOCKED || lockmap[lid].state==RELEASING)){
      //mutiple retry error
      tprintf("lock-client\tid:%s\tretry lock:%ld\tretry error on holding lock state:%d\n",id.c_str(),lid,lockmap[lid].state);
      pthread_mutex_unlock(&mymutex);
      return ret;
    }
    if(lockmap[lid].wait_queue.empty()){
      //mutiple retry error
      tprintf("lock-client\tid:%s\tretry lock:%ld\tretry error on not wanting lock\n",id.c_str(),lid);
      lockmap[lid].state == NONE;
      pthread_mutex_unlock(&mymutex);
      cl->call(lock_protocol::release, lid, id, r);
      return ret;
    }
  }

  

  tprintf("lock-client\tid:%s\tretry lock:%ld\tdebug3\n",id.c_str(),lid);
  if(lockmap[lid].wait_queue.empty()){
    pthread_mutex_unlock(&mymutex);
    cl->call(lock_protocol::release, lid, id, r);
    return ret;
  }
  tid next = lockmap[lid].wait_queue.front();
  lockmap[lid].wait_queue.pop();
  tprintf("lock-client\tid:%s\tretry lock:%ld\tnext owner:%ld\t%d waiting\n",id.c_str(),lid,next,lockmap[lid].wait_queue.size());
  if(lockmap[lid].revoke_call){
    tprintf("lock-client\tid:%s\tretry lock:%ld\tresume revoke\n",id.c_str(),lid);
    lockmap[lid].state = RELEASING;
    //lockmap[lid].revoke_call = false;
  }
  else{
    lockmap[lid].state = LOCKED;
  }
  lockmap[lid].owner = next;
  pthread_cond_broadcast(&cv);
  pthread_mutex_unlock(&mymutex);

  return ret;
}



