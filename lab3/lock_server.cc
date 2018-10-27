// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

pthread_mutex_t mutex_map;
pthread_mutex_t mutex_queue;


lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&mutex_map, NULL);
  pthread_mutex_init(&mutex_queue, NULL);
  
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here

  printf("client %d acquire lock %d\n",clt,lid);
  pthread_mutex_lock(&mutex_map);
  if(lock_map.find(lid) == lock_map.end()){ //new lock
    printf("new lock %d\n",lid);
    lock_map.insert(pair<lock_protocol::lockid_t,int>(lid,clt));
    printf("client %d get lock %d\n",clt,lid);
    pthread_mutex_unlock(&mutex_map); 
  }
  else if(lock_map[lid] == 0){ //free lock
    printf("owner of lock %d is %d\n",lid,lock_map[lid]);
    lock_map[lid] = clt;
    printf("client %d get lock %d\n",clt,lid);
    pthread_mutex_unlock(&mutex_map); 
  }
  else if(lock_map[lid] == clt){ //acquire same lock
    pthread_mutex_unlock(&mutex_map); 

    pthread_mutex_lock(&mutex_queue);
    printf("clt %d acquire lock %d again\n",clt,lid);
    if(wait_queue.find(lid) == wait_queue.end()){
      queue<int> q;
      q.push(clt);
      wait_queue[lid] = q;
    }
    else{
      wait_queue[lid].push(clt);
    }
    int now = nacquire;
    pthread_mutex_unlock(&mutex_queue);

    while((now == nacquire) || (lock_map[lid] != clt))
      ;//spin wait
    printf("client %d get lock %d\n",clt,lid);
  }
  else{ //lock engaged
    pthread_mutex_unlock(&mutex_map); 

    pthread_mutex_lock(&mutex_queue);
    printf("queue size of lock %d: %d\n",lid, wait_queue[lid].size());
    if(wait_queue.find(lid) == wait_queue.end()){
      queue<int> q;
      q.push(clt);
      wait_queue[lid] = q;
    }
    else{
      wait_queue[lid].push(clt);
    }
    pthread_mutex_unlock(&mutex_queue);

    while(lock_map[lid] != clt){
      //printf("owner of lock %d is %d\n",lid,lock_map[lid]);
      //printf("client %d wait for lock %d\n",clt,lid);
    }//spin wait
    printf("client %d get lock %d\n",clt,lid);
  }
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here

  pthread_mutex_lock(&mutex_queue);
  nacquire += 1;
  printf("client %d release lock %d\n",clt,lid);
  if(wait_queue[lid].empty()){ //no one waits for the lock
    printf("nobody waits for lock %d\n",lid);
    lock_map[lid] = 0;
  }
  else{ //some one waits for the lock
    int cltid = wait_queue[lid].front();
    wait_queue[lid].pop();
    lock_map[lid] = cltid;
    printf("client %d will get lock %d\n",cltid,lid);
  }
  pthread_mutex_unlock(&mutex_queue);

  return ret;
}
