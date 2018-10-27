#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>


#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include <queue>
#include <map>

using namespace std;

typedef struct{
  lock_state state;
  string owner;
  queue<string> wait_queue;
}lock_info;


class lock_server_cache {
 private:
  int nacquire;
  map<lock_protocol::lockid_t,lock_info> lockmap;
  
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
