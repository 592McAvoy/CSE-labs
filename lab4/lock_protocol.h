// lock protocol

#ifndef lock_protocol_h
#define lock_protocol_h

#include "rpc.h"

enum lock_state { NONE, FREE, LOCKED, ACQUIRING, RELEASING, ASSIGNED };

class lock_protocol {
 public:
  enum xxstatus { OK=1, RETRY, RPCERR, NOENT, IOERR, GRANTED };
  typedef int status;
  typedef unsigned long long lockid_t;
  enum rpc_numbers {
    acquire = 0x7001,
    release,
    stat
  };
};

class rlock_protocol {
public:
    enum xxstatus { OK=6, RPCERR };
    typedef int status;
    enum rpc_numbers {
        revoke = 0x8001,
        retry = 0x8002,
        stat
    };
};

#endif 
