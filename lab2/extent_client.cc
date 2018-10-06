// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
} 

// a demo to show how to use RPC
extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::create, type, id);
  if (ret != extent_protocol::OK){
    printf("ret ERROR in create: %d\n",ret);
    ret = extent_protocol::RPCERR;
  }
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::get, eid, buf);
  if (ret != extent_protocol::OK){
    printf("ret ERROR in get: %d\n",ret);
    ret = extent_protocol::RPCERR;
  }
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::getattr, eid, attr);
  if (ret != extent_protocol::OK){
    printf("ret ERROR in getattr: %d\n",ret);
    ret = extent_protocol::RPCERR;
  }
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  int r;
  ret = cl->call(extent_protocol::put, eid, buf, r);
  if (ret != extent_protocol::OK){
    printf("ret ERROR in put: %d\n",ret);
    ret = extent_protocol::RPCERR;
    //ret = cl->call(extent_protocol::put, eid, buf, r);
  }
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  int r;
  ret = cl->call(extent_protocol::remove, eid, r);
  if (ret != extent_protocol::OK){
    printf("ret ERROR in remove: %d\n",ret);
    ret = extent_protocol::RPCERR;
  }
  return ret;
}


