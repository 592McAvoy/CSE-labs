#include "datanode.h"
#include <arpa/inet.h>
#include "extent_client.h"
#include <unistd.h>
#include <algorithm>
#include "threader.h"

using namespace std;

int DataNode::init(const string &extent_dst, const string &namenode, const struct sockaddr_in *bindaddr) {
  ec = new extent_client(extent_dst);

  // Generate ID based on listen address
  id.set_ipaddr(inet_ntoa(bindaddr->sin_addr));
  id.set_hostname(GetHostname());
  id.set_datanodeuuid(GenerateUUID());
  id.set_xferport(ntohs(bindaddr->sin_port));
  id.set_infoport(0);
  id.set_ipcport(0);

  // Save namenode address and connect
  make_sockaddr(namenode.c_str(), &namenode_addr);
  if (!ConnectToNN()) {
    delete ec;
    ec = NULL;
    return -1;
  }

  // Register on namenode
  if (!RegisterOnNamenode()) {
    delete ec;
    ec = NULL;
    close(namenode_conn);
    namenode_conn = -1;
    return -1;
  }

  /* Add your initialization here */
  NewThread(this, &DataNode::KeepAlive);

  return 0;
}

void DataNode::KeepAlive(){
  while(1){
    bool r = SendHeartbeat();
    string mes = r?"success":"failed";
    printf("datanode\tsend heartbeat\t%s\n",mes.c_str());fflush(stdout);
    sleep(1);//send once per second
  }
}

bool DataNode::ReadBlock(blockid_t bid, uint64_t offset, uint64_t len, string &buf) {
  /* Your lab4 part 2 code */
  printf("datanode\tReadBlock\tbid:%d\toff:%d\tlen:%d\n",bid,offset,len);fflush(stdout);
  extent_protocol::status ret;
  string tmp;
  ret = ec->read_block(bid, tmp);
  if(ret != extent_protocol::OK){
      return false;
  }
  buf = tmp.substr(offset, len);
  return true;
}

bool DataNode::WriteBlock(blockid_t bid, uint64_t offset, uint64_t len, const string &buf) {
  /* Your lab4 part 2 code */
  printf("datanode\tWriteBlock\tbid:%d\toff:%d\tlen:%d\n",bid,offset,len);fflush(stdout);
  //printf("\t\twrite content:%s\n",buf.c_str());
  extent_protocol::status ret;
  string tmp;
  ret = ec->read_block(bid, tmp);
  if(ret != extent_protocol::OK){
      return false;
  }
  tmp.replace(offset,len,buf);
  ret = ec->write_block(bid, tmp);
  if(ret != extent_protocol::OK){
      return false;
  }
  return true;
}

