#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));  
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf,(char*)blocks[id],BLOCK_SIZE);
  buf[BLOCK_SIZE] = '\0';
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  //memcpy(blocks[id], buf, BLOCK_SIZE);
  //printf("mem1\n");
  
  memcpy((char*)blocks[id], buf, BLOCK_SIZE);
  //memmove((char*)blocks[id], buf, BLOCK_SIZE);
  //printf("mem2\n");
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  uint32_t nblocks = sb.nblocks;
  uint32_t min_num = IBLOCK(sb.ninodes,sb.nblocks)+1;
  char buf[BLOCK_SIZE];

  for (uint32_t i=min_num;i<nblocks;i++){
    uint32_t map_block = BBLOCK(i);
    read_block(map_block,buf);
    int offset = i % BLOCK_SIZE;
    if(buf[offset] == 0){ //empty block
      buf[offset] = 1;    //mark the bit
      write_block(map_block,buf);
      return i;
    }
  }

  printf("Block out of range!\n");
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  char buf[BLOCK_SIZE];
  uint32_t map_block = BBLOCK(id);
  read_block(map_block,buf);
  int offset = id % BLOCK_SIZE;
  buf[offset] = 0;    //umark the bit
  write_block(map_block,buf);   
  
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

unsigned int
inode_manager::get_time()
{
  time_t seconds;
  seconds = time(NULL);
  unsigned int ss = (unsigned int)seconds;
  return ss;
}

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  uint32_t inum;
  struct inode* ino;
  uint32_t ninodes = bm->sb.ninodes;
  for(uint32_t i=1;i <= ninodes;i++){
    if(get_inode(i) == NULL){
      inum = i;
      ino = (struct inode*)malloc(sizeof(struct inode));
      ino->type = type;     
      ino->size = 0; 
      ino->ctime = get_time();
      
      put_inode(inum,ino);
      return inum;
    }
  }
  printf("inode out of range!\n");
  return 0;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */

  struct inode* ino = get_inode(inum);
  if(ino == NULL){
    return;
  }
  remove_file(inum);
  put_inode(inum,ino);
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }
  //printf("get 1\n");
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  //printf("get 2\n");

  ino_disk = (struct inode*)buf + inum%IPB;
  //printf("get 3\n");
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }
  //printf("get 4\n");
  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;
  //printf("get 5\n");
  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  //printf("put 1\n");
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  //printf("put 2\n");
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  //printf("ino_disk size: %d\n",ino_disk->size);
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
  //printf("put 3\n");
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  struct inode* ino = get_inode(inum);
  ino->atime = get_time();
  put_inode(inum,ino);
  char buf[BLOCK_SIZE];
  

  int left = ino->size;
  printf("inode file %d (need to read)size %d\n",inum, left);
  int total = 0;

  //char buffer[left+1];
  int blocks = left / BLOCK_SIZE + (left % BLOCK_SIZE > 0);

  //printf("malloc error\n");
  char* buffer = (char*)malloc(blocks*BLOCK_SIZE);
  //char* buffer = (char*)malloc(left);
  memset(buffer,0,blocks*BLOCK_SIZE);
  char* pp = buffer;

  //read from direct blocks
  for (int i=0;i<NDIRECT;i++){
    if(left <= 0){
      break;
    }
    memset(buf,0,BLOCK_SIZE);
    bm->read_block(ino->blocks[i],buf);
    
    int read = strlen(buf);
    printf("it:%d  id :%d read:%s\n",i,ino->blocks[i],buf);
    
    if(read<BLOCK_SIZE && left-read>0){
      read = BLOCK_SIZE;
    }    
    memcpy(pp,buf,read);
    left -= read;
    total += read;
    pp += read;
    //printf("left:%d\n",left);
  }

  //read from indirect blocks
  if(left>0){
    char tmp[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT],tmp);    
    uint* ind = (uint*)tmp; 
    //printf("indirect block num:%d\n",ino->blocks[NDIRECT]);  
    //printf("indirect block content:%s\n",tmp);

    const char *sep = ";"; //different content is divided by ?
    char *num;
    num = strtok(tmp, sep);
    blockid_t id = atoi(num);
    for(int i=0;i<NINDIRECT;i++){
      if(left <= 0){
        break;
      }    

      //bm->read_block(ind[i],buf);
      bm->read_block(id,buf);
      printf("it:%d  \nread:%s\n",i+NDIRECT,buf);
      //printf("read from indirect blocks %d\n",id);
      
      //int read = MIN(left,BLOCK_SIZE);
      int read = strlen(buf);
     
      if(read<BLOCK_SIZE && left-read>0){
        read = BLOCK_SIZE;
      }  
      memcpy(pp,buf,read);
      left -= read;
      total += read;
      pp += read;
      //printf("left:%d\n",left);
      if(left>0){
        num = strtok(NULL, sep);
        id = atoi(num);
      }
    }
  }

 
  *buf_out = buffer;
  
  
  *size = total;
  
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  if(size > MAXFILE*BLOCK_SIZE){
    printf("File too large!\n");
    return;
  }

  printf("need to write to ino %d size: %d \n",inum, size);
  struct inode* ino = get_inode(inum);
  int left = size;
  int total = 0;
  int init_size = ino->size;

  char* ptr = (char *)buf;
  if(init_size!=size){
    ino->ctime = get_time();
  }

  //free blocks if new size is smaller than original size
  if(init_size > size){
    int off1 = size / BLOCK_SIZE + (size%BLOCK_SIZE>0);
    int off2 = init_size / BLOCK_SIZE + (init_size%BLOCK_SIZE>0);
    if(off2 < NDIRECT){
      for(int i=off1;i<off2;i++){
        bm->free_block(ino->blocks[i]);
      }
    }
    else if(off1 < NDIRECT){
      for(int i=off1;i<NDIRECT;i++){
        bm->free_block(ino->blocks[i]);
      }
      char tmp[BLOCK_SIZE];
      bm->read_block(ino->blocks[NDIRECT],tmp);  

      const char *sep = ";"; //different content is divided by ?
      char *num;
      num = strtok(tmp, sep);
      blockid_t id;
      while(num != NULL){
        id = atoi(num);
        num = strtok(NULL, sep); 
        bm->free_block(id);  
      }
      bm->free_block(ino->blocks[NDIRECT]);
    }
  }

  //write to direct blocks
  for (int i=0;i<NDIRECT;i++){
    if(left <= 0){
      break;
    }

    //new size is larger than original size
    if(total >= init_size){
      blockid_t bid = bm->alloc_block();
      ino->blocks[i] = bid;
    }

    int write = MIN(left,BLOCK_SIZE);
    
    
    char ww[BLOCK_SIZE];
    bzero(ww, BLOCK_SIZE);
    memcpy(ww,ptr,write);
    bm->write_block(ino->blocks[i],ww);

    /* test */
    char bbb[BLOCK_SIZE];
    bm->read_block(ino->blocks[i],bbb);
    printf("write it:%d id:%d content:%s\n",i,ino->blocks[i],bbb);
    //bm->write_block(ino->blocks[i],ptr);
    printf("over\n");
    ptr += BLOCK_SIZE;
    left -= write;
    total += write;       
  }
  
  //write to indirect blocks
  if(left>0){
    if(init_size <= NDIRECT*BLOCK_SIZE){
      ino->blocks[NDIRECT] = bm->alloc_block();
    }
    char tmp[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT],tmp);  

    char nums[BLOCK_SIZE];
    memset(nums,0,BLOCK_SIZE);  
    memcpy(nums,tmp,BLOCK_SIZE);

    printf("indirect block num:%d\n",ino->blocks[NDIRECT]);  
    printf("indirect block content:%s\n",tmp);

    const char *sep = ";"; //different content is divided by ?
    char *num;
    num = strtok(tmp, sep);
    blockid_t id;
    for(int i=0;i<NINDIRECT;i++){
      if(left <= 0){
        break;
      }   

      if(total >= init_size){
        id = bm->alloc_block();
        char pp[10];
        sprintf(pp,"%d;",id);
        strcat(nums,pp);
      }
      else{
        id = atoi(num);
        num = strtok(NULL, sep);        
      }

      int write = MIN(left,BLOCK_SIZE);
      char ww[BLOCK_SIZE];
      bzero(ww, BLOCK_SIZE);
      memcpy(ww,ptr,write);

      printf("write it:%d id:%d content:%s\n",i+NDIRECT,id,ww);
      //bm->write_block(ino->blocks[i],ww);
      bm->write_block(id,ww);
      //printf("it %d  :write to indirect block %d\n",NDIRECT+i,ind[i]);
      ptr += BLOCK_SIZE;
      left -= write;
      total += write; 
    }

    /* new size is smaller than old size*/
    while(num != NULL){
      id = atoi(num);
      num = strtok(NULL, sep); 
      bm->free_block(id);  
    }

    printf("indirect block content:%s\n",nums);
    bm->write_block(ino->blocks[NDIRECT],nums);
    //printf("indirect block content:%s\n",nums);
  }  
  
  ino->size = total;
  ino->mtime = get_time();
  put_inode(inum,ino);
  //printf("write to inode size: %d\n",get_inode(inum)->size);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  struct inode* ino = get_inode(inum);
  if(ino == NULL){
    a.type = 0;
    return;
  }
  a.atime = ino->atime;
  a.ctime = ino->ctime;
  a.mtime = ino->mtime;
  a.size = ino->size;
  a.type = ino->type;
  
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */

  struct inode* ino = get_inode(inum);
  int size = ino->size;
  if(size <= 0){
    ino->size = 0;
    ino->type = 0;
    put_inode(inum, ino);
    return;
  }
  int off = size / BLOCK_SIZE + (size%BLOCK_SIZE>0);
  if(off < NDIRECT){
    for(int i=0;i<off;i++){
      bm->free_block(ino->blocks[i]);
    }
  }
  else{
    for(int i=0;i<NDIRECT;i++){
      bm->free_block(ino->blocks[i]);
    }
    char tmp[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT],tmp);  

    const char *sep = ";"; //different content is divided by ?
    char *num;
    num = strtok(tmp, sep);
    blockid_t id;
    while(num != NULL){
      id = atoi(num);
      num = strtok(NULL, sep); 
      bm->free_block(id);  
    }
    bm->free_block(ino->blocks[NDIRECT]);
  }
  ino->size = 0;
  ino->type = 0;
  put_inode(inum, ino);
  
  return;
}
