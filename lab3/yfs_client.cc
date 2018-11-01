// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;

int create_mutex = -1;

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  //lc = new lock_client(lock_dst);
  lc = new lock_client_cache(lock_dst);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
}


yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

//util function
bool 
yfs_client::isValid(const char* name)
{
    if(strchr(name,'\\')||strchr(name,'/')||\
    strchr(name,'?')||strchr(name,':')||\
    strchr(name,'*')||strchr(name,'\"')||\
    strchr(name,'<')||strchr(name,'>')){
        return false;
    }
    return true;
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    //return ! isfile(inum);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a dir\n", inum);
    return false;
}

bool
yfs_client::issymlink(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    //return ! isfile(inum);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    } 
    printf("issymlink: %lld is not a symlink\n", inum);
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)



// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    printf("====================set attr==================\n");
    string buf;

    lc->acquire(ino);
    r = ec->get(ino, buf);
    if( r != OK){
        return r;
    }

    int init = buf.length();//init size
    printf("init size:%d new size:%d\n",init,size);
    
    if(size == init){
    }
    if(size < init){
        string newbuf = buf.substr(0,size);
        r = ec->put(ino,newbuf);
        if(r != OK){
            return r;
        }
    }
    if(size > init){
        char newbuf[size];
        memset(newbuf,0,size);
        strcat(newbuf,buf.c_str());
        r = ec->put(ino,newbuf); 
        if(r != OK){
            return r;
        }
    }
    lc->release(ino);
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    printf("====================create file %s==================\n",name);
    if(!isValid(name)){
        printf("filename %s is not valid\n",name);
        return RPCERR;
    }
    bool found;

    lc->acquire(create_mutex);
    lookup(parent,name,found,ino_out);
    if(found){
        printf("file %s has existed\n",name);
        lc->release(create_mutex);
        return EXIST;
    }

    //cteate file
    if(ec->create(2,ino_out) != OK){
        lc->release(create_mutex);
        return r;
    }
    printf("new file ino: %d\n",ino_out);

    //modify parents
    string buf;
    lc->acquire(parent);
    r = ec->get(parent,buf);    
    if(r != OK){
        lc->release(parent);
        return r;
    }
    
;
    if(buf.length()>0){
        buf += "?";
    }
    buf += string(name);
    buf += ":";
    buf += filename(ino_out);
    
    //printf("new buf\n%s\n",buf.c_str());
    r = ec->put(parent,string(buf));
    lc->release(parent);
    lc->release(create_mutex);
    if(r != OK){
        return r;
    }

    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    printf("====================make dir %s==================\n",name);
    if(!isValid(name)){
        printf("filename %s is not valid\n",name);
        return RPCERR;
    }
    bool found;

    lc->acquire(create_mutex);
    lookup(parent,name,found,ino_out);
    if(found){
        printf("dir %s has existed\n",name);
        lc->release(create_mutex);
        return EXIST;
    }

    //cteate dir
    r = ec->create(1,ino_out);
    if(r != extent_protocol::OK){
        lc->release(create_mutex);
        return r;
    }    

    //modify parents
    string buf;
    lc->acquire(parent);
    r = ec->get(parent,buf);
    if( r != OK){
        lc->release(parent);
        return r;
    }
    
;
    if(buf.length()>0){
        buf += "?";
    }
    buf += string(name);
    buf += ":";
    buf += filename(ino_out);
    
    //printf("new buf\n%s\n",buf.c_str());
    r = ec->put(parent,string(buf));
    lc->release(parent);
    lc->release(create_mutex);
    if( r != extent_protocol::OK){
        return r;
    }


    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    /*
    * ============ my fomat of directory content =============
    * | filename1 : inoNum1 ? filename2 : inoNum2 ? -------- |
    */


    printf("====================look up %s==================\n",name);
    if(!isdir(parent)){
        found = false;
        printf("--------parent not a dir -------\n");
        return RPCERR;
    }
    string file_name = string(name);
    
    list<dirent> list;
    readdir(parent,list);
    int size = (int)list.size();
    printf("list in lookup size:%d\n",size);
    for(int i=0;i<size;i++){
        dirent dd = list.front();
        list.pop_front();
        //printf("filename:\n%s\n",dd.name.c_str());
        //printf("searchname:\n%s\n",name);
        if(file_name == dd.name){
            found = true;
            ino_out = dd.inum;
            return OK;
        }
    }
    found = false;
    printf("------parent doesn't have file %s\n",name);
    return NOENT;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    printf("====================read dir ==================\n");

    if(!isdir(dir)){
        printf("this is not a dir!!\n");
        return RPCERR;
    }

    string buf;

    lc->acquire(dir);
    r = ec->get(dir,buf);
    lc->release(dir);
    if(r != extent_protocol::OK){
        return r;
    }

    //printf("----------finish get-----------\n");
    char * content = new char[strlen(buf.c_str())+1];
    strcpy(content, buf.c_str());
    //printf("content:\n%s\n",content);
    const char *sep = "?"; //different content is divided by ?
    char *tmp;
    tmp = strtok(content, sep);
    while(tmp){
        //printf("total content:\n%s \n", tmp);
        string total = string(tmp);
        int pos = total.find(':');//[filename:inoNum]
        dirent dd;
        dd.inum = n2i(total.substr(pos+1));
        dd.name = total.substr(0,pos);

        list.push_back(dd);
        //printf("list in readdir size:%d\n",(int)list.size());
        tmp = strtok(NULL, sep);//get next content
    }
    printf("list in readdir size:%d\n",(int)list.size());
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */

    printf("====================read==================\n");

    string buf;

    lc->acquire(ino);
    r = ec->get(ino,buf);
    lc->release(ino);
    if(r != extent_protocol::OK){
        return r;
    }
    int len = buf.length();
    //cout<<"read:\n"<<buf<<endl;
    //printf("init size:%d\n",len);
    //printf("offset:\t%d\nread size:%d\n",off,size);
    if(off>len){
        data = "\0";
    }
    else if(off+size > len){
        data = buf.substr(off);
    }
    else{
        data = buf.substr(off,size);
    }
    //cout<<"output:\n"<<data<<endl<<"size:"<<data.size()<<endl;
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    printf("===================write==================\n");
    string buf;
    lc->acquire(ino);
    r = ec->get(ino,buf);
    if(r != extent_protocol::OK){
        lc->release(ino);
        return r;
    }
    //cout<<"read :\n"<<buf<<endl;
    int len = buf.length();
    
    string final;
    //printf("init content:\n%s\n",buf.c_str());
    //printf("init size:%d\n",len);
    //printf("offset:\t%d\ndata size:%d\n",off,size);

    string tmp(size,0);
    for(int i=0;i<size;i++){
        tmp[i] = data[i];
    }

    if(off>len){
        //char w_buf[off];
        //memset(w_buf,0,off);
        //strcat(w_buf,buf.c_str());
        final = buf;
        for(int i=len;i<off;i++){
            final += '\0';
        }
        //printf("----------after loop, final size:%d\n",final.length());
        final += tmp;
    }
    else if(off+size>len){
        final = buf.substr(0,off)+tmp;
    }
    else{
        final = buf.substr(0,off)+tmp + buf.substr(off+size);
    }
    
    //printf("after size:%d\n",final.length());
    //printf("-----------final content----------\n");
    //cout<<final<<endl;

    r = ec->put(ino,final);
    lc->release(ino);
    if(r != OK){
        return r;
    }    

    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    printf("============unlink %s==================\n",name);
    inum ino;
    bool found;
    lookup(parent,name,found,ino);
    if(!found){
        printf("%s does not exist\n");
        return ENOENT;
    }
    if(isdir(ino)){
        printf("can not unlink a directory\n");
        return RPCERR;
    }

    //delete entry from parent
    string buf;

    lc->acquire(parent);
    r = ec->get(parent,buf);
    if(r != OK){
        lc->release(parent);
        return r;
    }
    string dele = string(name) + ":" +filename(ino);
    //printf("delete content:\n%s\n",dele.c_str());
    size_t pos = buf.find(dele);
    string final;
    int len;
    if(pos == 0){
        len = dele.length();
        final = buf.substr(len);
    }else{
        len = dele.length()+1;
        final = buf.substr(0,pos-1)+buf.substr(pos+len-1);
    }
    //printf("final content:\n%s\n",final.c_str());
    r = ec->put(parent,final);
    lc->release(parent);
    if(r != OK){
        return r;
    }

    lc->acquire(ino);
    r = ec->remove(ino);
    lc->release(ino);
    if(r != OK){
        return r;
    }


    return r;
}

int
yfs_client::symlink(const char* link, inum parent, const char* name, inum &ino)
{
    int r = OK;
    printf("====================symlink %s==================\n",name);
    if(!isValid(name)){
        printf("filename %s is not valid\n",name);
        return RPCERR;
    }
    bool found;
    lc->acquire(create_mutex);
    lookup(parent,name,found,ino);
    if(found){
        printf("file %s has existed\n",name);
        lc->release(create_mutex);
        return EXIST;
    }
    //cteate symlink inode
    
    r = ec->create(3,ino);
    if(r != OK){
        lc->release(create_mutex);
        return r;
    }
    string content = string(link);
    printf("link content:\n%s\n",link);
    r = ec->put(ino,content);
    if(r != OK){
        lc->release(create_mutex);
        return r;
    }

    //modify parents
    string buf;
    lc->acquire(parent);
    r = ec->get(parent,buf);
    if(r != OK){
        lc->release(parent);
        return r;
    }    
;
    if(buf.length()>0){
        buf += "?";
    }
    buf += string(name);
    buf += ":";
    buf += filename(ino);
    
    //printf("new buf\n%s\n",buf.c_str());
    r = ec->put(parent,string(buf));
    lc->release(parent);
    lc->release(create_mutex);
    if(r != OK){
        return r;
    }
  

    return OK;
}
 
 
int
yfs_client::readlink(inum ino, string &link)
{
    int r = OK;
    printf("====================readlink ==================\n");
    if(!issymlink(ino)){
        printf("not a symbol link!\n");
        return RPCERR;
    }

    lc->acquire(ino);
    r = ec->get(ino,link);
    lc->release(ino);
    if( r != OK){
        return r;
    }
    printf("get link:\n%s\n",link.c_str());

    return OK;
    
}

