#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <openssl/sha.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <bits/stdc++.h>
#define MAPPER_COUNT 5
#define REDUCER_COUNT 3
pthread_mutex_t lock; 

void master_thread(std::string);
void mapper_thread(std::string,int,std::string,int);
void reducer_thread(std::string,int,std::string);
void master_to_mapper_thread(int,struct sockaddr_in client_addr);
void init_system()
{
    int port=8080;
    std::string ip="";
     std::cout<<"----------------------------------------------\n";
    std::cout<<"init system"<<std::endl; 
    for(int i=2;i<=6;i++)
    {
        ip="127.0.0."+std::to_string(i);
        std::thread mapper(mapper_thread,ip,i+port,"mapper_"+(std::to_string(i-1)),i-1);
        mapper.detach();
    }
     std::cout<<"----------------------------------------------\n";
    /*for(int i=7;i<=9;i++)
    {
        std::thread reducer(reducer_thread,"127.0.0."+(i+'0'),i+port,"reducer_"+(i-6));
        reducer.detach();
    }*/
}
void init_master_connection(int *sockfd)
{
    int new_socket_fd;
    socklen_t clilen;
    char buffer[1024]; 
    struct sockaddr_in self_addr; 
    if((*sockfd=socket(AF_INET, SOCK_STREAM,0))<0)
    { 
        std::cout<<"socket creation failed"<<"\n"; 
        close(*sockfd);
        exit(0);
    } 
    memset(&self_addr, 0, sizeof(self_addr)); 
    self_addr.sin_family=AF_INET;
    self_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    self_addr.sin_port = htons(8080); 
    if(bind(*sockfd, (struct sockaddr *)&self_addr,sizeof(self_addr))<0)
    { 
        std::cout<<"TCP socket bind failed"<<"\n";
        close(*sockfd);
        exit(0);
    }
}
int getWordsCount(std::string file_name)
{
    std::string str;
    int count=0;
    std::ifstream in;
    in.open(file_name);
    while(in>>str)
        count++;
    in.close();
    return count;
}
int calhash(std::string str)
{
    int hash = 1;
    for (int i = 0; i < str.length(); i++) 
    {
        hash += pow(37,i)*str[i];
    }
    if(hash<0)
    hash+=10000;
return hash%3;
}
void master_thread(std::string file_name)
{
        int sockfd=0;
        socklen_t clilen;
        struct sockaddr_in client_addr; 
        char buffer[1024];
        int word_count=getWordsCount(file_name);
         std::cout<<"----------------------------------------------\n";
        std::cout<<"words in file:"<<word_count<<std::endl;
        init_master_connection(&sockfd);
        
        ////////// file partitioning//////////////
        int chunk_size=word_count/MAPPER_COUNT;
        std::ifstream in;
        std::ofstream out;
         in.open("input.txt");
        for(int i=1;i<=MAPPER_COUNT;i++)
        {
            std::string str;
            int count=0;
            std::string filename="mapper/mapper_";;
            out.open(filename+std::to_string(i));
            while(in>>str && count<chunk_size)
            {
                {
                    out<<str<<",1"<<"\n";
                 }
                    count++;
            }
            out.close();
          
        }
          in.close();
        std::cout<<"-------------------------\n";
        std::cout<<"File partitoning done\n";
        std::cout<<"-------------------------\n";
         std::cout<<"----------------------------------------------\n";
        /////////////////////////////////////////////////
        while(true)
        {
            std::cout<<"master listening... "<<std::endl;
            listen(sockfd,10);
            int new_socket_fd= accept(sockfd,(struct sockaddr *) &client_addr,&clilen);
            std::thread master_to_mapper(master_to_mapper_thread,new_socket_fd,client_addr);
            master_to_mapper.detach();
           
        }
        
       std::cout<<"master exited,task completed.."<<std::endl; 
}
void mapper_thread(std::string ip,int port,std::string thread_name,int thcount)
{
    pthread_mutex_lock(&lock); 
     std::cout<<"----------------------------------------------\n";
    std::cout<<"mutex held by:"<<thread_name<<std::endl;
    struct sockaddr_in master_addr,self_addr;  
    char buffer[1024];
    pthread_setname_np(pthread_self(), thread_name.c_str());
    int sockfd=socket(AF_INET, SOCK_STREAM, 0);
    self_addr.sin_family = AF_INET; 
    self_addr.sin_addr.s_addr = inet_addr(ip.c_str()); 
    self_addr.sin_port =htons(port); 
    bind(sockfd, (struct sockaddr *)&self_addr,sizeof(self_addr));
    
    if(sockfd<0) 
    {
        std::cout<<"failed to open socket"<<"\n";
        close(sockfd);
        exit(0);
    }
    bzero((char *) &master_addr, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    master_addr.sin_addr.s_addr=inet_addr("127.0.0.1");
    master_addr.sin_port = htons(8080);

    ///////////////////////////////////

   
     ////////////////////
    std::fstream fileh;
    std::string word;
    std::vector<std::string>names;
    
        std::string filename="mapper/mapper_"+std::to_string(thcount);
        fileh.open(filename,std::fstream::in);
         while(getline(fileh, word))
            names.push_back(word);
             fileh.close();
         sort(names.begin(), names.end());
         fileh.open(filename,std::fstream::out);
         for(int i=0;i<names.size();i++)
         {           
              fileh<<names[i]<<"\n";
         }
        fileh.close();
            
            ////////////////////
            ///////////////////
    std::ifstream in;
    in.open("mapper/mapper_"+std::to_string(thcount));
    std::string str;
  
    ///////////////////
    while(in>>str)
    {
        std::vector<std::string> tokens;
		tokens.clear();
		std::stringstream check1(str);
		std::string rawcode;
		while(getline(check1, rawcode, ','))
		{
			tokens.push_back(rawcode);
		}
         int reducer_num=calhash(tokens[0]);
         std::string filename="reducer/mapper_"+std::to_string(thcount)+"/red_"+std::to_string(abs(reducer_num));
         std::ofstream out;
         out.open(filename, std::fstream::out |std::fstream::app);
         out<<str<<"\n";
         out.close();
    }
    in.close();
    ///////////////////////////////////

    /* connect call to master */    
    if (connect(sockfd,(struct sockaddr *) &master_addr,sizeof(master_addr)) < 0) 
    {
        std::cout<<"Failed to connect to master"<<"\n";
        close(sockfd);
        exit(0);
    }
    /* reading input string from client */
   // bzero(buffer,1024);
    int i;
    for(i=0;i<thread_name.size();i++)
    {
        buffer[i]=thread_name[i];
    }   
    std::string cont;
    cont="file reduced";
    for(int j=0;j<cont.size();j++)
    {
        buffer[i]=cont[j];
        i++;
    }   
    int n=write(sockfd,buffer,strlen(buffer));
   
    if(n>0)
    {
        bzero(buffer,1024);
        std::cout<<thread_name<<":waiting for master reply"<<"\n";
        n=read(sockfd,buffer,1024);
        if(n<0)
            std::cout<<"No message from server"<<"\n";
        else
            std::cout<<"message from master to :"<<thread_name<<":"<<buffer<<std::endl;
    }
    ///////////////////////////
   
    ////////////////////////////
    pthread_mutex_unlock(&lock); 
    std::cout<<"mutex released by:"<<thread_name<<std::endl;
    /* actual work here  */
    close(sockfd);
    std::cout<<thread_name<<" thread closed "<<std::endl;
     std::cout<<"----------------------------------------------\n";
}
void master_to_mapper_thread(int new_socket_fd,struct sockaddr_in client_addr)
{
    std::cout<<"new connection accepted"<<std::endl;
    std::cout<<"from IP:"<<inet_ntoa(client_addr.sin_addr)<<std::endl;
    std::cout<<"from port:"<<ntohs(client_addr.sin_port)<<std::endl;
    char buffer[1024];
    int r=read(new_socket_fd,buffer,1024);
    if(r>0)
    {
        std::cout<<"-------------------------\n";
        std::cout<<buffer<<std::endl;
         std::cout<<"-------------------------\n";
        bzero(buffer,1024);
        buffer[0]='h';
        buffer[1]='e';
        buffer[2]='l';
        buffer[3]='l';
        buffer[4]='o';
        write(new_socket_fd,buffer,strlen(buffer));
    }
    else
        std::cout<<"received nothing"<<std::endl;

    std::cout<<"master to mapper thread is finished"<<std::endl;
}
void reducer_thread(std::string IP,std::string port,std::string thread_name)
{
//    std::cout<<"reducer thread"<<"\n";  
}

int main(int argc, char const *argv[])
{
    std::thread master(master_thread,"input.txt");
    sleep(2);
    init_system();
    master.join();
    return 0;
}
