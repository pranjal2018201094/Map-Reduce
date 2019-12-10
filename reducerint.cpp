#include<bits/stdc++.h>
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
#include<dirent.h>

//#include "master.hpp"
//#include "mapper.hpp"
//#include "reducer.hpp"
//#include "utilitiy.hpp"
#define MAPPER_COUNT 5
#define REDUCER_COUNT 3
int calhash(std::string str)
{
    long long  hash = 1;
    for (int i = 0; i < str.length(); i++) 
    {
        hash += pow(37,i)*str[i];
        hash%=3;
    }
   
return hash;
}
std::vector<std::string> tokenizer(std::string line,char delim)
{
    std::vector <std::string> tokens; 
    std::stringstream check1(line); 
    std::string intermediate; 
    while(std::getline(check1, intermediate,delim)) 
    { 
        tokens.push_back(intermediate); 
    }
    return tokens;
}

std::vector<std::string> getnames(std::string path) 
{

    DIR*    dir;
    dirent* pdir;
    std::vector<std::string> files;

    dir = opendir(path.c_str());

    while (pdir = readdir(dir)) {
        files.push_back(pdir->d_name);
    }
    
    return files;
}
void mapper_thread_inverted(std::string ip,int port,std::string thread_name,int thcount,std::vector<std::string>filelist,int l)
{
      
    //pthread_mutex_lock(&mapper_lock); 
    std::cout<<"----------------------------------------------\n";
    //mkdir("reducer", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    std::string red_path="reducer/"+thread_name;
    //mkdir(red_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    //std::cout<<"mutex held by:"<<thread_name<<std::endl;
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
    master_addr.sin_port = htons(8071);


    if (connect(sockfd,(struct sockaddr *) &master_addr,sizeof(master_addr)) < 0) 
    {
        std::cout<<"Failed to connect to master"<<"\n";
        close(sockfd);
        exit(0);
    }
    bzero(buffer,1024);
    //***changed strcpy     
    //strcpy(buffer,thread_name.c_str());
    strcpy(buffer,thread_name.c_str());
    int n=write(sockfd,buffer,strlen(buffer));
    //*********************FAULT************************

    std::cout<<"----------------------------------------------\n";
    //mkdir("reducer", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    //mkdir(red_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    //std::cout<<"mutex held by:"<<thread_name<<std::endl;
    struct sockaddr_in master_addr1,self_addr1;  
    char buffer1[1024];
    int sockfd1=socket(AF_INET, SOCK_STREAM, 0);
    self_addr1.sin_family = AF_INET; 
    self_addr1.sin_addr.s_addr = inet_addr(ip.c_str()); 
    self_addr1.sin_port =htons(8070); 
    bind(sockfd1, (struct sockaddr *)&self_addr1,sizeof(self_addr1));
    
    if(sockfd1<0) 
    {
        std::cout<<"failed to open socket"<<"\n";
        close(sockfd1);
        exit(0);
    }
    bzero((char *) &master_addr1, sizeof(master_addr1));
    master_addr1.sin_family = AF_INET;
    master_addr1.sin_addr.s_addr=inet_addr("127.0.0.1");
    master_addr1.sin_port = htons(8070);


    if (connect(sockfd1,(struct sockaddr *) &master_addr1,sizeof(master_addr1)) < 0) 
    {
        std::cout<<"Failed to connect"<<"\n";
        close(sockfd1);
        exit(0);
    }
    //int num = 2016; 
  
    // declaring output string stream 
    std::ostringstream str1; 
  
    // Sending a number as a stream into output 
    // string 
    str1 <<l; 

    bzero(buffer1,1024);
    //***changed strcpy     
    //strcpy(buffer,thread_name.c_str());
    std::string temp = str1.str();
    std::string send = "r"+temp;
    strcpy(buffer1,send.c_str());
    int n1=write(sockfd1,buffer1,strlen(buffer1));
    while(1)
    {
    /* reading input string from client */
        
        if(n>0)
        {
            bzero(buffer,1024);
            std::cout<<thread_name<<":waiting for master reply"<<"\n";
            n=read(sockfd,buffer,1024);
            //receives the work details
            if(n<0)
                std::cout<<"No message from server"<<"\n";
            else
            {
                std::string msg(buffer);
                std::vector<std::string> job = tokenizer(msg,'*');
              //std::string msgtosend = taskstr+"*"+job_idstr+"*"+reducernumberstr+"*"+folder+"*";
                
                std::stringstream task1 (job[1]); 
                int job_id = 0; 
                task1 >> job_id;

                std::stringstream task2 (job[2]); 
                int reducer_num = 0; 
                task2 >> reducer_num;

                //std::string folder = job[3];
                std::string foldertowork = job[1]+"mapout"+job[2];

                std::cout<<"Started===> Jobid: "<<job_id<<" reducer_task_num: "<< reducer_num<<std::endl;
                std::vector<std::string> dir = getnames(foldertowork);
                sort(dir.begin(),dir.end());
                std::string command ="cat "; 
                for(int i=2;i<dir.size();i++)
                {
                    command = command+foldertowork+"/"+dir[i]+" ";
                }
                std::string maked = job[1]+"Reducer"+job[2];

                mkdir(maked.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
                command = command +">"+maked+"/filetoreduce.txt";
                //std::string filetoopen =foldertowork+"/"+dir[reducer_num+1];
                system(command.c_str());
                std::string toreduce  =maked+"/filetoreduce.txt";
                std::string tosorted = maked+"/sorted.txt";
                std::string finalreduce = maked+"/reduced.txt";

                command = "sort "+toreduce+" >"+tosorted;
                system(command.c_str());


                std::ifstream ipfile (tosorted.c_str());
                std::string line;
                std::string word;
                std::string fn;
                std::string build;
                std::vector<std::string> tokens;
                int count =0;
                std::ofstream opfile (finalreduce.c_str());

                if(job[0]=="1")//wc
                {
                    
                    if(getline(ipfile, line))
                    {
                        word = line; //tokens[0];
                        count ++;
                    }
                    while (getline(ipfile, line))
                    {
                        if(line==word)
                        {
                            count ++;
                        }
                        else
                        {
                            std::ostringstream fsr6; 
                            fsr6 << (count);
                            std::string countstr  = fsr6.str();
                            build = word+" <"+countstr+">";
                            opfile<<build<<std::endl;
                            word = line;
                            count = 1;
                            //fn = tokens[1];
                            //build = word+":"+fn;
                        }
                    }
                    ipfile.close();
                    opfile.close();

                    std::cout<<"Finished===> Jobid: "<<job_id<<" reducer_task_num: "<< reducer_num<<std::endl;
                    bzero(buffer,1024);
                    strcpy(buffer,msg.c_str());
                    int n=write(sockfd,buffer,strlen(buffer));
                }
                else //inverted index
                {
                    // std::ifstream ipfile (tosorted.c_str());
                    // std::string line;
                    // std::string word;
                    // std::string fn;
                    // std::string build;
                    // std::vector<std::string> tokens;

                    // std::ofstream opfile (finalreduce.c_str());
                    if(getline(ipfile, line))
                    {
                        tokens=tokenizer(line,':');
                        word = tokens[0];
                        fn = tokens[1];
                        build = word+":"+fn;
                    }
                    while (getline(ipfile, line))
                    {
                        tokens=tokenizer(line,':');
                        if(tokens[0]==word)
                        {
                            build= build+tokens[1];
                        }
                        else
                        {
                            opfile<<build<<std::endl;
                            word = tokens[0];
                            fn = tokens[1];
                            build = word+":"+fn;
                        }
                    }
                    ipfile.close();
                    opfile.close();
                    
                    std::cout<<"Finished===> Jobid: "<<job_id<<" reducer_task_num: "<< reducer_num<<std::endl;
                    bzero(buffer,1024);
                    strcpy(buffer,msg.c_str());
                    int n=write(sockfd,buffer,strlen(buffer));

                }
                
            }
        }
    }
}


int main()
{

    std::string ip;
    int port = 8071;
    //std::cin>>ip;
    int i;
    
    std::cin>>i;
    int j= i;
    i=i+15;
    ip="127.0.0."+std::to_string(i);
    port = port + i;
    std::string thr = "reducer";
    std::vector<std::string>filel;
    std::string f1 = "given/split_1";
    filel.push_back(f1);
    mapper_thread_inverted(ip,port,thr,1,filel,j);
    return 0;
}