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
#include <sys/types.h>
//#include "master.hpp"
//#include "mapper.hpp"
//#include "reducer.hpp"
//#include "utilitiy.hpp"

#define MAPPER_COUNT 5
#define REDUCER_COUNT 3
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
void mapper_thread_inverted(std::string ip,int port,std::string thread_name,int thcount,std::vector<std::string>filelist,int j)
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
    str1 <<j; 

    bzero(buffer1,1024);
    //***changed strcpy     
    //strcpy(buffer,thread_name.c_str());
    std::string temp = str1.str();
    std::string send = "m"+temp;
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
                std::cout<<msg<<"TEST"<<std::endl;
                std::vector<std::string> job = tokenizer(msg,'*');
                //taskstr+"*"+job_idstr+"*"+mappernumberstr+"*"+folder+"*";
                std::stringstream task1 (job[1]); 
                int job_id = 0; 
                task1 >> job_id;

                std::stringstream task2 (job[2]); 
                int mapper_num = 0; 
                task2 >> mapper_num;

                std::string folder = job[3];
                
                std::cout<<"Started===> Jobid: "<<job_id<<" mapper_task_num: "<< mapper_num<<std::endl;
                std::vector<std::string> dir = getnames(folder);
                sort(dir.begin(),dir.end());
                std::string filetoopen =folder+"/"+dir[mapper_num+1];

                
                std::string str;
                struct stat info;
                std::string pathname1 = job[1]+"mapout1"; 
                if( stat( pathname1.c_str(), &info ) == 0 )
                {}
                else
                {
                    mkdir(pathname1.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
                }
                
                std::string pathname2 = job[1]+"mapout2"; 
                if( stat( pathname2.c_str(), &info ) == 0 )
                {}
                else
                {
                    mkdir(pathname2.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
                }
                std::string pathname3 = job[1]+"mapout3"; 
                if( stat( pathname3.c_str(), &info ) == 0 ){}
                else
                {
                    mkdir(pathname3.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
                }

                std::ofstream out1;
                std::string f1 = pathname1+"/"+job[2];
                out1.open(f1.c_str(), std::fstream::out |std::fstream::app);

                std::ofstream out2;
                std::string f2 = pathname2+"/"+job[2];
                out2.open(f2.c_str(), std::fstream::out |std::fstream::app);

                std::ofstream out3;
                std::string f3 = pathname3+"/"+job[2];
                out3.open(f3.c_str(), std::fstream::out |std::fstream::app);
                if(job[0]=="1")//wc
                {
                    std::ifstream in;
                    in.open(filetoopen.c_str());
                    std::string a1,a2;
                    while (getline(in,a1))
                    {
                        int hash=calhash(a1);
                        if(hash==0)
                            out1 <<a1<<std::endl;
                        else if(hash==1)
                            out2 <<a1<<std::endl;
                        else if(hash==2)
                            out3 <<a1<<std::endl;
                    }
                    in.close();
                    out1.close();
                    out2.close();
                    out3.close();
                    std::cout<<"Finished===> Jobid: "<<job_id<<" mapper_task_num: "<< mapper_num<<std::endl;
                    bzero(buffer,1024);
                    strcpy(buffer,msg.c_str());
                    int n=write(sockfd,buffer,strlen(buffer));
                }
                else //inverted index
                {
                    std::cout<<"Started===> Jobid: "<<job_id<<" mapper_task_num: "<< mapper_num<<std::endl;
                    int times = (dir.size()-2)/5;
                    int sizedir = dir.size() - 2;

                    for(int j=0;j<sizedir;j++)
                    {
                        int findex = j%5;
                        findex++;
                        if(findex==mapper_num)
                        {
                            std::ifstream infd;
                            std::string filetoopen =folder+"/"+dir[j+2];
                            infd.open(filetoopen); //open this
                            std::string a1,a2;
                            while (getline(infd,a1))
                            {
                                // cout<<a1<<endl;
                                //char i=s[6];
                                std::ostringstream fsr; 
                                fsr << (j+1); 
                                std::string towrite= a1+":<"+fsr.str()+">";
                                int hash=calhash(a1);
                                // if(hash<0)
                                //     hash=hash+3;
                                if(hash==0)
                                    out1 <<towrite<<std::endl;
                                else if(hash==1)
                                    out2 <<towrite<<std::endl;
                                else if(hash==2)
                                    out3 <<towrite<<std::endl;
                                // cout<<hash<<" "<<towrite<<endl;
                            }
                            //infd.close();

                            infd.close();
                            
                        }
                    }
                    out1.close();
                    out2.close();
                    out3.close();
                    std::cout<<"Finished===> Jobid: "<<job_id<<" mapper_task_num: "<< mapper_num<<std::endl;
                    bzero(buffer,1024);
                    strcpy(buffer,msg.c_str());
                    int n=write(sockfd,buffer,strlen(buffer));
                }
                
            }
        }
    }
   

     
    bzero(buffer,1024);
    strcpy(buffer,"done");
    write(sockfd,buffer,strlen(buffer));
    std::cout<<"mutex released by:"<<thread_name<<std::endl;
    close(sockfd);
    std::cout<<thread_name<<" thread closed "<<std::endl;
    std::cout<<"----------------------------------------------\n";
}


int main()
{
    std::string ip;
    int port = 8078;
    //std::cin>>ip;
    int i;
    std::cin>>i;
    i=i+1;
    ip="127.0.0."+std::to_string(i);
    port = port + i;
    std::string thr = "mapper";
    std::vector<std::string>filel;
    std::string f1 = "given/split_1";
    filel.push_back(f1);
    mapper_thread_inverted(ip,port,thr,1,filel,i-1);

    return 0;
}