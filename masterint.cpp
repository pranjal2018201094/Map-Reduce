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
#include <atomic>
#include <poll.h> 
static  pthread_mutex_t mapper_lock;
static  pthread_mutex_t reducer_lock;
static  pthread_mutex_t file_lock; 
static  pthread_mutex_t counter_lock;
//#include "master.hpp"
//#include "mapper.hpp"
//#include "reducer.hpp"
//#include "utilitiy.hpp"
std::vector<int> mappers;
std::vector<int>reducers;
std::vector<int>reducercalled;
std::map<int, int> Map;
std::map<int, int> Reduce;
std::vector<int> mappers1;
std::vector<int> mappers1index;
std::vector<int>reducers1;
std::vector<int> reducers1index;
std::vector<std::string>jobs;
#define MAPPER_COUNT 5
#define REDUCER_COUNT 3
int totaljobs=0;
std::vector<int>jobcount;
std::vector<int>jobcount2;
std::vector<std::string> tokenizer(std::string line,char delim);
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
void reducer_handle(int task,int job_id,int reducernumber,int socket,int redsize);
void fault(int new_socket_fd1,int new_socket_fd,int tasktype);
void init_system_inverted(std::vector<std::string>filename)
{
    // int port=8080;
    // std::string ip="";
    // std::cout<<"----------------------------------------------\n";
    // std::cout<<"init system"<<std::endl; 
    // for(int i=2;i<=6;i++)
    // {
    //     ip="127.0.0."+std::to_string(i);
    //     std::thread mapper(mapper_thread_inverted,ip,i+port,filename[i-2],i-1,filename);
    //     mapper.detach();
    // }
    //  std::cout<<"----------------------------------------------\n";
    // /*for(int i=7;i<=9;i++)
    // {
    //     std::thread reducer(reducer_thread,"127.0.0."+(i+'0'),i+port,"reducer_"+(i-6));
    //     reducer.detach();
    // }*/
}
void init_system()
{    
    // int port=8080;
    // std::string ip="";
    //  std::cout<<"----------------------------------------------\n";
    // std::cout<<"init system"<<std::endl; 
    // for(int i=2;i<=6;i++)
    // {
    //     ip="127.0.0."+std::to_string(i);
    //     std::thread mapper(mapper_thread,ip,i+port,"mapper_"+(std::to_string(i-1)),i-1);
    //     mapper.detach();
    // }
    //  std::cout<<"----------------------------------------------\n";
}
void mapper_handle(int task,int job_id,int mappernumber,int socket,std::string folder,int mapsize)
{
    char buffer[1024];
    std::ostringstream fsr1; 
    fsr1 << (task); 
    std::string taskstr  = fsr1.str();

    std::ostringstream fsr2; 
    fsr2 << (job_id); 
    std::string job_idstr  = fsr2.str();
    
    std::ostringstream fsr3; 
    fsr3 << (mappernumber);
    std::string mappernumberstr  = fsr3.str();
    int flag =0;
    //receive
    int fcount =0;
    for(int i=0;i<=4;i++)
    {
        int index = i%mapsize;
        index++;
        if(index==mappernumber)
        {
            fcount++;
        }
    }
    for(int i =0;i<fcount;i++)
    {
        bzero(buffer,1024);
        read(socket,buffer,strlen(buffer));
        jobcount[job_id]++;
    }
    std::cout<<"mapper"<<mappernumber<< "finished for Job_id==> "<<job_id<<std::endl;
    if(jobcount[job_id]==5)
    {
        std::cout<<"MAPPER TASK DONE- For Job_id==> "<<job_id<<std::endl;
        if(reducercalled[job_id]==0)
        {
            reducercalled[job_id]=1;
            std::cout<<"Calling Reducers now for Job_id ==> "<<job_id<<std::endl;
            int reducersflag[3]={0,0,0};
            int redsize = reducers.size();
            for(int i=0;i<=2;i++)
            {
                int index = i%redsize;
                int socket1 = reducers[index];
                //char buffer[1024];
                std::ostringstream fsr6; 
                fsr6 << (i+1);
                std::string reducernumberstr  = fsr6.str();
                std::string msgtosend = taskstr+"*"+job_idstr+"*"+reducernumberstr+"*"+folder+"*";

                //send the task to reducer
                sleep(1);
                bzero(buffer,1024);
                strcpy(buffer,msgtosend.c_str());
                int n=write(socket1,buffer,strlen(buffer));
                if(reducersflag[index]!=1)
                {
                    std::thread handler_reducer(reducer_handle,task,job_id,i+1,socket1,redsize);
                    handler_reducer.detach();
                }
                reducersflag[index]= 1;
            }

        }

        //make a flag before firing the reducers
    }
    // while(1)
    // {
    //     if(jobcount[job_id]==5)
    //     {
    //         flag= 1;
    //         std::cout<<"Mappers done for Job_id==> "<<job_id<<std::endl;
    //         //fire the reducers
    //         for(int j=0;j<=2;j++)
    //         {
    //             std::cout<<"Reducers fired"<<std::endl;
    //             // int index = j%reducers.size();
    //             // int socket = reducers[index];
    //             // std::thread handler_reducer(reducer_handle,task,job_id,j+1,socket);
    //             // handler_reducer.detach();
    //         }
    //     }
    //     if(flag==1){
    //         std::cout<<"break"<<std::endl;
    //         break;
    //     }
        
    //     //std::string msg(buffer);
    //     //int jobid=0;
    //     //read the message and check which job_id is complete. and save that to an integer
    //     std::string msg2(buffer);
    //     std::vector<std::string> jobs = tokenizer(msg2,'*');
    //     std::stringstream task1 (jobs[1]); 
    //     int job_1id = 0; 
    //     task1 >> job_1id;
    //     jobcount[job_1id]++;
    // }
    
}
void reducer_handle(int task,int job_id,int reducernumber,int socket,int redsize)
{
    //std::string msgtosend = taskstr+"*"+job_idstr+"*"+reducernumberstr+"*";
    char buffer[1024];
    std::ostringstream fsr1; 
    fsr1 << (task); 
    std::string taskstr  = fsr1.str();

    std::ostringstream fsr2; 
    fsr2 << (job_id); 
    std::string job_idstr  = fsr2.str();
    
    std::ostringstream fsr3; 
    fsr3 << (reducernumber);
    std::string reducernumberstr  = fsr3.str();
    int flag =0;
    //receive
    int fcount =0;
    for(int i=0;i<=2;i++)
    {
        int index = i%redsize;
        index++;
        if(index==reducernumber)
        {
            fcount++;
        }
    }
    for(int i =0;i<fcount;i++)
    {
        bzero(buffer,1024);
        read(socket,buffer,strlen(buffer));
        jobcount2[job_id]++;
    }
    std::cout<<"reducer"<<reducernumber<< "finished for Job_id==> "<<job_id<<std::endl;
    if(jobcount2[job_id]==3)
    {
        std::cout<<"REDUCER TASK DONE- For Job_id==> "<<job_id<<std::endl;
        std::cout<<"************Job FINISHED for Job_id==> "<<job_id<<"***************"<<std::endl;
    }
}
void func()
{
    int job_id = 0;
    while(1)
    {
        std::cout<<"Enter the task\n1.Word Count\n2.Inverted Indexing"<<std::endl;
        int task;
        std::cin>>task;
        totaljobs++;
        jobcount.push_back(0);
        jobcount2.push_back(0);
        reducercalled.push_back(0);
        if(task==1||task==2) //1 for word count. 2 for inverted word count
        {
            std::cout<<"Enter input folder containing files for the task \n";
            std::string filename;
            std::cin>>filename;
            //create a folder named {job_id'job_id'} and make splits for 5.
            //call file split on folder 'job_id'.

            std::ostringstream fsr; 
            fsr << (job_id); 
            std::string jobid  = fsr.str(); 
            std::string folder = filename;//"given";//"job_id"+jobid;
            sleep(1);
            //lock mapper vector list
            int mapsize = mappers.size();
            int mappersflag[5]={0,0,0,0,0};
            for(int i=0;i<=4;i++)
            {
                int index = i%mapsize;
                int socket = mappers[index];
                char buffer[1024];
                std::ostringstream fsr1; 
                fsr1 << (task); 
                std::string taskstr  = fsr1.str();

                std::ostringstream fsr2; 
                fsr2 << (job_id); 
                std::string job_idstr  = fsr2.str();
                
                std::ostringstream fsr3; 
                fsr3 << (i+1);
                std::string mappernumberstr  = fsr3.str();

                std::string msg = taskstr+"*"+job_idstr+"*"+mappernumberstr+"*"+folder+"*";

                //send the task to mapper
                sleep(1);
                bzero(buffer,1024);
                strcpy(buffer,msg.c_str());
                int n=write(socket,buffer,strlen(buffer));

                if(mappersflag[index]!=1)
                {
                    mappersflag[index]= 1;
                    std::thread handler_mapper(mapper_handle,task,job_id,i+1,socket,folder,mapsize);
                    handler_mapper.detach();
                }
                
                
            }
        }
        /* else if(task==2)
        // {
        //     //for invertedindex
        //     
        // }
        */
        job_id++;
    }

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
        std::exit(0);
    } 
    memset(&self_addr, 0, sizeof(self_addr)); 
    self_addr.sin_family=AF_INET;
    self_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    self_addr.sin_port = htons(8071); 
    if(bind(*sockfd, (struct sockaddr *)&self_addr,sizeof(self_addr))<0)
    { 
        std::cout<<"TCP socket bind failed"<<"\n";
        close(*sockfd);
        std::exit(0);
    }
    //int sockfd=0;
    //socklen_t clilen;
    struct sockaddr_in client_addr; 
    //char buffer[1024];

    std::cout<<"\n===========================================================================\n";
    std::cout<<"\n*********************** Master *******************************\n";
    std::cout<<"\n===========================================================================\n";
    /////////////////////////////////////////////////
    int mappercount = 0;
    int reducercount  = 0;
    std::cout<<"\n|||||||||||||||||||||||||||||||||||||||||||||||\n";
    std::cout<<"Master listening.............. \n"<<std::endl;

    //***************************************FAULT TOLERANCE*********************************
    int sockfd1;
    int new_socket_fd1;
    socklen_t clilen1;
    char buffer1[32]; 
    struct sockaddr_in self_addr1; 
    if((sockfd1=socket(AF_INET, SOCK_STREAM,0))<0)
    { 
        std::cout<<"socket creation failed"<<"\n"; 
        close(sockfd1);
        std::exit(0);
    } 
    memset(&self_addr1, 0, sizeof(self_addr1)); 
    self_addr1.sin_family=AF_INET;
    self_addr1.sin_addr.s_addr = inet_addr("127.0.0.1");
    self_addr1.sin_port = htons(8070); 
    if(bind(sockfd1, (struct sockaddr *)&self_addr1,sizeof(self_addr1))<0)
    { 
        std::cout<<"TCP socket bind failed"<<"\n";
        close(sockfd1);
        std::exit(0);
    }
    struct sockaddr_in client_addr1; 
    int mappercount1 = 0;
    int reducercount1  = 0;
   // while(1)
   // {
    
        
        
    //}
    
    
    while(1)
    {
        listen(sockfd1,20);
        listen(*sockfd,20);
        std::cout<<"|||||||||||||||||||||||||||||||||||||||||||||||\n";
        
        int new_socket_fd= accept(*sockfd,(struct sockaddr *) &client_addr,&clilen);
        bzero(buffer,1024);
        read(new_socket_fd,buffer,1024);
        std::string msg(buffer);
        
        //******************FAULT TOLERANCE************************
        
        new_socket_fd1= accept(sockfd1,(struct sockaddr *) &client_addr1,&clilen1);
        bzero(buffer1,32);
        read(new_socket_fd1,buffer1,32);
        std::string msg1(buffer1);
        if(msg1[0]=='m')
        {
            std::string indx = msg1.substr(1, msg1.size()-1);
            mappercount1++;
            std::cout<<"Connected to mapper checker: "<<mappercount1<<std::endl;
            std::cout<<"msg "<<msg1<<std::endl;
            std::stringstream geek(indx); 
            int x = 0; 
            geek >> x;
            Map.insert(std::pair<int, int>(new_socket_fd1,new_socket_fd));
            if(msg[0]=='m')
            {
                mappercount++;
                std::cout<<"Connected to mapper. Mapper count so far: "<<mappercount<<std::endl;
                mappers.push_back(new_socket_fd);
            
            }
            std::thread fault_handle(fault,new_socket_fd1,new_socket_fd,1);
            fault_handle.detach();
        }
        else if(msg1[0]=='r')
        {
            std::string indx = msg1.substr(1, msg1.size()-1);
            reducercount1++;
            std::cout<<"Connected to reducer checker: "<<reducercount1<<std::endl;
            std::cout<<"msg "<<msg1<<std::endl;
            std::stringstream geek(indx);
            int x = 0; 
            geek >> x;
            Reduce.insert(std::pair<int, int>(new_socket_fd1,new_socket_fd));
            if(msg[0]=='r')
            {
                reducercount++;
                std::cout<<"connected to reducer. Reducer count so far: "<<reducercount<<std::endl;
                reducers.push_back(new_socket_fd);
            }
            std::thread fault_handle(fault,new_socket_fd1,new_socket_fd,2);
            fault_handle.detach();
        }

    }
        
        
    
}
void fault(int new_socket_fd1,int new_socket_fd,int tasktype)
{
    std::cout<<"Initiated"<<std::endl;
    std::string type= "Mapper";
    if(tasktype==2)
    {
        type= "Reducer";
    }
    struct pollfd pfd;
    pfd.fd = new_socket_fd1;
    pfd.events = POLLIN | POLLHUP | POLLRDNORM;
    pfd.revents = 0;
    //std::cout<<"Initiated1"<<std::endl;
    while (pfd.revents == 0) 
    {
        //std::cout<<"Initiated2"<<std::endl;
        // call poll with a timeout of 100 ms
        if (poll(&pfd, 1, 100) > 0) 
        {
            // if result > 0, this means that there is either data available on the
            // socket, or the socket has been closed
            char buffer2[32];
            if (recv(new_socket_fd1, buffer2, sizeof(buffer2), MSG_PEEK | MSG_DONTWAIT) == 0) 
            {
                //std::cout<<"Initiated3"<<std::endl;
                bzero(buffer2,32);
                
                if(tasktype==1)
                {
                    //std::cout<<"Initiatedmap"<<std::endl;
                    for(auto i=mappers.begin();i!=mappers.end();i++)
                    {
                        if(*i==new_socket_fd)
                        {
                            //std::cout<<"Initiated"<<std::endl;
                            mappers.erase(i);
                            //std::cout<<"Initiated erased"<<std::endl;
                            break;
                        }   
                    }
                    //std::cout<<"*******CLOSED "<<std::endl;
                }
                else
                {
                    for(auto i=reducers.begin();i!=reducers.end();i++)
                    {
                        if(*i==new_socket_fd)
                        {
                            reducers.erase(i);
                            break;
                        }   
                    }
                   // std::cout<<"*******CLOSED 22222"<<std::endl;
                }
                //std::cout<<"*******CLOSED "<<type<<"with sock id "<<new_socket_fd1<<std::endl;
                close(new_socket_fd1);
                close(new_socket_fd);
                // do something else, e.g. go on vacation
            }
        }
    }
    //std::cout<<"Initiated LAST"<<std::endl;
}
void cleanmaster()
{
    DIR *dir;
    std::string dir_name="mapper";
    struct dirent *dirp;
    std::string path;
    dir=opendir(dir_name.c_str());
    if(dir==NULL)
    {
        perror("Error opening directory");
        exit(0);
    }

    while ((dirp= readdir(dir))!=NULL)
    {
        std::string file(dirp->d_name);
        path=dir_name+"/"+file;
        remove(path.c_str());
    }
    rmdir(dir_name.c_str());
    closedir(dir);   
}
void init_file_partition(int chunk_size,std::string input_filename)
{
        std::ifstream in;
        std::ofstream out;
        in.open(input_filename);
        std::string filename="mapper";
        mkdir("mapper", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        for(int i=1;i<=MAPPER_COUNT;i++)
        {
            std::string str;
            int count=0;
            out.open(filename+"/mapper_"+std::to_string(i));
            while(count!=chunk_size)
            {
                in>>str;
                out<<str<<"\n";
                count++;
            }
            out.close(); 
        }
          in.close();
        std::cout<<"-------------------------\n";
        std::cout<<"File partitoning done\n";
        std::cout<<"-------------------------\n";
         std::cout<<"----------------------------------------------\n";
}
void master_thread_inverted(std::vector<std::string>filename)
{
    int sockfd=0;
    socklen_t clilen;
    struct sockaddr_in client_addr; 
    char buffer[1024];
    // int word_count=getWordsCount(file_name);
    //  std::cout<<"----------------------------------------------\n";
    // std::cout<<"words in file:"<<word_count<<std::endl;
    // std::ifstream fin("FinalOutput.txt");
    // if(fin) {
    //   remove("FinalOutput.txt");
    //   fin.close();
    // }

    
   
    
    ////////// file partitioning//////////////

    
    // int chunk_size=word_count/MAPPER_COUNT;
    //  init_file_partition(chunk_size,file_name);
        std::cout<<"\n===========================================================================\n";
        std::cout<<"\n*********************** Master *******************************\n";
        std::cout<<"\n===========================================================================\n";
    /////////////////////////////////////////////////
    int mappercount = 0;
    int reducercount  = 0;
    while(1)
    {
        std::cout<<"\n|||||||||||||||||||||||||||||||||||||||||||||||\n";
        std::cout<<"master listening.............. \n"<<std::endl;
        std::cout<<"|||||||||||||||||||||||||||||||||||||||||||||||\n";
        listen(sockfd,10);
        int new_socket_fd= accept(sockfd,(struct sockaddr *) &client_addr,&clilen);
        bzero(buffer,1024);
        read(new_socket_fd,buffer,1024);
        std::string msg(buffer);
        if(msg[0]=='m')
        {
            mappercount++;
            //save new_socket_fd with a serial number.
            std::cout<<"Connected to mapper. Mapper count so far"<<mappercount<<" \n";
            //std::thread master_to_mapper(master_to_mapper_thread_inverted,new_socket_fd,client_addr,filename);
            //master_to_mapper.detach();
        }
        else if(msg[0]=='r')
        {
            reducercount++;
            //save new_socket_fd with a serial number.
            std::cout<<"connected to a reducer "<<reducercount<<" \n";
             //std::cout<<"connected to a reducer";
            // std::cout<<"message from:"<<msg<<std::endl;
            // std::thread master_to_reducer(master_to_reducer_thread,new_socket_fd,client_addr);
            // master_to_reducer.detach();
        }
        else if(msg[0]=='k')
        {
            cleanmaster();
            std::cout<<"*********************** Task ****************************"<<msg<<"\n";
            break;
        }
            
    }       
    std::cout<<"master exited,task completed.."<<std::endl; 
    std::exit(0);
}

void master_thread(std::string file_name)
{
    // int sockfd=0;
    // socklen_t clilen;
    // struct sockaddr_in client_addr; 
    // char buffer[1024];
    // int word_count=getWordsCount(file_name);
    //     std::cout<<"----------------------------------------------\n";
    // std::cout<<"words in file:"<<word_count<<std::endl;
    // std::ifstream fin("FinalOutput.txt");
    // if(fin) {
    //     remove("FinalOutput.txt");
    //     fin.close();
    // }

    
    // init_master_connection(&sockfd);
    
    // ////////// file partitioning//////////////

    
    // int chunk_size=word_count/MAPPER_COUNT;
    
    //     init_file_partition(chunk_size,file_name);
    //     std::cout<<"\n===========================================================================\n";
    //     std::cout<<"\n*********************** Mapper *******************************\n";
    //     std::cout<<"\n===========================================================================\n";
    // /////////////////////////////////////////////////
    // while(1)
    // {
    //     std::cout<<"\n|||||||||||||||||||||||||||||||||||||||||||||||\n";
    //     std::cout<<"master listening.............. \n"<<std::endl;
    //     std::cout<<"|||||||||||||||||||||||||||||||||||||||||||||||\n";
    //     listen(sockfd,10);
    //     int new_socket_fd= accept(sockfd,(struct sockaddr *) &client_addr,&clilen);
    //     bzero(buffer,1024);
    //     read(new_socket_fd,buffer,1024);
    //     std::string msg(buffer);
    //     if(msg[0]=='m')
    //     {
    //         std::thread master_to_mapper(master_to_mapper_thread,new_socket_fd,client_addr);
    //         master_to_mapper.detach();
    //     }
    //     else if(msg[0]=='r')
    //     {
    //         std::cout<<"message from:"<<msg<<std::endl;
    //         std::thread master_to_reducer(master_to_reducer_thread,new_socket_fd,client_addr);
    //         master_to_reducer.detach();
    //     }
    //     else if(msg[0]=='k')
    //     {
    //         cleanmaster();
    //         std::cout<<"*********************** Task ****************************"<<msg<<"\n";
    //         break;
    //     }
            
    // }       
    // std::cout<<"master exited,task completed.."<<std::endl; 
    // std::exit(0);
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



int main(int argc, char const *argv[])
{
    //job_id starts from 0
    
    int sockfd = 0;
    //init_master_connection(&sockfd);
    std::thread init(init_master_connection,&sockfd);
    std::thread whil(func);
    
    init.join();
    whil.join();
    return 0;
}