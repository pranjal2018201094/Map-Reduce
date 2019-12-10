#include<bits/stdc++.h>
#include <fstream>
#include<sstream>
using namespace std;

int main()
{
    //filenames contains the name of all the files to be considered for the operation
    string filenames[]={"split_1.txt","split_2.txt","split_3.txt","split_4.txt","split_5.txt","split_6.txt"};
    
    ofstream opfile ("inverted_mapper.txt");
    if (opfile.is_open())
    {
        for(int i=0;i<6;i++)
        {
        //cout<<filenames[i];
            ifstream fd (filenames[i].c_str());
            string a1,a2;
            while (getline(fd,a1))
            {
                cout<<a1<<endl;
                ostringstream fsr; 
                fsr << i; 
                string towrite= a1+" <"+fsr.str()+">";
                opfile <<towrite<<endl;
            }
            fd.close();
        }
    
    opfile.close();
    }
    


    return 0;
}