#include<bits/stdc++.h>
#include <fstream>
#include<sstream>
using namespace std;

#include "kwaymergesort.h"

bool alphaAsc(const string &a, const string &b) { return a < b; }

int calhash(std::string str)
{
    long long hash = 1;
    for (int i = 0; i < str.length(); i++) 
    {
        hash += pow(37,i)*str[i];
        hash%=3;
    }
   
return hash;
}
vector<string> tokenizer(string line)
{
    vector <string> tokens; 
    stringstream check1(line); 
    string intermediate; 
    while(getline(check1, intermediate, ':')) 
    { 
        tokens.push_back(intermediate); 
    }
    return tokens;
}
void sort_ing(string file_to_sort,string file_to_save) {

    string inFile       = file_to_sort;
    int  bufferSize     = 100000;      // allow the sorter to use 100Kb (base 10) of memory for sorting.  
                                       // once full, it will dump to a temp file and grab another chunk.     
    bool compressOutput = false;       // not yet supported
    string tempPath     = "./";        // allows you to write the intermediate files anywhere you want.
    
    // sort the lines of a file lexicographically in ascending order (akin to UNIX sort, "sort FILE")
    ofstream opfile (file_to_save.c_str());
    KwayMergeSort<string> *sorter = new KwayMergeSort<string> (inFile, 
                                                               &opfile, 
                                                               alphaAsc, 
                                                               bufferSize, 
                                                               compressOutput, 
                                                               tempPath);
    sorter->Sort();
        opfile.close();
}

int main()
{
    system("cat out1-1.txt out1-2.txt out1-3.txt out1-4.txt out1-5.txt >filetoreduce.txt");
    sort_ing("filetoreduce.txt","sorted.txt");

    //final reduce

    
    ifstream ipfile ("sorted.txt");
    string line;
    string word;
    string fn;
    string build;
    vector<string> tokens;

    ofstream opfile ("reduced.txt");
    if(getline(ipfile, line))
    {

        tokens=tokenizer(line);
        word = tokens[0];
        fn = tokens[1];
        build = word+":"+fn;
    }
    while (getline(ipfile, line))
    {
        tokens=tokenizer(line);
        if(tokens[0]==word)
        {
            build= build+tokens[1];
        }
        else
        {
            opfile<<build<<endl;
            word = tokens[0];
            fn = tokens[1];
            build = word+":"+fn;
        }
    }
    ipfile.close();
    opfile.close();
    return 0;
}