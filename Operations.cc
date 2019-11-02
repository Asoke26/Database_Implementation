//
// Created by ad26 on 5/15/19.
//

#include "Operations.h"
#include <vector>
#include <iostream>
#include <sstream>
#include <string>
#include <cstring>
#include <algorithm>

using namespace std;

Operations ::Operations(Catalog& _catalog) : catalog(&_catalog){}

Operations ::~Operations() {}

void Operations::Execute(AttsAndAttsType *_attswithType, char *_tblName, char *_fileName, char *_command,char _sc, char* aggrigate) {
    attswithType=_attswithType;
    tblName=_tblName;
    fileName=_fileName;
    command=_command;

    // Create Table
    if(attswithType!=NULL && tblName!= NULL && fileName==NULL && command==NULL && aggrigate==NULL){

        string tableName(tblName);

        vector<string> atts;
        vector<string> attsTypes;

        while (attswithType!=NULL){
            string attName(attswithType->name);
            string attType(attswithType->type);

            cout<<attName<<"  "<<attType<<endl;

            atts.push_back(attName);
            attsTypes.push_back(attType);
            attswithType=attswithType->next;
        }

        reverse(atts.begin(),atts.end());
        reverse(attsTypes.begin(),attsTypes.end());

        if(catalog->CreateTable(tableName,atts,attsTypes)){
            cout<<"Table "<<tableName<<" created successfully"<<endl;
            catalog->Save();
        }
        else cout<<"Table creation failed"<<endl;
    } else if(tblName!=NULL && fileName!=NULL && attswithType==NULL && command== NULL && aggrigate==NULL){ // LOAD DATA
        string tableName(tblName);

        // Getting schema from catalog
        Schema schema;
        catalog->GetSchema(tableName,schema);

        ///// Retrive Current working Directory
        char cwd[1000];
        getcwd(cwd, sizeof(cwd));
        string _cwd(cwd);

        string heapfilepath(_cwd+"/heap_files/"+tableName+".heap");

        char* heapFile=new char[heapfilepath.size()+1];

        strcpy(heapFile,heapfilepath.c_str());

        DBFile dbFile;
        dbFile.Create(heapFile,Heap);
        dbFile.Load(schema,fileName);

        // Changing the datafile location to heap file
        catalog->SetDataFile(tableName,heapfilepath);
        dbFile.Close();
        catalog->Save();

    }
    else if(tblName!=NULL && attswithType==NULL && command== NULL && fileName==NULL && _sc!='*' && aggrigate==NULL){ // DROP TABLE
        string tableName(tblName);

        if(catalog->DropTable(tableName)){
            cout<<tableName<<" dropped !!"<<endl;
            catalog->Save();
        }
    }
    else if(tblName!=NULL && attswithType==NULL && command== NULL && fileName==NULL && _sc=='*' && aggrigate==NULL){
        string tableName(tblName);

        Schema schema;
        catalog->GetSchema(tableName,schema);


        ///// Retrive Current working Directory
        char cwd[1000];
        getcwd(cwd, sizeof(cwd));
        string _cwd(cwd);

        string heapfilepath(_cwd+"/heap_files/"+tableName+".heap");
        char* heapFile=new char[heapfilepath.size()+1];
        strcpy(heapFile,heapfilepath.c_str());

        DBFile dbFile;
        dbFile.Open(heapFile);

        Record record;
        int rc=0;
        cout<<"______________________________________________________________________________________________"<<endl;
        while (dbFile.GetNext(record))
        {
            record.print(cout,schema);
            cout<<endl;
            rc++;
        }
        cout<<"______________________________________________________________________________________________"<<endl;
        cout<<"Number of tuples "<<rc<<endl;
        dbFile.Close();

    }
    else if(tblName!=NULL && attswithType==NULL && command== NULL && fileName==NULL && _sc=='*' && aggrigate!=NULL){
        string tableName(tblName);

        Schema schema;
        catalog->GetSchema(tableName,schema);


        ///// Retrive Current working Directory
        char cwd[1000];
        getcwd(cwd, sizeof(cwd));
        string _cwd(cwd);

        string heapfilepath(_cwd+"/heap_files/"+tableName+".heap");
        char* heapFile=new char[heapfilepath.size()+1];
        strcpy(heapFile,heapfilepath.c_str());

        DBFile dbFile;
        dbFile.Open(heapFile);

        Record record;
        int rc=0;
//        cout<<"______________________________________________________________________________________________"<<endl;
        while (dbFile.GetNext(record))
        {
//            record.print(cout,schema);
//            cout<<endl;
            rc++;
        }
//        cout<<"______________________________________________________________________________________________"<<endl;

        cout<<"| Number of tuples "<<rc<<" |"<<endl;

        dbFile.Close();
    }
}