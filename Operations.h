//
// Created by ad26 on 5/15/19.
//

#ifndef NOT_WORKING_OPERATIONS_H
#define NOT_WORKING_OPERATIONS_H
/*
 * This Class with take input catalog,attswithType,table,file,command
 * It will support CREATE TABLE, DROP TABLE, LOAD DATA COMMAND
 * *
 * */

#include "Catalog.h"
#include "ParseTree.h"
#include "Schema.h"
#include "DBFile.h"
#include "Record.h"

using namespace std;

class Operations{
private:
    Catalog* catalog;
    AttsAndAttsType* attswithType;
    char* tblName;
    char* fileName;
    char* command;


public:
    Operations(Catalog& _catalog);
    virtual ~Operations();

    void Execute(AttsAndAttsType* _attswithType,char* _tblName,char* _fileName,char* _command,char _sc,char* aggrigate);
};




#endif //NOT_WORKING_OPERATIONS_H
