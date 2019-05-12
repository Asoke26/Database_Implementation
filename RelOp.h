#ifndef _REL_OP_H
#define _REL_OP_H

#include <iostream>
#include <fstream>
#include <unordered_set>
#include <unordered_map>

#include "Schema.h"
#include "Record.h"
#include "DBFile.h"
#include "Function.h"
#include "Comparison.h"
#include "InefficientMap.cc"
#include "EfficientMap.cc"
#include "Swapify.h"
#include "Keyify.h"

using namespace std;




class RelationalOp {
protected:
	// the number of pages that can be used by the operator in execution
//	int rs;
	int noPages;
public:
	// empty constructor & destructor
	RelationalOp() : noPages(-1) {}
	virtual ~RelationalOp() {}

	// set the number of pages the operator can use
	void SetNoPages(int _noPages) {noPages = _noPages;}

	// every operator has to implement this method
	virtual bool GetNext(Record& _record) = 0;

	/* Virtual function for polymorphic printing using operator<<.
	 * Each operator has to implement its specific version of print.
	 */
    virtual ostream& print(ostream& _os) = 0;

    /* Overload operator<< for printing.
     */
    friend ostream& operator<<(ostream& _os, RelationalOp& _op);

    virtual Schema returnSchema()=0;
};

class Scan : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;

	// physical file where data to be scanned are stored
	DBFile file;

public:
	Scan(Schema& _schema, DBFile& _file);
	virtual ~Scan();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

	virtual Schema returnSchema(){ return schema;}
};

class Select : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;

	// selection predicate in conjunctive normal form
	CNF predicate;
	// constant values for attributes in predicate
	Record constants;

	// operator generating data
	RelationalOp* producer;

public:
	Select(Schema& _schema, CNF& _predicate, Record& _constants,
		RelationalOp* _producer);
	virtual ~Select();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

    virtual Schema returnSchema(){ return schema;}
};

class Project : public RelationalOp {
private:
	// schema of records input to operator
	Schema schemaIn;
	// schema of records output by operator
	Schema schemaOut;

	// number of attributes in input records
	int numAttsInput;
	// number of attributes in output records
	int numAttsOutput;
	// index of records from input to keep in output
	// size given by numAttsOutput
	int* keepMe;

	// operator generating data
	RelationalOp* producer;

public:
	Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
		int _numAttsOutput, int* _keepMe, RelationalOp* _producer);
	virtual ~Project();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

    virtual Schema returnSchema(){ return schemaOut;}
};

class Join : public RelationalOp {
private:
	// schema of records in left operand
	Schema schemaLeft;
	// schema of records in right operand
	Schema schemaRight;
	// schema of records output by operator
	Schema schemaOut;

	// selection predicate in conjunctive normal form
	CNF predicate;

	// operators generating data
	RelationalOp* left;
	RelationalOp* right;

	TwoWayList<Record> leftRec;
    TwoWayList<Record> rightRec;
    TwoWayList<Record> outputJoin;

    // Hash Join
//    EfficientMap <KeyString , TwoWayList<Record>> HT,HT_R,HT_L;  // For Key String Based Implementation
    EfficientMap <Record , TwoWayList<Record>> HT_R,HT_L;
    TwoWayList<Record> TempReturn,runningRec;
    OrderMaker OML,OMR;

    int i;
    bool leftActive,rightActive,returnActive;
    int leftCount,rightCount;
    int buildPhase;
    bool joinTrue;

public:
	Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
		CNF& _predicate, RelationalOp* _left, RelationalOp* _right);
	virtual ~Join();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

    virtual Schema returnSchema(){ return schemaOut;}
	int level;
    int noTuples;
};

class DuplicateRemoval : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;

	// operator generating data
	RelationalOp* producer;

	OrderMaker distOM;
	EfficientMap <Record, SwapInt> HT;
    unordered_set <string> hash_tbl;
	Function func;

public:
	DuplicateRemoval(Schema& _schema, RelationalOp* _producer);
	virtual ~DuplicateRemoval();

	virtual bool GetNext(Record& _record);


	virtual ostream& print(ostream& _os);

    virtual Schema returnSchema(){ return schema;}
};

class Sum : public RelationalOp {
private:
	// schema of records input to operator
	Schema schemaIn;
	// schema of records output by operator
	Schema schemaOut;

	// function to compute
	Function compute;

	// operator generating data
	RelationalOp* producer;

    int sumcomputed;
	int running_sum;
	Type  resType;

public:
	Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
		RelationalOp* _producer);
	virtual ~Sum();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

    virtual Schema returnSchema(){ return schemaOut;}
};

class GroupBy : public RelationalOp {
private:
    struct valNode{
        double valNum;
        Record valRec;
    };
	// schema of records input to operator
	Schema schemaIn;
	// schema of records output by operator
	Schema schemaOut;

	// grouping attributes
	OrderMaker groupingAtts;
	// function to compute
	Function compute;

	// operator generating data
	RelationalOp* producer;

	unordered_map<string,valNode> groups;
    unordered_map<string,valNode> :: iterator it;
    OrderMaker grpOM;
    bool grpbySumDone;


public:
	GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
		Function& _compute,	RelationalOp* _producer);
	virtual ~GroupBy();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

    virtual Schema returnSchema(){ return schemaOut;}
};

class WriteOut : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;

	// output file where to write the result records
	string outFile;

	// operator generating data
	RelationalOp* producer;

	ofstream outfileStream;
	int rs;
    bool printSchema;

public:
	WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer);
	virtual ~WriteOut();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

	virtual Schema returnSchema(){ return schema;}
};


class QueryExecutionTree {
private:
	RelationalOp* root;

public:
	QueryExecutionTree() {}
	virtual ~QueryExecutionTree() {}

	void ExecuteQuery();
	void SetRoot(RelationalOp& _root) {root = &_root;}

    friend ostream& operator<<(ostream& _os, QueryExecutionTree& _op);
};

#endif //_REL_OP_H
