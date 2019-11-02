#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

#include	<vector>
#include	<algorithm>
#include	<unordered_map>
#include	<cstring>
#include    <string>
using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {

	// create a SCAN operator for each table in the query
    TableList * tables = _tables;
    
    while ( tables != NULL ) {
        DBFile db_file;
        Schema scan_schema;
        string tbl_name{tables->tableName};

		// Open current file
		string data_file_path="";
        catalog->GetDataFile(tbl_name,data_file_path);

        char * file_path=new char[data_file_path.size()+1];
        strcpy(file_path,data_file_path.c_str());
        db_file.Open(file_path);

        // Printing the table
        catalog->GetSchema(tbl_name,scan_schema);

		//catalog->GetSchema(tbl_name,scan_schema);
		Scan * scan=new Scan(scan_schema,db_file);
		pushDown[tbl_name]=(RelationalOp*) scan;
		

		CNF cnf;
		Record record;
		cnf.ExtractCNF(*_predicate,scan_schema,record);

		// push-down selections: create a SELECT operator wherever necessary
		if(cnf.numAnds != 0){
			Select * select= new Select(scan_schema,cnf,record,(RelationalOp*) scan);
			pushDown[tbl_name]=(RelationalOp*) select;
			//cout<<"Select "<<*select<<" : "<<select<<endl;
		}

		tables=tables->next;
	}

	

	// call the optimizer to compute the join order
	OptimizationTree* root;
	optimizer->Optimize(_tables, _predicate, root);


	// create join operators based on the optimal order computed by the optimizer
    OptimizationTree * jo_root=root;
    // printTree(root);

    RelationalOp* relop=joinTree(root,_predicate,pushDown,0);

    RelationalOp* joinOrder=(RelationalOp*) relop;




	// create the remaining operators based on the query
	Schema joinSchema=joinOrder->returnSchema();
	// SELECT FROM WHERE
    if(_groupingAtts == NULL) {

        if(_finalFunction == NULL) { // check _finalFunction first
            // a Project operator is appended at the root
            Schema schemaOut = joinSchema;
            int numAttsInput = joinSchema.GetNumAtts();
            int numAttsOutput = 0;
            vector<int> attsToKeep;
            vector<Attribute> atts = joinSchema.GetAtts();
            bool isFound;

            while(_attsToSelect != NULL) {
                string attrName = string(_attsToSelect->name);
                isFound = false;
                for(int i = 0; i < atts.size(); i++) {
                    if(atts[i].name == attrName) {
                        isFound = true;
                        attsToKeep.push_back(i);
                        numAttsOutput++;
                        break;
                    }
                }

                _attsToSelect = _attsToSelect->next;
            }

            // reverse the vector; _attsToSelect has reverse order of attributes
            reverse(attsToKeep.begin(), attsToKeep.end());

            int* keepMe = new int[attsToKeep.size()];
            copy(attsToKeep.begin(), attsToKeep.end(), keepMe);

            schemaOut.Project(attsToKeep);

            Project* project = new Project(joinSchema, schemaOut, numAttsInput, numAttsOutput,
                                           keepMe, joinOrder);


            if(_distinctAtts != 0) {
                Schema schemaIn = project->returnSchema();
                DuplicateRemoval* distinct = new DuplicateRemoval(schemaIn, project);
                joinOrder = (RelationalOp*) distinct;
            } else {
                joinOrder = (RelationalOp*) project;
            }
        } else {

            vector<string> attributes, attributeTypes;
            vector<unsigned int> distincts;
//            char *value=_finalFunction->leftOperand->value;
//            string temp;
//            temp=value;

            string attr;
            attr="SUM";
            Function compute; compute.GrowFromParseTree(_finalFunction, joinSchema);

            attributes.push_back(attr);
//            attributes.push_back(attr);
            attributeTypes.push_back("Float");
//            attributeTypes.push_back("String");
            distincts.push_back(1);
            Schema schemaOut(attributes, attributeTypes, distincts);
//            cout<<"IN SUM "<<joinSchema<<endl;
            Sum* sum = new Sum(joinSchema, schemaOut, compute, joinOrder);
            joinOrder = (RelationalOp*) sum;
        }
    }
	else // Means we are in Select From Where Groupby clause
	{
		vector<string> atts,attType;
		vector<unsigned int> distincts; 
		NameList* groupingAtts=_groupingAtts;
		Function compute;
		FuncOperator* finalFunc=_finalFunction;
		if(finalFunc!=NULL)
		{
			compute.GrowFromParseTree(finalFunc,joinSchema);

            string attr="SUM";
            atts.push_back(attr);
			attType.push_back("FLOAT");
			distincts.push_back(1);
		}
				
		int numgrpAtts{};
		vector<int> keepMe;
		while(groupingAtts != NULL)
		{
			string groupingAttName(groupingAtts->name);
			keepMe.push_back(joinSchema.Index(groupingAttName));
			atts.push_back(groupingAttName);
			Type type=joinSchema.FindType(groupingAttName);
			switch (type)
			{
				case Integer:
					attType.push_back("INTEGER");
					break;
				case Float:
					attType.push_back("FLOAT");
					break;
				case String:
					attType.push_back("STRING");
					break;
			
				default:
					attType.push_back("UNKNOWN");
					break;
			}
//			distincts.push_back(joinSchema.GetDistincts(groupingAttName));
			
//			atts.push_back(groupingAttName);
			distincts.push_back(joinSchema.GetDistincts(groupingAttName));
			numgrpAtts++;

			groupingAtts=groupingAtts->next;
		}

		Schema outputSchema(atts,attType,distincts);
		int* keepMeOrder=new int[keepMe.size()];
		copy(keepMe.begin(),keepMe.end(),keepMeOrder);
		OrderMaker grpingOrder(joinSchema,keepMeOrder,numgrpAtts);

        //cout<<"Grouping Schema 1st "<<outputSchema<<endl;

		GroupBy* groupby=new GroupBy(joinSchema,outputSchema,grpingOrder,compute,joinOrder);

		//cout<<"Grouping Schema "<<outputSchema<<endl;
		joinOrder=(RelationalOp*) groupby;
	}

	Schema finalSchema=joinOrder->returnSchema();
	string outputFile="ExecutionTree.txt";
	WriteOut * wo=new WriteOut(finalSchema,outputFile,joinOrder);
	joinOrder=(RelationalOp*) wo;
	

	_queryTree.SetRoot(*joinOrder);

	_tables=NULL;
	_attsToSelect=NULL;
	_finalFunction=NULL;
	_predicate=NULL;
	_groupingAtts=NULL;

}

RelationalOp* QueryCompiler::joinTree(OptimizationTree* &root, AndList* &_predicate,unordered_map<string,RelationalOp*> &pushDown,int level)
{
	if(root->leftChild==NULL && root->rightChild==NULL)
	{
		return pushDown.find(root->tables[0])->second;
	}
	else
	{
		RelationalOp* leftRop=joinTree(root->leftChild,_predicate,pushDown,level+1);
		RelationalOp* rightRop=joinTree(root->rightChild,_predicate,pushDown,level+1);

		Schema leftSchema,rightSchema,outSchema;
		CNF cnf;
		leftSchema=leftRop->returnSchema();
		rightSchema=rightRop->returnSchema();
		cnf.ExtractCNF(*_predicate,leftSchema,rightSchema);
		outSchema.Append(leftSchema);
		outSchema.Append(rightSchema);

		Join* join=new Join(leftSchema,rightSchema,outSchema,cnf,leftRop,rightRop);

		join->level=level;
		join->noTuples=root->noTuples;
		return (RelationalOp*) join;
	}

}

void QueryCompiler::printTree(OptimizationTree* & root)
{
    if(root->leftChild==NULL && root->rightChild==NULL)
    {
        cout<<root->tables[0]<<" "<<root->noTuples<<" # ";
        return;
    }
    printTree(root->leftChild);
    printTree(root->rightChild);
}