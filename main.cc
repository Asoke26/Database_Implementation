#include <iostream>
#include <string>
#include <cstdio>
#include <cstring>
#include <cstdlib>

#include "Catalog.h"
//#include "QueryParser.h"
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "RelOp.h"
#include "DBFile.h"

using namespace std;

extern "C"{
#include "QueryParser.h"
}

// these data structures hold the result of the parsing
extern struct FuncOperator* finalFunction; // the aggregate function
extern struct TableList* tables; // the list of tables in the query
extern struct AndList* predicate; // the predicate in WHERE
extern struct NameList* groupingAtts; // grouping attributes
extern struct NameList* attsToSelect; // the attributes in SELECT
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query

extern "C" int yyparse();
extern "C" int yylex_destroy();


int main () {
	// this is the catalog

	while(1)
	{
		string dbFile = "catalog.sqlite";
		Catalog catalog(dbFile);

		// this is the query optimizer
		// it is not invoked directly but rather passed to the query compiler
		QueryOptimizer optimizer(catalog);

		// this is the query compiler
		// it includes the catalog and the query optimizer
		QueryCompiler compiler(catalog, optimizer);


		// the query parser is accessed directly through yyparse
		// this populates the extern data structures
			cout<<"Please enter your query : or exit\n"<<endl;
			int parse = -1;
			if (yyparse () == 0) {
				cout << "OK!" << endl;
				parse = 0;
			}
			else {
				cout << "Error: Query is not correct!" << endl;
				parse = -1;
			}

			yylex_destroy();

			if (parse != 0) return -1;
			int something=0;

			// at this point we have the parse tree in the ParseTree data structures
			// we are ready to invoke the query compiler with the given query
			// the result is the execution tree built from the parse tree and optimized

			if(parse==0)
			{
				QueryExecutionTree queryTree;
				compiler.Compile(tables, attsToSelect, finalFunction, predicate,
								groupingAtts, distinctAtts, queryTree);

				cout << queryTree << endl;


				queryTree.ExecuteQuery();


			}


	}

	return 0;
}
