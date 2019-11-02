#include <iostream>
#include <string>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <chrono>
#include <vector>

#include "Catalog.h"
//#include "QueryParser.h"
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "RelOp.h"
#include "DBFile.h"
#include "Operations.h"

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
extern struct AttsAndAttsType* attswithType; // Attributes and Type for create table command
extern char* table; // To support create table and load
extern char* file; // Filename for load table
extern char* command; // Exit
extern char sc;
extern char* aggrigate; // Support select count *


extern "C" int yyparse();
extern "C" int yylex_destroy();


int main () {
	// this is the catalog
	int count=0;

	while(1)
	{
        char input;

        if(count==0){
            string output="_____________________________________Options__________________________________________________\n";
            output+="|    Press P for Execution Plan                                                               |\n";
            output+="|    Press Q to Execute a Query                                                               |\n";
            output+="|    Press C to Print Menu                                                                    |\n";
            output+="|    Press E to Exit                                                                          |\n";
            output+="|_____________________________________________________________________________________________|";

            cout<<output<<endl;
        }


        cout<<"sql>";
        cin>>input;

        if(input=='S' || input=='s'){

        }
        if(input=='E' || input=='e'){
            cout<<"Exiting!! \nThanks for using SQL DB"<<endl;
            break;
        }

        if(!(input=='E' || input=='e' || input=='P' || input=='p' || input== 'Q' || input=='q' || input=='C' || input=='c')){
            std::cerr<<"Invalid Option !!!"<<endl;
            continue;
        }

        if(input=='C' || input=='c'){
            string output="_____________________________________Options__________________________________________________\n";
                   output+="|    Press P for Execution Plan                                                               |\n";
                   output+="|    Press Q to Execute a Query                                                               |\n";
                   output+="|    Press C to Print Menu                                                                    |\n";
                   output+="|    Press E to Exit                                                                          |\n";
                   output+="|    Press S to check SYNTAX                                                                  |\n";
                   output+="|_____________________________________________________________________________________________|";

                   cout<<output<<endl;
            count++;
            continue;
        }

        auto tf1 = std::chrono::high_resolution_clock::now();
		string dbFile = "catalog.sqlite";
		Catalog catalog(dbFile);

		// this is the query optimizer
		// it is not invoked directly but rather passed to the query compiler
		QueryOptimizer optimizer(catalog);

		// this is the query compiler
		// it includes the catalog and the query optimizer
		QueryCompiler compiler(catalog, optimizer);

		// This is created to support CREATE, LOAD , DROP , EXIT COMMAND
		Operations operations(catalog);

		// the query parser is accessed directly through yyparse
		// this populates the extern data structures

		    cout<<"__________________________________________Enter Query_________________________________________"<<endl;
            cout<<"sql>";

			int parse = -1;
			if (yyparse () == 0) {
				cout << "Query is supported by this system!" << endl;
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

			    /***********************************CREATE LOAD DROP*******************************************/
			    if(table!=NULL || attswithType!=NULL || file!=NULL || command !=NULL || sc=='*' || aggrigate!=NULL){
                    auto tl1 = std::chrono::high_resolution_clock::now();
                    operations.Execute(attswithType,table,file,command,sc,aggrigate);
                    auto tl2 = std::chrono::high_resolution_clock::now();

                    std::cout << "Operation took "
                              << std::chrono::duration_cast<std::chrono::milliseconds>(tl2-tl1).count()
                              << " milliseconds\n";

                    continue;
			    }

			    /**********************************************************************************************/



                auto tp1 = std::chrono::high_resolution_clock::now();
				QueryExecutionTree queryTree;
				compiler.Compile(tables, attsToSelect, finalFunction, predicate,
								groupingAtts, distinctAtts, queryTree);

                auto tp2 = std::chrono::high_resolution_clock::now();
				if(input=='P' || input=='p') cout << queryTree << endl;

                auto te1 = std::chrono::high_resolution_clock::now();
				if(input== 'Q' || input=='q') {
                    cout<<"________________________________________Output________________________________________________"<<endl;
                    queryTree.ExecuteQuery();

				}
                auto te2 = std::chrono::high_resolution_clock::now();

                std::cout << "Query plan took "
                          << std::chrono::duration_cast<std::chrono::milliseconds>(tp2-tp1).count()
                          << " milliseconds\n";
                std::cout << "Query Execution took "
                          << std::chrono::duration_cast<std::chrono::milliseconds>(te2-te1).count()
                          << " milliseconds\n";

			}

        count++;
        auto tf2 = std::chrono::high_resolution_clock::now();


        std::cout << "Total time elapsed "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(tf2-tf1).count()
                  << " milliseconds\n\n";
	}

	return 0;
}
