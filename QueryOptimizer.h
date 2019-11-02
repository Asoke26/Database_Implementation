#ifndef _QUERY_OPTIMIZER_H
#define _QUERY_OPTIMIZER_H

#include "Schema.h"
#include "Catalog.h"
#include "ParseTree.h"
#include "RelOp.h"
#include "EfficientMap.h"
#include "Comparison.h"

#include <string>
#include <vector>
#include <map>
#include <sstream>

using namespace std;

#define ull unsigned long long int
// data structure used by the optimizer to compute join ordering
struct OptimizationTree {
	// list of tables joined up to this node
	vector<string> tables;
	// number of tuples in each of the tables (after selection predicates)
	vector<int> tuples;
	// number of tuples at this node
	ull noTuples;

	// connections to children and parent
	OptimizationTree* parent;
	OptimizationTree* leftChild;
	OptimizationTree* rightChild;
};

class QueryOptimizer {
private:
	Catalog* catalog;

	/*Data structures
	+++++++++++++++
	1. Map : map <table_name> -> tuple (size, cost, order)
	-- table_name : a name that puts all the table names together, when there are more
	-- size : the cardinality, i.e., number of tuples, in table(s)
	-- cost : the sum of intermediate number of tuples in the tree (query optimization cost)
	-- order : join order with groupings of tables explicit*/

	struct tbl_info{
		ull size,cost,no_distinct;
		string order;
		Schema tbl_Schema;
	};

	map <string,tbl_info> map_table_info;
	map <string,string> two_tbl_pairs;
	map <string,string> tbl_map;
	map <string,tbl_info> intermidiate_map;
	

public:
	QueryOptimizer(Catalog& _catalog);
	virtual ~QueryOptimizer();

	void Optimize(TableList* _tables, AndList* _predicate, OptimizationTree* &_root);
	void generateTree(OptimizationTree* &_root,string joinOrder,int level,string donttouch);
	void printTree(OptimizationTree* &root);
	OptimizationTree * newopTree();

	void partition(AndList*,string);
	bool permutation(string &);
	void greedyjoin(string &,int);
	ull joincardinality(Schema,Schema,AndList*);
};

#endif // _QUERY_OPTIMIZER_H
