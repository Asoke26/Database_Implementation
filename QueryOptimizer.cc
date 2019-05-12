#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <bits/stdc++.h> 

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;

//int SIZE{};

QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
}

QueryOptimizer::~QueryOptimizer() {
    map_table_info.clear();
    two_tbl_pairs.clear();
    tbl_map.clear();
    intermidiate_map.clear();
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* &_root) {
	// compute the optimal join order
	// vector<string> tablesS;
	// vector<string> tableOrder;
	// int indexing=0;
	// TableList * tables=_tables;
	// while(tables!=NULL)
    // {
	// 	string tblName=tables->tableName;
	// 	string index=to_string(indexing);
	//     tablesS.push_back(tblName);
	// 	tableOrder.push_back(index);
	// 	two_tbl_pairs[index]=tblName;
	//     tables=tables->next;
	// 	indexing++;
    // }

	// string joinOrder="";
    // for(int i{};i<tableOrder.size();i++)
    // {
    //     if(i==tableOrder.size()-1)joinOrder=joinOrder+tableOrder[i];
    //     else joinOrder=joinOrder+tableOrder[i]+",";

    // }
	// _root=new OptimizationTree;
    // _root->rightChild=NULL;
    // _root->leftChild=NULL;
	// generateTree(_root,joinOrder);
//    cout<<"IN OPTIMIZER"<<endl;
// 	printTree(_root);



	
	/* Applying Join ordering algorithm
	 * Pre-processing
	+++++++++++++++++++++++++++++++++++++++++++++++
	1. fill the map for every individual table name
	-- Map<(T)> = (size from catalog, 0, (T))
	+++++++++++++++++++++++++++++++++++++++++++++++ */
	
	TableList * tables=_tables;
	vector <string> table_Pos;
	vector <string> table_Pos_backup;
	vector <string> table_Names;
	int index_table_pos{};

	while(tables != NULL){
		string tblName{tables->tableName};
		unsigned int num_of_tuples{};
		catalog->GetNoTuples(tblName,num_of_tuples);
		Schema tblSchma;
		catalog->GetSchema(tblName,tblSchma);

		string pos=to_string(index_table_pos);

		map_table_info[pos].order=pos;
		map_table_info[pos].cost=0;
		map_table_info[pos].size=num_of_tuples;
		map_table_info[pos].tbl_Schema=tblSchma;
		map_table_info[pos].no_distinct=0;
		
		table_Pos.push_back(pos);
		table_Pos_backup.push_back(pos);
		table_Names.push_back(tblName);
		tbl_map[pos]=tblName;
		two_tbl_pairs[tblName]=pos;

		index_table_pos++;
		tables=tables->next;
		
	}





	/* +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	push-down selections: update the size for each table based on the estimation of
	number of tuples following the application of selection predicates
	-- Map<(T)> = (size after push-down selection, 0, (T))
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
	CNF cnf;
	tables=_tables;
	
	while(tables != NULL)
	{
		Schema tblSchema;
		Record tblRecord;
		string tblName{tables->tableName};
		catalog->GetSchema(tblName,tblSchema);
		cnf.ExtractCNF(*_predicate,tblSchema,tblRecord);
		int divisor{1};

		if(cnf.numAnds>0)
		{
			for(int i{};i<cnf.numAnds;i++)
			{
				if(cnf.andList[i].operand1== Left || cnf.andList[i].operand1== Right)
				{
					vector<Attribute> attributes=tblSchema.GetAtts();
					if(cnf.andList[i].op==LessThan || cnf.andList[i].op==GreaterThan) divisor=3; // As per formula of inequality estimation
					else divisor=attributes[cnf.andList[i].whichAtt1].noDistinct;		
				}

				if(cnf.andList[i].operand2 == Left || cnf.andList[i].operand2 == Right)
				{
					vector<Attribute> attributes=tblSchema.GetAtts();
					if(cnf.andList[i].op==LessThan || cnf.andList[i].op==GreaterThan) divisor=3; // As per formula of inequality estimation
					else divisor=attributes[cnf.andList[i].whichAtt1].noDistinct;		
				}
			}
			string pos=two_tbl_pairs[tblName];
			map_table_info[pos].size=map_table_info[pos].size/divisor;
		}
		tables=tables->next;
	}

	/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		fill the map for every pair of two tables (T1,T2)
		-- sort ascending T1, T2 (no need to have entries in Map for (T1,T2) and (T2,T1)
		because of commutativity)
		estimate cardinality of join using distinct values from Catalog and join predicates; 
		if you do not have a join predicate between two tables, the cost is infinity
		-- Map<(T1,T2)> = (Map<(T1)>.size*Map<(T2)>.size/selectivity, 0, (T1,T2))
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
	tables=_tables;

	for(TableList * tblOut=tables;tblOut!=NULL;tblOut=tblOut->next)
	{
		for(TableList * tblIn=tblOut->next;tblIn!=NULL;tblIn=tblIn->next)
		{
			string tbl1=tblOut->tableName;
			string tbl2=tblIn->tableName;

			Schema sch1=map_table_info[two_tbl_pairs[tbl1]].tbl_Schema;
			Schema sch2=map_table_info[two_tbl_pairs[tbl2]].tbl_Schema;

			vector<Attribute> att1=sch1.GetAtts();
			vector<Attribute> att2=sch2.GetAtts();

			ull tpl1Size,tpl2Size;
			tpl1Size= map_table_info[two_tbl_pairs[tbl1]].size;
			tpl2Size=map_table_info[two_tbl_pairs[tbl2]].size;

			string tblList="";
			tblList+=two_tbl_pairs[tbl1];
			tblList+=two_tbl_pairs[tbl2];

			vector<string> tblListVector;
			tblListVector.push_back(tbl1);
			tblListVector.push_back(tbl2);

			CNF cnf;
			cnf.ExtractCNF(*_predicate,sch1,sch2);
			int div1=-1;
			int div2=-1;
			int div=1;

			if(cnf.numAnds>0)
			{
				for(int i=0;i<cnf.numAnds;i++)
				{
					if(cnf.andList[i].operand1==Left)
					{
						div1=att1[cnf.andList[i].whichAtt1].noDistinct;
						div2=att2[cnf.andList[i].whichAtt2].noDistinct;
					}

					if(cnf.andList[i].operand1==Right)
					{
						div1=att1[cnf.andList[i].whichAtt2].noDistinct;
						div2=att2[cnf.andList[i].whichAtt1].noDistinct;
					}
					div=max(div1,div2);
				}
			}
			
			if(cnf.numAnds==0) div=1;

			Schema tmp= sch1;
			tmp.Append(sch2);

			map_table_info[tblList].size=(tpl1Size*tpl2Size)/div;
			map_table_info[tblList].cost=0;
			map_table_info[tblList].order=tblList;
			map_table_info[tblList].tbl_Schema=tmp;
			map_table_info[tblList].no_distinct=div;

            intermidiate_map[tblList].size=(tpl1Size*tpl2Size)/div;
            intermidiate_map[tblList].cost=0;
            intermidiate_map[tblList].order=tblList;
            intermidiate_map[tblList].tbl_Schema=tmp;
            intermidiate_map[tblList].no_distinct=div;

			table_Pos.push_back(tblList);


		}
	}

	string joinOrder="";
	for(int i{};i<table_Pos_backup.size();i++)joinOrder+=table_Pos_backup[i];
    int SIZE=joinOrder.size();
    if(SIZE==1)
    {
        _root=new OptimizationTree;
        _root->rightChild=NULL;
        _root->leftChild=NULL;
        _root->noTuples=map_table_info[joinOrder].size;
        _root->tables.push_back(tbl_map[joinOrder]);
        return;
    }
	//partition(_predicate,joinOrder);
	string tempOrder="";
    greedyjoin(tempOrder,SIZE);

     string finalJoinOrder="";
     for(int i{};i<tempOrder.size();i++)
     {
         if(i==tempOrder.size()-1)finalJoinOrder=finalJoinOrder+tempOrder[i];
         else finalJoinOrder=finalJoinOrder+tempOrder[i]+",";

     }
//
	cout<<"Table ID : Table"<<endl;
	for(map<string,string>::iterator it=tbl_map.begin();it!=tbl_map.end();it++)
	{
		cout<<it->first<<" : "<<it->second<<endl;
	}
	cout<<"\n\n\nJoin Order "<<endl;
	for(int i{};i<tempOrder.size();i++)
	{
		if(i==0)
		{
			for(int j{};j<tempOrder.size();j++)cout<<"(";
			cout<<tempOrder[i];
		}
		else cout<<"⋈ "<<tempOrder[i]<<")";
	}
	

	////// Join Cardinality ESTMATION
	if(tempOrder.size()==2)
	{
		string indx,indx1;
		stringstream ss;
		ss<<tempOrder[0];
		ss>>indx;
		string tbl1=tbl_map[indx];
		ss.clear();
		ss<<tempOrder[1];
		ss>>indx1;
		string tbl2=tbl_map[indx1];
		Schema sch1,sch2;
		catalog->GetSchema(tbl1,sch1);
		catalog->GetSchema(tbl2,sch2);
		ull nodistinct=joincardinality(sch1,sch2,_predicate);
		string join=tempOrder;
		map_table_info[join].size=(map_table_info[indx].size*map_table_info[indx1].size)/nodistinct;
		map_table_info[join].no_distinct=nodistinct;
		Schema temp=sch1;
		temp.Append(sch2);
		map_table_info[join].tbl_Schema=temp;
	}
	else
	{
		for(int i=2;i<tempOrder.size();i++)
		{
			string tbl1=tempOrder.substr(0,i);
			stringstream ss; ss<<tempOrder[i];
			string tbl2;ss>>tbl2;
			Schema sch1=map_table_info[tbl1].tbl_Schema;
			Schema sch2=map_table_info[tbl2].tbl_Schema;
			ull nodistinct2=joincardinality(sch1,sch2,_predicate);
			string join=tbl1+tbl2;
			ull nodistinct=max(nodistinct2,map_table_info[tbl1].no_distinct);
			map_table_info[join].size=(map_table_info[tbl1].size*map_table_info[tbl2].size)/nodistinct;
			map_table_info[join].no_distinct=nodistinct;
			map_table_info[join].order=join;
			Schema temp=sch1;
			temp.Append(sch2);
			map_table_info[join].tbl_Schema=temp;

		}
	}
	cout<<"\nESTIMATED JOIN CARDINALITY : "<<map_table_info[joinOrder].size<<endl;
	reverse(finalJoinOrder.begin(),finalJoinOrder.end());
	//cout<<"Final Order "<<finalJoinOrder<<endl;
	cout<<endl<<endl<<endl;;
	_root=new OptimizationTree;
    _root->rightChild=NULL;
    _root->leftChild=NULL;
//	for(int i{};i<joinOrder.size();i++)
//	{
//		stringstream ss;
//		string temp;
//		ss<<joinOrder[i];
//		ss>>temp;
//		_root->tables.push_back(tbl_map[temp]);
//		ss.clear();
//	}

	_root->noTuples=map_table_info[joinOrder].size;
	// cout<<"Generating Tree : "<<endl;
	generateTree(_root,finalJoinOrder,0,joinOrder);
	cout<<"Printing Tree "<<endl;
	// printTree(_root);

}

ull QueryOptimizer::joincardinality(Schema sch1,Schema sch2,AndList* _predicate)
{
	CNF cnf;
	ull result[2];
	cnf.ExtractCNF(*_predicate,sch1,sch2);
	ull max_distinct=0,s1,s2;
	if(cnf.numAnds>0)
	{
		vector<Attribute> atts1=sch1.GetAtts();
		vector<Attribute> atts2=sch2.GetAtts();
		for(int i=0;i<cnf.numAnds;i++)
		{
			if(cnf.andList[i].operand1==Left)
			{
				s1=atts1[cnf.andList[i].whichAtt1].noDistinct;
				s2=atts2[cnf.andList[i].whichAtt2].noDistinct;
			}

			if(cnf.andList[i].operand1==Right)
			{
				s1=atts1[cnf.andList[i].whichAtt2].noDistinct;
				s2=atts2[cnf.andList[i].whichAtt1].noDistinct;
			}
			max_distinct=max(s1,s2);
		}
	}
	return max_distinct;

}


void QueryOptimizer::partition(AndList * predicate, string tablelist)
{
	ull minCost=INT32_MAX;
	ull size,cost;
	string tables=tablelist,order; sort(tables.begin(),tables.end());
	Schema sch1,sch2,temp;

	bool check=true;
	map<string,tbl_info> :: iterator it;
	
	for(it=map_table_info.begin();it!=map_table_info.end();it++)
	{
		 
		if(it->first==tables){check= false;break;}
	}
	if(check==false) return;
	
	int tblSize=tables.size();
	bool if_permuted=true;

	while(if_permuted)
	{
		for(int j{};j<tblSize-2;j++)
		{
			string left{""},right{""};
			for(int i{};i<=j;i++)left+=tablelist[i];
			for(int i{j+1};i<tblSize;i++)right+=tablelist[i];
			partition(predicate,left);
			partition(predicate,right);

			sort(left.begin(),left.end());
			sort(right.begin(),right.end());

			cost=map_table_info[left].cost+map_table_info[right].cost;
			if(j!=0)cost+=map_table_info[left].size;
			if(j!=tblSize-2)cost+=map_table_info[right].size;



			if(cost<minCost)
			{
				sch1=map_table_info[left].tbl_Schema;
				sch2=map_table_info[right].tbl_Schema;
				
				vector<Attribute> att1=sch1.GetAtts();
				vector<Attribute> att2=sch2.GetAtts();
				CNF cnf;
				cnf.ExtractCNF(*predicate,sch1,sch2);
				ull div1=-1,div2=-1,div=0;

				if(cnf.numAnds>0)
				{
					for(int i=0;i<cnf.numAnds;i++)
					{
						if(cnf.andList[i].operand1==Left)
						{
							div1=att1[cnf.andList[i].whichAtt1].noDistinct;
							div2=att2[cnf.andList[i].whichAtt2].noDistinct;
						}

						if(cnf.andList[i].operand1==Right)
						{
							div1=att1[cnf.andList[i].whichAtt2].noDistinct;
							div2=att2[cnf.andList[i].whichAtt1].noDistinct;
						}
						div=max(div1,div2);
					}
				}
				
				if(cnf.numAnds==0) div=1;
				size=(map_table_info[left].size*map_table_info[right].size)/div;
				string o1=map_table_info[left].order;
				string o2=map_table_info[right].order;
				order=o1+o2;
				minCost=cost;
				temp=sch1;
				temp.Append(sch2);
				////// Test Purpose
//                 if(tablelist.size()==SIZE)
//                 {
//                     intermidiate_map[tablelist].cost=cost;
//                     intermidiate_map[tablelist].order=order;
//                     intermidiate_map[tablelist].tbl_Schema=temp;
//                     intermidiate_map[tablelist].size=size;
//
//                 }
			}
			//// Test Purpose
//            string o1=map_table_info[left].order;
//            string o2=map_table_info[right].order;
//            order=o1+o2;
//             intermidiate_map[tablelist].cost=cost;
//             intermidiate_map[tablelist].order=order;
//             intermidiate_map[tablelist].tbl_Schema=temp;
//             intermidiate_map[tablelist].size=size;
		}
		if_permuted=permutation(tablelist);
	}


    map_table_info[tables].cost=cost;
    map_table_info[tables].order=order;
    map_table_info[tables].tbl_Schema=temp;
    map_table_info[tables].size=size;


}

void QueryOptimizer::greedyjoin(string &tblOrder,int size)
{
    map<string,tbl_info> :: iterator it,it1;
	int visited[size];
	for(int i{};i<size;i++)visited[i]=-1;

	int count{};
    for(it=intermidiate_map.begin();it!=intermidiate_map.end();it++)
	{
        int min_cardinality=INT32_MAX;
        string pos="";
		for(it1=intermidiate_map.begin();it1!=intermidiate_map.end();it1++)
		{
			if(it1->second.size<min_cardinality)
			{
				min_cardinality=it1->second.size;
				pos=it1->first;
//				cout<<"Count"<<count<<endl;
			}
//			cout<<"INnner"<<endl;
		}
//		cout<<"Outer"<<endl;
		int a,b;
		string first,second;
		first=pos[0];second=pos[1];
		stringstream ss1;
		ss1<<pos[0];
		ss1>>a;
		ss1.clear();
		ss1<<pos[1];
		ss1>>b;
		ss1.clear();
		
		if(visited[a]==-1 && visited[b]==-1)
		{
			tblOrder+=pos;visited[a]=visited[b]=1;
		}
		else if(visited[a]==-1 && visited[b]!=-1)
		{

			tblOrder+=first;visited[a]=1;

		}
		else if(visited[a]!=-1 && visited[b]==-1){tblOrder+=second;visited[b]=1;}
		else {intermidiate_map.erase(pos);continue;}
		intermidiate_map.erase(pos);
		count++;
		if(count==size-1)break;
	}
}

/*
The following algorithm generates the next permutation lexicographically after a given permutation. It changes the given permutation in-place.
The method goes back to Narayana Pandita in 14th century India, and has been rediscovered frequently.

Find the largest index k such that a[k] < a[k + 1]. If no such index exists, the permutation is the last permutation.
Find the largest index l greater than k such that a[k] < a[l].
Swap the value of a[k] with that of a[l].
Reverse the sequence from a[k + 1] up to and including the final element a[n].
*/

bool QueryOptimizer::permutation(string &tblList)
{
	int k=-1,l=0;
	for(int i{};i<tblList.size();i++)
	{
		if(tblList[i]<tblList[i+1])k=i;
	}
	if(k==-1) return false;

	for(int i{k+1};i<tblList.size();i++)
	{
		if(tblList[k]<tblList[i])l=i;
	}
	swap(tblList[k],tblList[l]);
	reverse(tblList.begin()+k+1,tblList.end());
	return true;
}


void QueryOptimizer:: generateTree(OptimizationTree* &root,string joinOrder,int level,string notouch)
{
    string left,right;
    int point=joinOrder.find(',');
	if(point != string::npos)
	{
		left=joinOrder.substr(0,point);
		right=joinOrder.substr(point+1);
		root -> leftChild = new OptimizationTree;
		generateTree(root->leftChild,left,level+1,notouch);
		root -> rightChild= new OptimizationTree;
		generateTree(root->rightChild,right,level+1,notouch);
		return;
	}

	for(int i{};i<joinOrder.size();i++)
	{
		root->tables.push_back(tbl_map[joinOrder]);
		root->tuples.push_back(map_table_info[joinOrder].size);
		string search=notouch.substr(0,notouch.size()-level+1);
		root->noTuples=map_table_info[search].size;
		// cout<<"Level "<<level<<" "<<tbl_map[joinOrder]<<"" <<search<<" "<<map_table_info[search].size<<endl;

	}
	
	if(joinOrder.size()==1)
	{
		root->leftChild=NULL;
		root->rightChild=NULL;
		return;
	}
	

}

void QueryOptimizer::printTree(OptimizationTree* & root)
{
	if(root->leftChild==NULL && root->rightChild==NULL)
	{
		cout<<root->tables[0]<<" "<<root->noTuples<<" # ";
        return;
	}
	printTree(root->leftChild);
	printTree(root->rightChild);
}

OptimizationTree* QueryOptimizer::newopTree()
{
	OptimizationTree* newTree=(OptimizationTree*)malloc(sizeof(OptimizationTree));
	newTree->leftChild=NULL;
	newTree->rightChild=NULL;
	return newTree;
}