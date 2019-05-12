#include <iostream>
#include "RelOp.h"
#include <iomanip>
#include <bits/stdc++.h>
#include <sstream>

using namespace std;

//
ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {
	schema=_schema;
	file=_file;
}

bool Scan::GetNext(Record &_record) {
    int ret=file.GetNext(_record);
    if(ret==true)
        return true;
    else return false;
}

Scan::~Scan() {

}

ostream& Scan::print(ostream& _os) {
	return _os << "  SCANING[" << schema << "]";
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {
	schema=_schema;
	predicate=_predicate;
	constants=_constants;
	producer=_producer;

}

bool Select::GetNext(Record &_record) {
    while(true)
    {
        bool ret=producer->GetNext(_record);
        if(ret== false) return false;
        else
        {
            ret=predicate.Run(_record,constants);
            if(ret==true){
                return true;
            }
        }
    }

}

Select::~Select() {

}

ostream& Select::print(ostream& _os) {
	_os << "σ [";
	for(int i{};i<predicate.numAnds;i++)
	{
		Comparison whr_cond = predicate.andList[i];
		vector<Attribute> selectAtts=schema.GetAtts();
		//_os<<"before";
		// Left side of a condition

		if(whr_cond.operand1==Left){
			_os << selectAtts[whr_cond.whichAtt1].name<<" ";
		}
		
		// Operator of a condition
		if(whr_cond.op==LessThan)
			_os << " < ";
		else if(whr_cond.op==Equals)
			_os <<" = ";
		else _os <<" > ";

		// Right Side of COndition

		if(whr_cond.operand2==Right){
			_os<< selectAtts[whr_cond.whichAtt2].name<<" ";
		}
		else if(whr_cond.operand2==Literal){
			if(whr_cond.attType==Integer)
				_os << *(int *)(constants.GetColumn(whr_cond.whichAtt2));
            else if(whr_cond.attType==Float)
            {
                double * literalVal=(double *)(constants.GetColumn(whr_cond.whichAtt2));
                _os<<*literalVal;
            }
            else if(whr_cond.attType==String)
            {
                char * val = (char *)(constants.GetColumn(whr_cond.whichAtt2));
                _os << (char *)(constants.GetColumn(whr_cond.whichAtt2));
            }


		}

		if(i==predicate.numAnds-1) _os << "] ";
		else _os << " & ";
		//_os.flush();
		
	}
	// cout<<"σ END"<<endl;
	_os<<"Select schema : ["<<schema<<"] \n";
	_os <<*producer;
	return _os;
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {
		schemaIn=_schemaIn;
		schemaOut=_schemaOut;
		numAttsInput=_numAttsInput;
		numAttsOutput=_numAttsOutput;
		keepMe=_keepMe;
		producer=_producer;
}

Project::~Project() {

}

bool Project::GetNext(Record &_record) {
    bool ret=producer->GetNext(_record);
    if(true==ret)
    {
        _record.Project(keepMe,numAttsOutput,numAttsInput);
    }
    else return false;
}

ostream& Project::print(ostream& _os)
{
	_os << "	Π ⤋ \n\t[";
	vector<Attribute> projectAtts=schemaOut.GetAtts();
	for (int i{};i<numAttsOutput;i++){
		_os<<projectAtts[i].name;
		if(i==numAttsOutput-1)_os<<"";
		else _os<<",";
	}
	cout<<"]\n\t\t"<<endl;
    _os<<"Project schema : ["<<schemaOut<<"] ";
	_os <<*producer;
	return _os;
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) : schemaRight(_schemaRight),schemaLeft(_schemaLeft),schemaOut(_schemaOut),
	predicate(_predicate),left(_left),right(_right),buildPhase(0),i(0),joinTrue(false),leftActive(true),leftCount(0),rightActive(
                false),rightCount(0),returnActive(false){

//    OML=OrderMaker(schemaLeft);

    //// Find The join attribute and set orderMaker for Left Child
        Comparison c = predicate.andList[0];
        vector <Attribute> attListLeft = schemaLeft.GetAtts();

        int pos; int *keepMe = new int[1];
        if (c.operand1 == Left) {
            pos = c.whichAtt1;
            keepMe[0]=pos;
            OML=OrderMaker(schemaLeft,keepMe,1);
        } else if (c.operand1 == Right) {
            pos = c.whichAtt1;
            keepMe[0]=pos;
            OMR=OrderMaker(schemaRight,keepMe,1);
        }

        int pos1; int * keepMe1=new int[1];
        if (c.operand2 == Left) {
            pos1 = c.whichAtt2;
            keepMe1[0]=pos1;
            OML=OrderMaker(schemaLeft,keepMe1,1);

        } else if (c.operand2 == Right) {
            pos1 = c.whichAtt2;
            keepMe1[0]=pos1;
            OMR=OrderMaker(schemaRight,keepMe1,1);
        }

}

Join::~Join() {

}




ostream& Join::print(ostream& _os)
{
		_os << "  "<<"JOIN ⋈ " ;
	for (int i = 0; i < predicate.numAnds; i++) {
			if (i == 0) _os << "["; else _os << " [";


			Comparison c = predicate.andList[i];
			vector<Attribute> attListLeft = schemaLeft.GetAtts();
			vector<Attribute> attListRight = schemaRight.GetAtts();
			if (c.operand1 == Left){
				int pos = c.whichAtt1;
				_os << attListLeft[pos].name;
			}
			else if (c.operand1 == Right) {
				int pos = c.whichAtt1;
				_os << attListRight[pos].name;;
			}

			if (c.op == LessThan) _os << " < ";
			else if (c.op == GreaterThan) _os << " > ";
			else _os << " = ";

			if (c.operand2 == Left){
				int pos = c.whichAtt2;
				_os << attListLeft[pos].name;
			}
			else if (c.operand2 == Right) {
				int pos = c.whichAtt2;
				_os << attListRight[pos].name;;
			}

			if (i < predicate.numAnds-1) _os << "] &"; else _os << "]";
		}

		_os << "]";
		_os << "\n";
		for(int i = 0; i < level+1; i++)
			_os << "\t";
		_os << " ├++++ " << *right;

		_os << "\n";
		for(int i = 0; i < level+1; i++)
			_os << "\t";
		_os << " ├++++ " << *left;


	return _os;
}

/************************Symmetric Hash Join **********************************/


bool Join::GetNext(Record &_record)
{
    if(returnActive){
        Record temp;
        temp = TempReturn.Current();
        _record=temp;
        TempReturn.Remove(temp);
        if(TempReturn.Length()==0 && leftActive== true) {

            if(rightCount>0 && rightCount<10){
                returnActive= false;
                leftCount=0;
            }
            else{
                returnActive= false;
                leftActive= false;
                rightActive= true;
                rightCount=0;
            }


        }
        else if(TempReturn.Length()==0 && rightActive== true) {

            if(leftCount>0 && leftCount <10){
                returnActive= false;
                rightCount=0;
            }
            else{
                returnActive= false;
                leftActive= true;
                rightActive= false;
                leftCount=0;
            }


        }

        return true;
    }

    if(leftActive){
        Record tempRec;
        while (left->GetNext(tempRec)){
            tempRec.SetOrderMaker(&OML);

            if (HT_L.IsThere(tempRec)) {

                Record key;
                key=tempRec;
                HT_L.Find(key).Append(tempRec);

                /// Temporary store for storing running Records

                runningRec.Append(key);


            } else {

                Record key;
                key=tempRec;
                TwoWayList<Record> temp;
                temp.Append(tempRec);
                HT_L.Insert(key, temp);
                runningRec.Append(key);   /// Temporary store for storing running Records
            }
            leftCount++;
            if(leftCount==10) break;

        }

        if(HT_R.Length()>0){

            runningRec.MoveToStart();

            while(runningRec.RightLength()!=0){

                Record key;
                key=runningRec.Current();
                runningRec.Remove(runningRec.Current());
                TwoWayList<Record> TempStore;

                if (HT_R.IsThere(key)) {

                    TempStore.CopyFrom(HT_R.Find(key));
                    TempStore.MoveToStart();
                    int *keepMe = new int[schemaOut.GetNumAtts()];
                    for (int j = 0; j < schemaLeft.GetNumAtts(); j++)keepMe[j] = j;
                    for (int i = 0; i < schemaRight.GetNumAtts(); i++)keepMe[schemaLeft.GetNumAtts() + i] = i;

                    while ( TempStore.RightLength() != 0 ) {
                        Record joinRec;
                        joinRec.MergeRecords(TempStore.Current(), key, schemaLeft.GetNumAtts(),
                                             schemaRight.GetNumAtts(), keepMe, schemaOut.GetNumAtts(),
                                             schemaLeft.GetNumAtts());
                        TempReturn.Append(joinRec);

                        TempStore.Advance();

                    }
                    returnActive= true;
                }

                runningRec.Advance();
            }
            TempReturn.MoveToStart();
        }
    }

    if(rightActive){
        Record tempRec;
        while (right->GetNext(tempRec)){
            tempRec.SetOrderMaker(&OMR);

            if (HT_R.IsThere(tempRec)) {

                Record key;
                key=tempRec;
                HT_R.Find(key).Append(tempRec);

                /// Temporary store for storing running Records

                runningRec.Append(key);

            } else {

                Record key;
                key=tempRec;
                TwoWayList<Record> temp;
                temp.Append(tempRec);
                HT_R.Insert(key, temp);

                runningRec.Append(key); /// Temporary store for storing running Records

            }
            rightCount++;
            if(rightCount==10) break;

        }

        if(HT_L.Length()>0){

            runningRec.MoveToStart();


            while(runningRec.RightLength()!=0){

                Record key;
                key=runningRec.Current();
                runningRec.Remove(runningRec.Current());
                TwoWayList<Record> TempStore;

                if (HT_L.IsThere(key)) {

                    TempStore.CopyFrom(HT_L.Find(key));
                    TempStore.MoveToStart();
                    int *keepMe = new int[schemaOut.GetNumAtts()];
                    for (int j = 0; j < schemaLeft.GetNumAtts(); j++)keepMe[j] = j;
                    for (int i = 0; i < schemaRight.GetNumAtts(); i++)keepMe[schemaLeft.GetNumAtts() + i] = i;

                    while ( TempStore.RightLength() != 0 ) {
                        Record joinRec;
                        joinRec.MergeRecords(TempStore.Current(), key, schemaLeft.GetNumAtts(),
                                             schemaRight.GetNumAtts(), keepMe, schemaOut.GetNumAtts(),
                                             schemaLeft.GetNumAtts());
                        TempReturn.Append(joinRec);

                        TempStore.Advance();

                    }
                    returnActive= true;
                }

                runningRec.Advance();
            }

            TempReturn.MoveToStart();
        }


    }
}

/************************Hash Join **********************************/

/*

bool Join::GetNext(Record &_record) {


    if (buildPhase == 0) {
        Record tempRec;
        while ( left->GetNext(tempRec)) {

            tempRec.SetOrderMaker(&OML);

            if (HT_L.IsThere(tempRec)) {

                Record key;
                key=tempRec;
                HT_L.Find(key).Append(tempRec);

            } else {

                Record key;
                key=tempRec;
                TwoWayList<Record> temp;
                temp.Append(tempRec);
                HT_L.Insert(key, temp);
            }
            buildPhase = 1;

        }


    }


    if(buildPhase==1){
        Record tempRec;
        while ( right->GetNext(tempRec) && TempReturn.RightLength() == 0) {
            tempRec.SetOrderMaker(&OMR);
            TwoWayList<Record> TempStore;


            if (HT_L.IsThere(tempRec)) {

                TempStore.CopyFrom(HT_L.Find(tempRec));
                TempStore.MoveToStart();
                int *keepMe = new int[schemaOut.GetNumAtts()];
                for (int j = 0; j < schemaLeft.GetNumAtts(); j++)keepMe[j] = j;
                for (int i = 0; i < schemaRight.GetNumAtts(); i++)keepMe[schemaLeft.GetNumAtts() + i] = i;

                while ( TempStore.RightLength() != 0 ) {
                    Record joinRec;
                    joinRec.MergeRecords(TempStore.Current(), tempRec, schemaLeft.GetNumAtts(),
                                         schemaRight.GetNumAtts(), keepMe, schemaOut.GetNumAtts(),
                                         schemaLeft.GetNumAtts());
                    TempReturn.Append(joinRec);

                    TempStore.Advance();

                }
                TempReturn.MoveToStart();
                buildPhase = 2;
            }
            if(buildPhase==2)break;

        }
    }


    if(buildPhase==2 && TempReturn.Length() > 0){
        Record temp;
        temp = TempReturn.Current();
        _record=temp;
        TempReturn.Remove(temp);
        if(TempReturn.Length()==0) buildPhase=1;
        return true;
    }

    return false;

}


*/


/************************Nexted Loop Join **********************************/
/*

bool Join::GetNext(Record &_record){

    if(buildPhase==0){

        /// Inserting Record from RightSide and LeftSide into List

        Record temp;
        while(right->GetNext(temp)){
            rightRec.Insert(temp);
        }

        while(left->GetNext(temp)){
            leftRec.Insert(temp);
        }


        bool rightSmaller;
        if(rightRec.Length()<leftRec.Length()) {
            rightSmaller= true;
        }
        else{
            rightSmaller= false;
        }

        if(rightSmaller){

            rightRec.MoveToStart();
            leftRec.MoveToStart();

            int * attstoKeep=new int[schemaOut.GetNumAtts()];

            for (int i = 0; i <rightRec.Length() ; ++i) {
                for (int j=0;j<leftRec.Length();++j){

                     bool match= predicate.Run(leftRec.Current(),rightRec.Current());

                     if(match){

                         for(int j=0;j<schemaLeft.GetNumAtts();j++)attstoKeep[j]=j;
                         for(int i=0;i<schemaRight.GetNumAtts();i++)attstoKeep[schemaLeft.GetNumAtts()+i]=i;

                         Record newRec;
                         newRec.MergeRecords(leftRec.Current(),rightRec.Current(),schemaLeft.GetNumAtts(),schemaRight.GetNumAtts(),attstoKeep,schemaOut.GetNumAtts(),schemaLeft.GetNumAtts());

                         outputJoin.Append(newRec);
                     }
                     leftRec.Advance();
                }
                rightRec.Advance();
                leftRec.MoveToStart();
            }
        }
        else{
            rightRec.MoveToStart();
            leftRec.MoveToStart();

            int * attstoKeep=new int[schemaOut.GetNumAtts()];

            for (int i = 0; i <leftRec.Length() ; ++i) {
                for (int j = 0; j < rightRec.Length(); ++j) {

                    bool match=predicate.Run(leftRec.Current(),rightRec.Current());

                    if(match){

                        for(int j=0;j<schemaLeft.GetNumAtts();j++)attstoKeep[j]=j;
                        for(int i=0;i<schemaRight.GetNumAtts();i++)attstoKeep[schemaLeft.GetNumAtts()+i]=i;

                        Record newRec;
                        newRec.MergeRecords(leftRec.Current(),rightRec.Current(),schemaLeft.GetNumAtts(),schemaRight.GetNumAtts(),attstoKeep,schemaOut.GetNumAtts(),schemaLeft.GetNumAtts());

                        outputJoin.Append(newRec);
                    }
                    rightRec.Advance();
                }
                leftRec.Advance();
                rightRec.MoveToStart();
            }
        }

        outputJoin.MoveToStart();
        buildPhase = 1;


    }

    /// Returning the output records

    while(outputJoin.RightLength()!=0) {
        _record=outputJoin.Current();
        outputJoin.Advance();
        return true;

    }

    if(outputJoin.RightLength()==0){
        buildPhase= 0;
        return false;
    }


}
*/

DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) : schema(_schema), producer(_producer), distOM(_schema) {


}

DuplicateRemoval::~DuplicateRemoval() {

}

bool DuplicateRemoval::GetNext(Record &_record) {


    while (producer->GetNext(_record)){
        _record.SetOrderMaker(&distOM);

        if(HT.IsThere(_record)==0){
            int intResult{};
            double doubleResult{};
            Record rec_ins;
            rec_ins=_record;
            SwapInt finalResult=intResult+doubleResult;
            HT.Insert(rec_ins,finalResult);
            return true;
        }


    }

}

ostream& DuplicateRemoval::print(ostream& _os) {
	_os << "DISTINCT⤋\t" <<"DISTINCT schema : ["<<schema<<"] \n"<< *producer;
	return _os;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute, RelationalOp* _producer) : sumcomputed(false),running_sum(0){
		schemaIn=_schemaIn;
		schemaOut=_schemaOut;
		compute=_compute;
		producer=_producer;
}

Sum::~Sum() {

}
bool Sum::GetNext(Record &_record) {

    char* recBits = new char[1];
    double result = 0; Type resType;

    if(sumcomputed==0){
        Record tmp;
        while(producer->GetNext(tmp)) {
            int resInt = 0; double resDbl = 0;
            resType = compute.Apply(tmp, resInt, resDbl);
            result += resInt + resDbl;

            sumcomputed = 1;
        }
    }

    if(sumcomputed==1) {
        int recSize;

        *((double *) recBits) = result;
        recSize = sizeof(double);

        char* recComplete = new char[sizeof(int) + sizeof(int) + recSize];
        ((int*) recComplete)[0] = 2*sizeof(int) + recSize;
        ((int*) recComplete)[1] = 2*sizeof(int);
        memcpy(recComplete+2*sizeof(int), recBits, recSize);

        _record.Consume(recComplete);
        sumcomputed=3;
        return true;
    }
    else {
        return false;
    }


}

ostream& Sum::print(ostream& _os) {
	return _os << "SUM ⤋ "<<"SUM schema : ["<<schemaOut<<"] \n"<<*producer;
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) : grpOM(schemaOut), grpbySumDone(false){
		schemaIn=_schemaIn;
		schemaOut=_schemaOut;
		groupingAtts=_groupingAtts;
		compute=_compute;
		producer=_producer;
}

GroupBy::~GroupBy() {

}

bool GroupBy::GetNext(Record &_record) {

    if(grpbySumDone == false){
        Record tempRec;
        while(producer->GetNext(tempRec))
        {
            Schema tempS;
            tempS=schemaOut;
            vector<int> attsKeep;
            vector<Attribute> atts=schemaOut.GetAtts();
            for(int i=0;i<schemaOut.GetNumAtts();i++){
                if(atts[i].name!="SUM"){
                    attsKeep.push_back(i);
                }
            }
            
            tempS.Project(attsKeep);
            

            double finalResult{};double doubleResult{}; int intResult{};
            compute.Apply(tempRec,intResult,doubleResult);
            finalResult=intResult+doubleResult;

            

            tempRec.Project(&groupingAtts.whichAtts[0],tempS.GetNumAtts(),schemaOut.GetNumAtts());

            string mapKey=tempRec.findKey(tempS);

            if(groups.find(mapKey)==groups.end()){
                
                valNode value;
                value.valNum=finalResult;
                value.valRec=tempRec;
                groups[mapKey]=value;
            }
            else{
                groups[mapKey].valNum+=finalResult;
            }

            it=groups.begin();
            grpbySumDone=true;
        }
    }

    
    if(grpbySumDone){
        if(it!=groups.end())
        {

            Record temp,newRec;;
            char* recBit = new char[1];
            int recSize;
            *((double *) recBit) =it->second.valNum ;
            recSize = sizeof(double);


            char* recRet = new char[sizeof(int) + sizeof(int) + recSize];
            ((int*) recRet)[0] = 2*sizeof(int) + recSize;
            ((int*) recRet)[1] = 2*sizeof(int);
            memcpy(recRet+2*sizeof(int), recBit, recSize);

            temp.Consume(recRet);
            newRec.AppendRecords(temp,(*it).second.valRec,1,1);

            /***********************Finding grouping attribute schema***************************************/

            Schema tempS;
            tempS=schemaOut;
            vector<int> attsKeep;
            vector<Attribute> atts=schemaOut.GetAtts();
            for(int i=0;i<schemaOut.GetNumAtts();i++){
                if(atts[i].name!="SUM"){
                    attsKeep.push_back(i);
                }
            }

            tempS.Project(attsKeep);


            _record=newRec;
            newRec.Nullify();
            it++;
            return true;
        }
    }
       
    return false;
        



}

ostream& GroupBy::print(ostream& _os) {
	return _os << "GROUP BY "<<"GROUPING schema : ["<<schemaOut<<"] \n"<<*producer;
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) : rs(0),printSchema(false) {
	schema=_schema;
	outFile=_outFile;
	producer=_producer;
}

WriteOut::~WriteOut() {
//	if(outfileStream.is_open())
//	{
//		outfileStream.close();
//	}
}

bool WriteOut::GetNext(Record &_record) {
//    int count{};
    if(producer->GetNext(_record))
    {
        // if(!printSchema){
        //     vector<Attribute> atts=schema.GetAtts();
        //     for (int j = 0; j <schema.GetNumAtts() ; ++j) {
        //         cout<<"_________________________";
        //     }
        //     cout<<endl;
        //     for (int i = 0; i < schema.GetNumAtts(); ++i) {
        //         string name=
        //         cout<<"|     "<<atts[i].name;
        //     }
        // }
       cout<<fixed;
       _record.print(cout,schema);
       cout<<endl;
       rs++;
    } else{
        cout<<"Number of row survived : "<<rs<<endl;
        return false;
    }
}

ostream& WriteOut::print(ostream& _os) {
	return _os<<endl<<endl<<*producer<<endl;
}

void QueryExecutionTree::ExecuteQuery()
{
    Record record;
    while(root->GetNext(record)){}
}

ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	if(_op.root!=NULL)
        return  _os << "QUERY EXECUTION TREE" << *_op.root;
}
