

#ifndef DATASTRUCTURE_H_
#define DATASTRUCTURE_H_

# include "Config.h"
# include "Schema.h"
# include "Swap.h"
# include <string>
# include <iostream>
using namespace std;
// Data Structure to stores the Table Info of the Catalog
// Built with compatibility with EfficientMap and InEfficientMap in mind
struct tableInfo {
private:
    string name;                                                                            // metaTable info
    string path;                                                                            // metaTable info
    int nuTuples;                                                                           // metaTable info
    bool add;                                                                               // Table was added
    bool changedAttributes;                                                                 // Attributes was changed
    bool changedTables;                                                                     // Table was changed
    Schema listOfAtts;                                                                      // This schema class is used to stores information regarding the metaAtrtibutes

public:
    tableInfo();
    tableInfo(string na, string pa, int tu);
    void Swap(tableInfo& withMe);
    void CopyFrom(tableInfo& withMe);

    // Interfacing with the object
    void setName(string na);
    void setPath(string pa);
    void setTuples(int tu);
    void setSchema(const Schema& _other);
    void setChangedT(bool changed);
    void setAdd(bool add);
    void setChangedA(bool changed);

    string& getName();
    string& getPath();
    int& getTuples();
    Schema& getSchema();
    bool& getChangedT();
    bool& getChangedA();
    bool& getAdd();

};

string convertType(Type typeI);


#endif /* DATASTRUCTURE_H_ */
