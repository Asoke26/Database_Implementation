#ifndef DBFILE_H
#define DBFILE_H

#include <string>
#include <sys/types.h>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"

using namespace std;


class DBFile {
private:
	File file;
	string fileName;



	/*// Config.h -> file types enum FileType {Heap, Sorted, Index};*/
    FileType file_type;

    /*From File.h*/
    Page current_page;

    off_t page_index;
    bool first_position;


public:
	DBFile ();
	virtual ~DBFile ();
	DBFile(const DBFile& _copyMe);
	DBFile& operator=(const DBFile& _copyMe);

	int Create (char* fpath, FileType file_type);
	int Open (char* fpath);
	int Close ();

	void Load (Schema& _schema, char* textFile);

	void MoveFirst ();
	void AppendRecord (Record& _addMe);
	int GetNext (Record& _fetchMe);
};

#endif //DBFILE_H
