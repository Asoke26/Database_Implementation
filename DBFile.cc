#include <string>
#include <sys/stat.h>
#include <cstdlib>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

#include <sys/stat.h>

using namespace std;


DBFile::DBFile () : fileName("") {
    page_index=0;
}

DBFile::~DBFile () {

}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {
    fileName=f_path;
    file_type=f_type;

    /*
    // open file
	// if length is 0, create new file; existent file is erased
	// return 0 on success, -1 otherwise
     int file :: Open(int length, char* fName);
    */
    // char * f_name=new char[fileName.size()+1];
    // strcopy(f_name,fileName);
    file.Open(0,f_path);
}

int DBFile::Open (char* f_path) {
    fileName=f_path;

    /*stat() from sys/stat.h POSIX header in order to get the size of a file.*/
    struct stat st;
    stat(fileName.c_str(),&st);
    int length=st.st_size;
    file.Open(length,f_path);
}

void DBFile::Load (Schema& schema, char* textFile) {

    MoveFirst();
    FILE* textData = fopen(textFile, "r");

    while(true) {
        Record record;
        if(record.ExtractNextRecord(schema, *textData)) { // success on extract
            AppendRecord(record);
        } else { // no data left or error
            break;
        }
    }
    file.AddPage(current_page, page_index++); // add the last page to the file
    fclose(textData);
}

int DBFile::Close () {
    if(file.Close())return true;
    else return false;
}

void DBFile::MoveFirst () {
    current_page.EmptyItOut();
    page_index=0;
    first_position= true;

}

void DBFile::AppendRecord (Record& rec) {
    if(!current_page.Append(rec)) {
        file.AddPage(current_page, page_index++);
        current_page.EmptyItOut();
        current_page.Append(rec);
    }
}

int DBFile::GetNext (Record& rec) {
//    MoveFirst();

    int ret=current_page.GetFirst(rec);

    if(true==ret)
        return true;
    else
    {
        if(page_index==file.GetLength()) return false;
        else
        {

            file.GetPage(current_page,page_index);
            ret=current_page.GetFirst(rec);
            page_index++;
            return true;
        }
    }

//    off_t length_of_file=file.GetLength(); // Length of the file in pages
//
//    if(current_page.GetFirst(rec)==0)
//    {
//        if(page_index==length_of_file)break;
//        else file.GetPage(current_page,page_index++);
//    }
//    else
//    {
//        return 0;
//    }
//    return 1;
}
