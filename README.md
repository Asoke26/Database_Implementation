
# Database_Implementation
The project aims at design and Implementation of a relational database. Which includes Implementing below database components :  
a) Database Catalog  
b) Query Compiler.  
c) Query Optimizer.   
d) Data Loader and Executor.   
e) Operator Implemented:  
* Select  
* Project  
* Join (Nested Loop Join, Hash Join, Symmetric Hash Join)  
* Duplicate Removal  
* Sum  
* Group By  

Initial structure for the project was supplied by Instructor.  

My Contribution :
  1) Implementing methods for operators.
  2) Implementing Heap scan for storing records.
  3) Implementing Joining Algorithm blocking and non-blocking.
  4) Develop commandline interface for database.
  5) Modify Parser to support below commands:
      * CREATE TABLE table-name(att1type1,att2type2, ...)
      * LOAD DATA table-nameFROMtext-file
      * SELECT * FROM table-name
      * SELECT COUNT(*) FROM table-name

