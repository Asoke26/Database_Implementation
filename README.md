<pre>
# Database_Implementation
The project aims at design and Implementation of a relational database. Which includes Implementing below database components :  
a) Database Catalog  
b) Query Compiler.  
c) Query Optimizer.   
d) Data Loader and Executor.   
e) Operator Implemented:  
    i) Select  
    ii) Project  
    iii) Join (Nested Loop Join, Hash Join, Sym-metric Hash Join)  
    iv) Duplicate Removal  
    v) Sum  
    vi) Group By  

Initial structure for the project was supplied by Instructor.  

My Contribution :
  1) Implementing methods for operators.
  2) Implementing Heap scan for storing records.
  3) Implementing Joining Algorithm blocking and non-blocking.
  4) Develop commandline interface for database.
  5) Modify Parser to support below commands:
      i) CREATE TABLE table-name(att1type1,att2type2, ...)
      ii) LOAD DATA table-nameFROMtext-file
      iii) SELECT * FROM table-name
      iv) SELECT COUNT(*) FROM table-name
</pre>
