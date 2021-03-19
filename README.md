# KAJ-Query-Engine (CS3223 Database Systems Implementation Project)
*AY2020/2021 Semester 2*, *School of Computing*, *National University of Singapore*

## Team members
- [Kong Jian Wei](https://github.com/sharptail)
- [Koh Vinleon](https://github.com/glatiuden)
- [Amos Cheong Jit Hon](https://github.com/Amoscheong97)

## Project Summary
This project shows how query processing works in a “real” system, specifically focusing on a simple SPJ (Select-Project-Join) query engine.
It also shows how different query execution trees have different performance results, which will provide some motivation for query optimization. 
More information about the project can be found [here.](https://www.comp.nus.edu.sg/~tankl/cs3223/project.html)

Our project report can be found [here](https://docs.google.com/document/d/1K5gtK9wIXcL2fHF9cyMgodyJRWJ2Nh6Xf3-oqyfJuGc/edit?usp=sharing).

## Implementation summary
In addition to the given SPJ (Select-Project-Join) query engine, our team implemented the following functions:
1.	**Block Nested Loops join**

    The implementation of the Block Nested Loops Join includes the Iterator Model with the following APIs:
    + open() - Initialize the necessary data structures to access the tuples
    + next() - Get the next tuple
    + close() -  End the process of retrieving tuples

    The difference between the Block Nested Loops Join and a normal Page Nested Loops Join is that for every API call to next(), the Page Nested Loops Join reads in a page from the outer relation to scan the inner relation while the newly implemented Block Nested Loops Join reads in a block of pages (The number of available buffers for reading the outer relation). 

    Implemented a method getBlock(int sizeofblock) to retrieve a block of tuples by providing the size of a block.

    The Block Nested Loops Join is computationally faster as it fully utilizes the memory buffers to read in more pages of the table.

    View The Code: [BlockNestedJava.java](https://github.com/Sharptail/KAJ-Query-Engine/blob/master/src/qp/operators/BlockNestedJoin.java)

2.  **SortMerge join** based on ExternalSortMerge and SortedRunComparator 
3.  **Distinct** based on ExternalSortMerge and SortedRunComparator 
4.  **Aggregate** functions (MIN, MAX, COUNT, AVG) 
5.  **OrderBy** (Supports DESC only)

    Implementation of OrderBy to support the sorting of output tuples. Users can call ORDERBY to sort in ascending order or ORDERBY DESC in descending order.

    Stores the output tuples into an ArrayList and make use of a Java Comparator to sort the ArrayList.

    View The Code: Refer to Line 244 in     [QueryMain.java](https://github.com/Sharptail/KAJ-Query-Engine/blob/master/src/QueryMain.java)


6.  Identified and fixed the following **bugs/limitations** in the SPJ engine given:
    1. Incorrect Data Type in RandomDB.java
    2. Incorrect Nested Join cost computation in PlanCost.java
    3. Allowing AVG and SUM operation on String in Attribute.java
    4. Execution goes to infinite loop if page size is smaller than tuple size for JOIN operations.

## Setup instructions
- Make sure you have [JAVA](https://www.java.com/en/) installed.
- Clone this repository into your local repository

      git clone https://github.com/Sharptail/KAJ-Query-Engine
  
- For Windows User

      .\queryenv.bat
      .\build.bat
  
- For Mac User

      source queryenv
      sh build.sh

- The details about table schema are specified in file [tablename].det. Eg. [CUSTOMER.det](https://github.com/Sharptail/KAJ-Query-Engine/blob/master/testcases/CUSTOMER.det) 
    - Use RandomDB class to generate serialized schema file [tablename].md and the data file in text format [tablename].txt. The command is
    - java RandomDB [tablename] [# of records]
    - Example
    
          java RandomDB CUSTOMER 13000

- The format of the <tablename>.det file is as shown below.

        [# of columns]  
        [size of tuple]  
        [attribute name] [data type] [range] [key type] [column size]

- Once you had the database records in text format in [tablename].txt file, then convert the records into object format. Use
    - java ConvertTxtToTbl [table name]
    - Example
    
            java ConvertTxtToTbl CUSTOMER
      
- Then write you query in some file, say query.in, and the result is required in query.out file. Then run the command to execute your query.

        java QueryMain query.in query.out
