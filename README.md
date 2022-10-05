# KAJ Query Engine
An attempt to create a "DBMS" using Java.

## Project Summary

This project shows how query processing works in a “real” system, specifically focusing on a simple SPJ (Select-Project-Join) query engine.
It also shows how different query execution trees have different performance results, which will provide some motivation for query optimization.
More information about the project can be found [here.](https://www.comp.nus.edu.sg/~tankl/cs3223/project.html)

Our project report can be found [here](https://docs.google.com/document/d/1K5gtK9wIXcL2fHF9cyMgodyJRWJ2Nh6Xf3-oqyfJuGc/edit?usp=sharing).

## Implementation summary

In addition to the given SPJ (Select-Project-Join) query engine, our team implemented the following functions:

1. **Block Nested Loops join**

   The implementation of the Block Nested Loops Join includes the Iterator Model with the following APIs:

   - open() - Initialize the necessary data structures to access the tuples
   - next() - Get the next tuple
   - close() - End the process of retrieving tuples

   The difference between the Block Nested Loops Join and a normal Page Nested Loops Join is that for every API call to next(), the Page Nested Loops Join reads in a page from the outer relation to scan the inner relation while the newly implemented Block Nested Loops Join reads in a block of pages (The number of available buffers for reading the outer relation).

   Implemented a method getBlock(int sizeofblock) to retrieve a block of tuples by providing the size of a block.

   The Block Nested Loops Join is computationally faster as it fully utilizes the memory buffers to read in more pages of the table.

   View The Code: [BlockNestedJoin.java](https://github.com/Sharptail/KAJ-Query-Engine/blob/master/src/qp/operators/BlockNestedJoin.java)

2. **SortMerge join** with ExternalSort

   The implementation of the SortMerge Join includes performing External Sort on the left and right relation.

   View The Code: [ExternalSort.java](https://github.com/Sharptail/KAJ-Query-Engine/blob/master/src/qp/operators/ExternalSort.java)

   The External Sort process includes:

   - Creating of Sorted Runs through temporarily files
   - Merging Sorted Runs through combining the temporary files

   With the two sorted left and right partition, it will be iterated in an ascending order to find and join matching tuples to produce the final output.

   View The Code: [SortMergeJoin.java](https://github.com/Sharptail/KAJ-Query-Engine/blob/master/src/qp/operators/SortMergeJoin.java)

3) **Distinct**

   Implementation of Distinct to remove duplicates tuples in the query result.

   Stores a list of printed tuples in an ArrayList and compare it with the next tuple that is going to be printed as shown in [QueryMain.java](https://github.com/Sharptail/KAJ-Query-Engine/blob/60b16ff4930970e208efd3b795f327d16bc92e52/src/QueryMain.java#L182)

   With an additional tuple comparison to compare every attribute of the tuples as shown in [Tuple.java](https://github.com/Sharptail/KAJ-Query-Engine/commit/f5461768f2bfe413d96d804971cf17e475fbe47b#diff-ad3531ca592a61a8c13fb4a70bd84c1ef571dc53c0f058fbad7b0b2191617929)

4. **Aggregate** functions (MIN, MAX, COUNT, AVG, SUM)

   Implementation of Aggregate to support performing aggregation operator on a particular column of a table. Users can call either MIN, MAX, COUNT, AVG or SUM to
   The implementation of Aggregate consist of 2 steps:

   - Appending the aggregated value to the end of the columns (View The Code: [Aggregate.java](https://github.com/Sharptail/KAJ-Query-Engine/blob/master/src/qp/operators/Aggregate.java))
   - Projecting out the desired columns (View The Code: [Project.java](https://github.com/Sharptail/KAJ-Query-Engine/blob/master/src/qp/operators/Project.java))

   Due to the absence of the `GROUPBY` operator in our project, our Query Engine supports queries of combining **Aggregated & Non-Aggregated Columns** to a certain extent. As tested on commercial DBMS (i.e., MySQL and PostgreSQL), this form of queries will typically require a `GROUPBY` operator on the non-aggregated column. However, other DBMS such as SQLite supports this form of queries without the `GROUPBY` operator to a certain extent. Hence, we have implemented as closely as the latter one to support such queries to a limited extent.

   - If **MAX** or **MIN** operator is part of the query, the returned non-aggregated columns' value is from the same tuple as the aggregated value. In the scenario where both **MAX** and **MIN** are in the same query, then the returned non-aggregated columns' value is from the **MIN** tuple.
   - If other aggregate operators (**COUNT**, **AVG**, **SUM**) is part of the query, the returned non-aggregated columns' value will typically be the first tuple of the table. If there is **JOIN** performed, then the returned non-aggregated columns' value will be from the first **JOIN** output tuple.
     In most scenario, there should be only one tuple being returned.

5) **OrderBy** (Supports DESC only)

   Implementation of OrderBy to support the sorting of output tuples. Users can call ORDERBY to sort in ascending order or ORDERBY DESC in descending order.

   Stores the output tuples into an ArrayList and make use of a Java Comparator to sort the ArrayList.

   View The Code: Refer to Line 244 in [QueryMain.java](https://github.com/Sharptail/KAJ-Query-Engine/blob/master/src/QueryMain.java)

6.  Identified and fixed the following **bugs/limitations** in the SPJ engine given:
    1. Incorrect Data Type in RandomDB.java
    2. Incorrect Nested Join cost computation in PlanCost.java
    3. Allowing AVG and SUM operation on String in Attribute.java
    4. Execution goes to infinite loop if page size is smaller than tuple size for JOIN operations.
    5. Left & right child not closed in close() method of NestedJoin.java

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
  - java RandomDB [tablename][# of records]
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
