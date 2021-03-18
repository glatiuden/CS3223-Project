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
2.  **SortMerge join** based on ExternalSortMerge and SortedRunComparator 
3.  **Distinct** based on ExternalSortMerge and SortedRunComparator 
4.  **Aggregate** functions (MIN, MAX, COUNT, AVG) 
5.  Identified and fixed the following **bugs/limitations** in the SPJ engine given:
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
    - java ConvertTxtToTbl <table name>
    - Example
    
            java ConvertTxtToTbl CUSTOMER
      
- Then write you query in some file, say query.in, and the result is required in query.out file. Then run the command to execute your query.

        java QueryMain query.in query.out