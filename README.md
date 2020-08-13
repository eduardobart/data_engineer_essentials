US Mortgage Public Data

The Federal Housing Finance Agency (FHFA) is an independent federal agency in the United States. FHFA is responsible for the effective supervision, regulation, and housing mission oversight of Fannie Mae, Freddie Mac (the Enterprises) and the Federal Home Loan Bank System. One of the goals of FHFA is increasing transparency in, and understanding of, the housing finance market by distributing a variety of data sets including an indicator of single-family house price trends called the House Price Index, as well as the Monthly Interest Rate Survey, the Public Use Databases, Conforming Loan Limits, and the Federal Home Loan Bank Members [1].


Challenge

Your challenge is to create a solution to help us get insights related to Fannie Mae and Freddie Mac Public Databases [2]. The challenge has three parts: ETL solution, SQL queries and visualization.


ETL Solution

Create an ETL solution to extract the databases from the source, transform the data and load it into a relational database. The entire ETL solution should be created using Python.

Data Source: 2018 Single-Family Census Tract of Fannie Mae and Freddie Mac available in [2]. 

Transformations: perform any transformation that you judge as essential for a better view and understanding of the databases. The Data Dictionary is also available in [2]. 

Relational database: load the transformed data into any free relational database (eg. MySQL, PostgreSQL, SQLite). Fannie Mae and Freddie Mac data should be loaded to the same table. 

Extra: Implement the Airflow DAG for this ETL process. This is an extra part, you should do only if you are familiar with Airflow.


SQL Queries

Perform SQL queries in the database that you created in order to answer the following questions:

1. What was the most common purpose of loans by state in 2018?

2. What are the average age and annual income of the borrowers that got loans for Home Improvement/Rehabilitation? 

3. What is the most common occupancy of the properties of the borrowers with the highest unpaid principal balance in California? These borrowers are first time home buyers?

4. Are there any records that appear in both Fannie Mae and Freddie Mac data? How would you remove duplicates?

Extra: Is there any other query that you think would bring relevant information?


Visualization

Create a report using a visualization tool (PowerBI or Tableau). Display any information about the data that you think is relevant. You can use the questions and answers above in order to choose what information to display.

 

Technologies

For this challenge you should use the technologies:

Programming language: Python

Relational Database: any free relational database (eg. MySQL, PostgreSQL, SQLite)

Language for querying the database: SQL

Visualization tool: PowerBI or Tableau


What we expect to see at the end?

Python files with the code that you implemented to read the files, transform the data and load the data into the SQL database. (If you did the extra part with Airflow, also send us the Python file with the DAG).

SQL file with the SQL code you used to create the tables and with the SQL queries that you created to answer the questions above. Add the answers as comments in this file.

File with the visualization of the data (PowerBI or Tableau).


References

[1] Federal Housing Finance Agency 

[2] Federal Housing Finance Agency Public Use Databases 

