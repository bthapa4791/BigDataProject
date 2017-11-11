
This project uses Cloudera Virtual Box and data analysis is done using different ecosystems of Hadoop. 
The source of data is in "DataForAnalysis" text file inside the project folder.

MapReduce:
Map Reduce is a framework for processing large volume of data in distributed computing. In this Java is used as primary language to model this framework.
The source code is inside the Airline folder.

Hive:
Apache Hive is an open-source data warehouse system for querying and analyzing large datasets stored in Hadoop files.
All the queries for analyzing the data using Hive are in "Airline Project Hive Queries" file.

Pig:
Pig is a high level scripting language that is used with Apache Hadoop for analyzing data.
All the queries for analyzing the data using Pig are in "Airline Project Pig Queries" file.

In order to run this project follow these steps:
1. Install Cloudera Virtual Box in your local machine.
2. Download the project and import the Airline folder in the eclipse.
3. Load the data file "DataForAnalysis" from local to hdfs:
    Run this command in terminal: 
              hadoop fs -copyFromLocal  /localSourceFolder /hdfsDestincationfolder
4. Create the executable jar file of the project from eclipse.
5. Execute the jar file
  hadoop jar /locationFor-airline.jar org.airline.a.JobDriver /input.txt /output

The project contains folder a, b, c, d, e and Utility.
Utility is the common code used by all folders.
Folders a, b, c, d and e are the solutions of the problems.

Problem a: Find list of Airports operating in the Country India.
Run the "hadoop jar /locationFor-airline.jar org.airline.a.JobDriver /input.txt /output
" while executing jar file.

Problem b: Find the list of Airlines having zero stops
Run the "hadoop jar /locationFor-airline.jar org.airline.b.JobDriver /input.txt /output
" while executing jar file.

Problem c: List of Airlines operating with code share
Run the "hadoop jar /locationFor-airline.jar org.airline.c.JobDriver /input.txt /output
" while executing jar file.

Problem d: Which country (or) territory having highest Airports?
Run the "hadoop jar /locationFor-airline.jar org.airline.d.JobDriver /input.txt /output
" while executing jar file.

Problem e: Find the list of Active Airlines in United state
Run the "hadoop jar /locationFor-airline.jar org.airline.e.JobDriver /input.txt /output
" while executing jar file.

For using Hive and Pig Please follow the file which have all the queries.
