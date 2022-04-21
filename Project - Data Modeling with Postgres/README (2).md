**Introduction**

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. 

The project is to create a Postgres database with tables designed to optimize queries on song play analysis and to design the ETL pipeline for this analysis.

**Project Description:**

Using python, data with postgres and ETL pipeline was build. i have defined the fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

**Schema for songplay analysis:**

**Fact:** 
  songplays records in log_data assosciated with songplay

**Dimension tables:**

**users:**
users from the app, contains user demographics 

**artists:**
artists from music database contains artist demographics

**songs:**
songs from music database 

**time:** 
timestamps of records in songplays broken down into specific units i.e (start_time, hour, day, week, month, year, weekday)

**Project design:**

Designed the database in an optimized way with having few tables and join of required tables give us the most information for the required analysis.
ETL pipeline was designed by reading json files and move the data from those files into required tables using python.

**Database script:**

Writing python create_tables.py  command in terminal,it helps to create and recreate tables easily

**Jupyter Notebook**

etl.ipynb, a Jupyter notebook is given for verifying each command and data as well and then using those statements and copying into etl.py and running it into terminal using "python etl.py" and then testing it on test.ipynb to see whether data has been loaded in all the tables

**Relevant files provided:** 

In addition to the data files, the project workspace includes six files:

**test.ipynb:** 
displays the first few rows of each table to let you check your database.

**create_tables.py:**
drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.

**etl.ipynb:**
reads and processes a single file from song_data and log_data and loads the data into your tables in jupyter notebook. 

**etl.py:**
reads and processes files from song_data and log_data and loads them into your tables in ET. 

**sql_queries.py:**
contains all sql queries, and is imported into the last three files above.


