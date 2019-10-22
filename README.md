# Problem
 Sparkify has so much data collected on songs and user activity on their music app and wants to perform analysis on their users and the songs they are playing. Currently, their data is stored in json format in two set of files, `song_data` and `log_data`. They current architecture does not allow them to do the analysis effectively.
 
# Solution
 Sparkify needs a database with the star schema and ETL pipeline. Since the analysis need to be optimized to query song play data, star schema should have songplay table as a fact table and other key identities (users, songs, artists, time) as dimension tables.  
 
Visual demonstrion of the final tables can be found below:
https://docs.google.com/spreadsheets/d/1BnDLl0OhkRmtb1qhyT11UzJ7Xp1LAB25DeqMVrgBQcY/edit?usp=sharing 
 
In order to populate the data effectively, we will use redshift cluster and use redshift tables as staging tables. we will load raw events and songs data into `stagingevents` and `stagingsongs`, respectively. Depending on the the cluster settings this can take minutes up to hours. For example, copying data into a redshift cluster with specification dc2.large(2nodes) takes roughly 1.5 hour, while loading data into a cluster with specification dc2.Large (4 nodes) take roughly about 0.5 hour. However, we have to be careful with the extra cost associated with using more nodes. 

When data is locally avaialble on redshift cluster, we will populate fact and dimension tables using data in staging tables. To remove duplicate instances, distinct keyboard is used.


# Run

## create_tables.py
Use this script to create or drop tables. To run:

`python create_tables.py`. 

## etl.py

Use this script to populate the tables, to run:

`python etl.py`. 

## dwh.cfg
add your redshift cluster specifications here. including database name, endpoint, and iam roles. also specify specify location of data on s3 bucket

## sql_queries.py
collect all create, insert, drop queries here. this script is internally used by etl.py and create_tables.py


# Sample Queries
Example of querying the songplay data for users in washington:
 `select * from songplays where location like '%Washington%'`
 
Sample data in stagingevents:
`select * from stagingevents limit 1`
 

