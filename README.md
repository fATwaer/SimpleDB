simple-db
=========

Code for all 6.830 labs will be available in this repo. Once you have set up your class repo, you pull lab code from here.

Directions for Repo Setup
-------------------------

Directions can be [here](https://github.com/MIT-DB-Class/course-info-2018)

Lab Submission
-----

Instructions for labs (including how to submit answers) are [here](https://github.com/MIT-DB-Class/course-info-2018)




Output Test
---
	
	java -jar dist/simpledb.jar parser catalog.txt

to run the database.

``` bash

SimpleDB> select * from data d;
Started a new transaction tid = 2
Added scan of table d
Added select list field null.*
The query plan is:
  π(d.f1,d.f2),card:0
  |
scan(data d)

d.f1	d.f2	
------------------
0
1 10 
2 20 
3 30 
4 40 
5 50 
6 60 
5 50 

 7 rows.
Transaction 2 committed.
```
