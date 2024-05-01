# Lab 3: Spark and Parquet Optimization Report

Name: Dhruv Sridhar, Barath RamaShankar, Sampreeth Avarri
 
NetID: ds7395, br2543, spa9659

## Part 1: Spark

#### Question 1: 
How would you express the following computation using SQL instead of the object interface: `sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)`?

Code:
```SQL

question_1_query = spark.sql('SELECT sid, sname, age FROM sailors WHERE age>40')

```


Output:
```

Q1: sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age) in SQL
+---+-------+----+
|sid|  sname| age|
+---+-------+----+
| 22|dusting|45.0|
| 31| lubber|55.5|
| 95|    bob|63.5|
+---+-------+----+

```


#### Question 2: 
How would you express the following using the object interface instead of SQL: `spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')`?

Code:
```python

reserves.filter(reserves.bid !=101).groupby(reserves.sid).agg(count(reserves.bid))

```


Output:
```

+---+----------+
|sid|count(bid)|
+---+----------+
| 22|         3|
| 31|         3|
| 74|         1|
| 64|         1|
+---+----------+

```

#### Question 3: 
Using a single SQL query, how many distinct boats did each sailor reserve? 
The resulting DataFrame should include the sailor's id, name, and the count of distinct boats. 
(Hint: you may need to use `first(...)` aggregation function on some columns.) 
Provide both your query and the resulting DataFrame in your response to this question.

Code:
```SQL

SELECT s.sid, s.sname, COUNT(DISTINCT b.bid) FROM sailors s JOIN reserves r ON s.sid = r.sid JOIN boats b ON r.bid = b.bid GROUP BY s.sid, s.sname

```


Output:
```

+---+-------+-------------------+
|sid|  sname|count(DISTINCT bid)|
+---+-------+-------------------+
| 64|horatio|                  2|
| 22|dusting|                  4|
| 31| lubber|                  3|
| 74|horatio|                  1|
+---+-------+-------------------+

```

#### Question 4: 
Implement a query using Spark transformations which finds for each artist term, compute the median year of release, maximum track duration, and the total number of artists for that term (by ID).
  What are the results for the ten terms with the shortest *average* track durations?
  Include both your query code and resulting DataFrame in your response.


Code:
```python

    track_joined=tracks.join(artist_term, "artistID")#joining tracks and artist_term on artistID
    summed=track_joined.groupby(track_joined.term).agg(
    percentile_approx(track_joined.year,0.5).alias("Median year"),
    max(track_joined.duration).alias("Max duration"),
    countDistinct(track_joined.artistID).alias("Total artists"))#calculating median year, max duration, and total number of artists
    avg_duration=track_joined.groupBy(track_joined.term).agg(avg(track_joined.duration).alias("Average Duration"))#calculating avg duration
    avg_sum_joined=summed.join(avg_duration, "term")#joining aggregate calculation and avg duration calculation to print
    top_lowest=avg_sum_joined.orderBy("Average Duration").limit(10)
    top_lowest.show()

```


Output:
```

+----------------+-----------+------------+-------------+------------------+
|            term|Median year|Max duration|Total artists|  Average Duration|
+----------------+-----------+------------+-------------+------------------+
|       mope rock|          0|    13.66159|            1|13.661589622497559|
|      murder rap|          0|    15.46404|            1| 15.46403980255127|
|    abstract rap|       2000|    25.91302|            1| 25.91301918029785|
|experimental rap|       2000|    25.91302|            1| 25.91301918029785|
|     ghetto rock|          0|    26.46159|            1|26.461589813232422|
|  brutal rapcore|          0|    26.46159|            1|26.461589813232422|
|     punk styles|          0|    41.29914|            1| 41.29914093017578|
|     turntablist|       1993|   145.89342|            1| 43.32922387123108|
| german hardcore|          0|    45.08689|            1|45.086891174316406|
|     noise grind|       2005|    89.80853|            2| 47.68869247436523|
+----------------+-----------+------------+-------------+------------------+

```
#### Question 5: 
Create a query using Spark transformations that finds the number of distinct tracks associated (through artistID) to each term.
  Modify this query to return only the top 10 most popular terms, and again for the bottom 10.
  Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response.
  ```python
  distinct_terms=track_joined.groupby(track_joined.term).agg(countDistinct(track_joined.trackID).alias("distinct term count"))#calculating distinct terms
    top_pos_terms=distinct_terms.orderBy(col("distinct term count").desc()).limit(10)#calculating top 10 most popular
    top_pos_only_terms=top_pos_terms.select(top_pos_terms.term)#showing ONLY the terms
    print('Showing only the top 10 most popular terms')
    top_pos_only_terms.show()
    top_neg_terms=distinct_terms.orderBy("distinct term count").limit(10)#calculating top 10 least popular
    top_neg_only_terms=top_neg_terms.select(top_neg_terms.term)#showing ONLY the terms
    print('Showing only the top 10 least popular terms')
    top_neg_only_terms.show()

```
Output: 
```
+----------------+
|            term|
+----------------+
|            rock|
|      electronic|
|             pop|
|alternative rock|
|         hip hop|
|            jazz|
|   united states|
|        pop rock|
|     alternative|
|           indie|
+----------------+

+--------------------+
|                term|
+--------------------+
|         polish kicz|
|               watmm|
|contemporary classic|
|     toronto hip hop|
|     german hardcore|
|       dranouter2007|
|      swedish artist|
|        pukkelpop 07|
|     classical tango|
|            mandinka|
+--------------------+ 

```


## Part 2: Parquet Optimization:

What to include in your report:
  - Tables of all numerical results (min, max, median) for each query/size/storage combination for part 2.3, 2.4 and 2.5.
  - How do the results in parts 2.3, 2.4, and 2.5 compare?
  - What did you try in part 2.5 to improve performance for each query?
  - What worked, and what didn't work?

Basic Markdown Guide: https://www.markdownguide.org/basic-syntax/

Execution times for the CSV files

| Sl.No | Test Case                           | Max Execution Time | Min Execution Time | Median Execution Time |
|-------|-------------------------------------|--------------------|--------------------|-----------------------|
| 1     | csv_sum_orders on small data        | 5.728994607925415  | 0.2049577236175537 | 0.2760331630706787    |
| 2     | csv_sum_orders on moderate data     | 6.46602725982666   | 0.3755972385406494 | 0.4761350154876709    |
| 3     | csv_sum_orders on big data          | 24.699562311172485 | 14.388707637786865 | 15.060358047485352    |




| Sl.No | Test Case                         | Max Execution Time | Min Execution Time | Median Execution Time |
|-------|-----------------------------------|--------------------|--------------------|-----------------------|
| 1     | csv_big_spender on small data      | 6.47797441482544   | 0.242637872695923  | 0.302946567535400     |
| 2     | csv_big_spender on moderate data   | 10.45426630973816  | 0.747713327407837  | 0.862599611282349     |
| 3     | csv_big_spender on big data        | 42.38619875907898  | 23.4384593963623   | 31.14611649513245     |




| Sl.No | Test Case                           | Max Execution Time | Min Execution Time | Median Execution Time |
|-------|-------------------------------------|--------------------|--------------------|-----------------------|
| 1     | csv_brian on small data             | 8.360567331314087  | 0.2334151268005371 | 0.36003923416137695   |
| 2     | csv_brian on moderate data          | 7.874406099319458  | 0.400028467178344  | 0.5806665420532227    |
| 3     | csv_brian on big data               | 18.27330183982849  | 10.12610673904419  | 10.934720993041992    |


Execution times for the Parquet files

| Sl.No | Test Case                           | Max Execution Time | Min Execution Time | Median Execution Time |
|-------|-------------------------------------|--------------------|--------------------|-----------------------|
| 1     |  pq_sum_orders on small data        | 9.047239303588867  | 0.28946638107299805| 0.3924369812011719    |
| 2     |  pq_sum_orders on moderate data     | 8.256673574447632  | 0.40137815475463867| 0.5094122886657715    |
| 3     |  pq_sum_orders on big data          | 8.562147378921509  | 2.2246053218841553 | 2.581321954727173     |



| Sl.No | Test Case                               | Max Execution Time | Min Execution Time | Median Execution Time |
|-------|-----------------------------------------|--------------------|--------------------|-----------------------|
| 1     | pq_big_spender on small data            | 6.227829694747925  | 0.246371269226074  | 0.336318492889404     |
| 2     | pq_big_spender on moderate data         | 10.995121479034424 | 0.544761896133423  | 0.652388334274292     |
| 3     | pq_big_spender on big data              | 17.984126567840576 | 8.200473546981812  | 11.06123161315918     |



| Sl.No | Test Case                           | Max Execution Time | Min Execution Time | Median Execution Time |
|-------|-------------------------------------|--------------------|--------------------|-----------------------|
| 1     |  pq_brian on small data             | 3.638312816619873  | 0.2100076675415039 | 0.2817726135253906    |
| 2     |  pq_brian on moderate data          | 5.309568881988525  | 0.3055000305175781 | 0.43214845657348633   |
| 3     |  pq_brian on big data               | 10.70052194595337  | 2.21852707862854   | 2.53103756904602055   |



Execution times for the Optimized Parquet files


| Sl.No | Test Case                               | Max Execution Time | Min Execution Time | Median Execution Time |
|-------|-----------------------------------------|--------------------|--------------------|-----------------------|
| 1     |Optimised pq_sum_orders on small data    | 5.146566867828369  | 0.20232892036437988| 0.24171113967895508   |
| 2     |Optimised pq_sum_orders on moderate data | 5.643989562988281  | 0.1944580078125    | 0.23640179634094238   |
| 3     |Optimised pq_sum_orders on big data      | 3.6185762882232666 | 0.2273845672607422 | 0.27552008628845215   |



| Sl.No | Test Case                                             | Max Execution Time | Min Execution Time | Median Execution Time |
|-------|-------------------------------------------------------|--------------------|--------------------|-----------------------|
| 1     | Optimized pq_big_spender on small big_spender data    | 5.883975505828857  | 0.255818605422974  | 0.346256494522095     |
| 2     | Optimized pq_big_spender on moderate big_spender data | 8.316442489624023  | 3.705665349960327  | 3.858831405639648     |
| 3     | Optimized pq_big_spender on big big_spender data      | 9.746892929077148  | 4.653920412063599  | 4.967272758483887     |




| Sl.No | Test Case                               | Max Execution Time | Min Execution Time | Median Execution Time |
|-------|-----------------------------------------|--------------------|--------------------|-----------------------|
| 1     |Optimised pq_brian on small data         | 3.3438291549682617 | 0.3049163818359375 | 0.3593902587890625    |
| 2     |Optimised pq_brian on moderate data      | 3.9505269527435303 | 0.17731261253356934| 0.24671316146850586   |
| 3     |Optimised pq_brian on big data           | 2.842482805252075  | 0.14726758003234863| 0.17941975593566895   |



  - How do the results in parts 2.3, 2.4, and 2.5 compare?
  From the computations done, we can see that parquet does indeed improve the computation speed on larger files. If we take a closer look, we can see that for all the queries, parquet significantly improves the execution time as the size of the file gets bigger. For instance, in the sum_orders query, on the small dataset, parquet file performance degraded. However when we run the query on larger files, the performance improved by more than 50%.  


  -What did you try in part 2.5 to improve performance for each query?
   What worked, and what didn't work?
  We will discuss the ways we tried to optimize each queries. 

  Sum_orders query:
  
  For this query we used the following code to run it- "SELECT COUNT(orders) FROM summed_order GROUP BY zipcode;"
  Here, we can see that we are grouping by "zipcode" and it makes sense to repartition by zipcode so that when we run the query it spark redistributes the data based on the hash of the column. All data belonging to a specific zipcode is all in the same partition. This increased our execution times. We also tried setting replication factor which did not work due to issues with dataproc. We also tried ordering by zipcode but the first method was the best to improve execution time.
  below is the optimization code that improved execution time for sum_orders query
 ```
sum_orders_small=spark.read.csv("hdfs:/user/pw44_nyu_edu/peopleSmall.csv", header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')

sum_orders_small.repartition("zipcode").write.parquet("hdfs:/user/ds7395_nyu_edu/people_small_sum_orders.parquet")
 ```
big_spender_query:

  the code for this query is- "SELECT last_name, first_name, zipcode, SUM(orders) AS total_orders FROM bigspender WHERE rewards=FALSE  GROUP BY last_name, first_name, zipcode HAVING SUM(orders) >=100". Here we are doing groupby again as we need unique values. Here we tried a lot of different things that did not seem to work. We spoke about this with Pascal as well and we had issues because of dataproc being overloaded. We tried filtering based on rewards=False and ordering by columns zipcode, first_name and last_name. This did not seem to work. We also tried setting replication factor here as well but once again due to dataproc issues we were unable to make this work. Another we issue we faced was inconsistent results due to dataproc being heavily used. We ran the pq_big_spender on small and moderate last week and upadted the values. However, when we ran this on the big data file, the time increased again. We spoke to Pascal and he said that this was happening due to dataproc being used by lot of people at the same time. Finally when we changed the SQL code to "SELECT last_name, first_name, SUM(orders) AS total_orders FROM bigspender WHERE rewards=FALSE  GROUP BY last_name, first_name HAVING SUM(orders) >=100" and then filtered based on rewards=False and repartitioned based on first_name and last_name, the execution times improved (Possibly because dataproc issue was resolved).

  below is the optimization code that improved execution time for big_spender query
  ```
  big_spender_small=spark.read.csv("hdfs:/user/pw44_nyu_edu/peopleSmall.csv", header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')

  big_spender_small.filter(big_spender_small['rewards']==False).repartition("first_name","last_name").write.parquet("hdfs:/user/br2543_nyu_edu/homework-4-69/part_2_storage/people_small_big_spender.parquet")
```
  Brian query:
  
  the code for this query is- "SELECT last_name,first_name, zipcode FROM brian_query WHERE first_name='Brian' AND loyalty=FALSE GROUP BY last_name, first_name, zipcode". In this query, we are grouping by first_name, last_name and zipcode to get unique values. To optimize this, we tried different things like filtering on first_name= Brian and then repartioning based on zipcode. This did not improve performance. We then decided to repartition by loyalty and the use PartitionBy on the first_name column, this arranges data having unique first names and this improved the execution times. We also tried replication factor here as well, but once again due to data proc issues it did not give us the results.  
  below is the optimization code that improved execution time for brian query
  ```
brian_small=spark.read.csv("hdfs:/user/pw44_nyu_edu/peopleSmall.csv", header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')

brian_small.repartition(2 ,"loyalty").write.partitionBy("first_name").parquet("hdfs:/user/ds7395_nyu_edu/people_small_brian.parquet")
```

  
