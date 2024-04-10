# Lab 3: Spark and Parquet Optimization Report

Name:
 
NetID: 

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

SELECT s.sid, s.sname, COUNT(DISTINCT b.bname) FROM sailors s JOIN reserves r ON s.sid = r.sid JOIN boats b ON r.bid = b.bid GROUP BY s.sid, s.sname

```


Output:
```

+---+-------+---------------------+
|sid|  sname|count(DISTINCT bname)|
+---+-------+---------------------+
| 64|horatio|                    1|
| 22|dusting|                    3|
| 31| lubber|                    3|
| 74|horatio|                    1|
+---+-------+---------------------+

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
  distinct_terms=track_joined.groupby(track_joined.term).agg(countDistinct(track_joined.trackID).alias("distinct term count"))#calculating distinct terms
    top_pos_terms=distinct_terms.orderBy(col("distinct term count").desc()).limit(10)#calculating top 10 most popular
    top_pos_only_terms=top_pos_terms.select(top_pos_terms.term)#showing ONLY the terms
    print('Showing only the top 10 most popular terms')
    top_pos_only_terms.show()
    top_neg_terms=distinct_terms.orderBy("distinct term count").limit(10)#calculating top 10 least popular
    top_neg_only_terms=top_neg_terms.select(top_neg_terms.term)#showing ONLY the terms
    print('Showing only the top 10 least popular terms')
    top_neg_only_terms.show()

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


## Part 2: Parquet Optimization:

What to include in your report:
  - Tables of all numerical results (min, max, median) for each query/size/storage combination for part 2.3, 2.4 and 2.5.
  - How do the results in parts 2.3, 2.4, and 2.5 compare?
  - What did you try in part 2.5 to improve performance for each query?
  - What worked, and what didn't work?

Basic Markdown Guide: https://www.markdownguide.org/basic-syntax/
