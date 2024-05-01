#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py
'''
import os

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, max, countDistinct, avg
from pyspark.sql.functions import percentile_approx



def main(spark, userID):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    userID : string, userID of student to find files in HDFS
    '''
    
    print('Lab 3 Example dataframe loading and SQL query')

    # Load the boats.txt and sailors.json data into DataFrame
    boats = spark.read.csv(f'hdfs:/user/{userID}/boats.txt')
    sailors = spark.read.json(f'hdfs:/user/{userID}/sailors.json')
    reserves= spark.read.json(f'hdfs:/user/{userID}/reserves.json')

    print('Printing boats inferred schema')
    boats.printSchema()
    print('Printing sailors inferred schema')
    sailors.printSchema()
    print('Printing reserves inferred schema')
    reserves.printSchema()

    # Why does sailors already have a specified schema?

    print('Reading boats.txt and specifying schema')
    boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')

    print('Printing boats with specified schema')
    boats.printSchema()

    # Give the dataframe a temporary view so we can run SQL queries
    boats.createOrReplaceTempView('boats')
    sailors.createOrReplaceTempView('sailors')
    reserves.createOrReplaceTempView('reserves')
    # Construct a query
    print('Example 1: SELECT age FROM sailors WHERE age>35 with SparkSQL (playing with spark)')
    query = spark.sql('SELECT age FROM sailors WHERE age>35')

    # Print the results to the console
    query.show()


    #####--------------YOUR CODE STARTS HERE--------------#####

    #make sure to load reserves.json, artist_term.csv, and tracks.csv
    #For the CSVs, make sure to specify a schema!
    #sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)
    print('Q1: sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age) in SQL')
    question_1_query = spark.sql('SELECT sid, sname, age FROM sailors WHERE age>40')
    #printing query
    question_1_query.show()
    #spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')
    print('Q2: reserves.filter(reserves.bid !=101).groupby(reserves.sid).agg(count(reserves.bid)) in python object interface')
    question_2_query= reserves.filter(reserves.bid !=101).groupby(reserves.sid).agg(count(reserves.bid))
    question_2_query.show()
    print('Q3: SELECT s.sid, s.sname, COUNT(DISTINCT b.bname) FROM sailors s JOIN reserves r ON s.sid = r.sid JOIN boats b ON r.bid = b.bid GROUP BY s.sid, s.sname')
    question_3_query=spark.sql('SELECT s.sid, s.sname, COUNT(DISTINCT b.bid) FROM sailors s JOIN reserves r ON s.sid = r.sid JOIN boats b ON r.bid = b.bid GROUP BY s.sid, s.sname')
    question_3_query.show()
    #bigger datasets
    artist_term = spark.read.csv(f'hdfs:/user/{userID}/artist_term.csv')
    artist_term.printSchema()
    #changing schema of artist_term as required
    artist_schema= StructType([
    StructField("artistID", StringType(), True),
    StructField("term", StringType(), True)
    ])
    artist_term = spark.read.csv(f'hdfs:/user/{userID}/artist_term.csv', schema=artist_schema)
    print('Printing artist_term with correct schema')
    artist_term.printSchema()
    tracks = spark.read.csv(f'hdfs:/user/{userID}/tracks.csv')
    tracks.printSchema()
    #changing tracks schema
    tracks_schema=StructType([
    StructField("trackID", StringType(), True),
    StructField("title", StringType(), True),
    StructField("release", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("duration", FloatType(), True),
    StructField("artistID", StringType(), True)
    ])
    tracks = spark.read.csv(f'hdfs:/user/{userID}/tracks.csv', schema=tracks_schema)
    print('Printing tracks with correct schema')
    tracks.printSchema()
    #Implement a query using Spark transformations which finds for each artist term, compute the median year of release, maximum track duration, and the total number of artists for that term (by ID). What are the results for the ten terms with the shortest average track durations? Include both your query code and resulting DataFrame in your response.
    print('Query to show the results for the ten terms with the shortest average track durations')
    track_joined=tracks.join(artist_term, "artistID")#joining tracks and artist_term on artistID
    summed=track_joined.groupby(track_joined.term).agg(
    percentile_approx(track_joined.year,0.5).alias("Median year"),
    max(track_joined.duration).alias("Max duration"),
    countDistinct(track_joined.artistID).alias("Total artists"))#calculating median year, max duration, and total number of artists
    avg_duration=track_joined.groupBy(track_joined.term).agg(avg(track_joined.duration).alias("Average Duration"))#calculating avg duration
    avg_sum_joined=summed.join(avg_duration, "term")#joining aggregate calculation and avg duration calculation to print
    top_lowest=avg_sum_joined.orderBy("Average Duration").limit(10)#calculating top 10 lowest avg track duration
    top_lowest.show()
    #part 5
    print('Spark transformations that finds the number of distinct tracks associated (through artistID) to each term.')
    distinct_terms=track_joined.groupby(track_joined.term).agg(countDistinct(track_joined.trackID).alias("distinct term count"))#calculating distinct terms
    top_pos_terms=distinct_terms.orderBy(col("distinct term count").desc()).limit(10)#calculating top 10 most popular
    top_pos_only_terms=top_pos_terms.select(top_pos_terms.term)#showing ONLY the terms
    print('Showing only the top 10 most popular terms')
    top_pos_only_terms.show()
    top_neg_terms=distinct_terms.orderBy("distinct term count").limit(10)#calculating top 10 least popular
    top_neg_only_terms=top_neg_terms.select(top_neg_terms.term)#showing ONLY the terms
    print('Showing only the top 10 least popular terms')
    top_neg_only_terms.show()
    #top_neg_terms.show()
    
    
    






# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user userID from the command line
    # We need this to access the user's folder in HDFS
    userID = os.environ['USER']

    # Call our main routine
    main(spark, userID)
