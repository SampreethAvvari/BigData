#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client pq_big_spender.py <file_path>
'''


# Import command line arguments and helper functions
import sys
import bench
import statistics

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def pq_big_spender(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will contains users with at least 100 orders but
    do not yet have a rewards card.

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the Parquet-backed file, e.g.,
        `hdfs:/user/{YOUR NETID}/peopleSmall.parquet`

    Returns
    df_big_spender:
        Uncomputed dataframe of the maximum income grouped by last_name
    '''

    bigspender = spark.read.parquet(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')

    bigspender.createOrReplaceTempView('bigspender')
    #DataFrame which computes users having at least 100 orders, and are not currently signed up for the rewards credit card program.
    res2=spark.sql('SELECT last_name, first_name, SUM(orders) AS total_orders FROM bigspender WHERE rewards=FALSE  GROUP BY last_name, first_name HAVING SUM(orders) >=100')
    
    
    return res2



def main(spark, file_path):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    which_dataset : string, size of dataset to be analyzed
    '''
    times = bench.benchmark(spark, 25, pq_big_spender, file_path)

    print(f'Times to run Query 25 times on {file_path}')
    print(times)
    print(f'Maximum Time taken to Big Spender Query 25 times on {file_path}:{max(times)}')
    print(f'Minimum Time taken to Big Spender Query 25 times on {file_path}:{min(times)}')
    print(f'Median Time taken to run Big Spender Query 25 times on {file_path}:{statistics.median(times)}')

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    # Get file_path for dataset to analyze
    file_path = sys.argv[1]

    main(spark, file_path)
