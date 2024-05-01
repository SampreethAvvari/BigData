#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client csv_brian.py <file_path>
'''


# Import command line arguments and helper functions
import sys
import bench
import statistics
# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def csv_brian(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will filters down to only include people with `first_name`
    of 'Brian' that are not yet in the loyalty program

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the CSV file, e.g.,
        `hdfs:/user/pw44_nyu_edu/peopleSmall.csv`

    Returns
    df_brian:
        Uncomputed dataframe that only has people with 
        first_name of 'Brian' and not in the loyalty program
    '''

    brian_query = spark.read.csv(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')

    brian_query.createOrReplaceTempView('brian_query')
    #DataFrame which computes users having at least 100 orders, and are not currently signed up for the rewards credit card program.
    res3=spark.sql("SELECT  last_name,first_name, zipcode FROM brian_query WHERE first_name='Brian' AND loyalty=FALSE GROUP BY last_name, first_name, zipcode ")
    res3.explain()
    return res3



def main(spark, file_path):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    which_dataset : string, size of dataset to be analyzed
    '''
    times = bench.benchmark(spark, 25, csv_brian, file_path)

    print(f'Times to run Basic Query 25 times on {file_path}')
    print(times)
    print(f'Maximum Time taken to Brian Query 25 times on {file_path}:{max(times)}')
    print(f'Minimum Time taken to Brian Query 25 times on {file_path}:{min(times)}')
    print(f'Median Time taken to run Brian Query 25 times on {file_path}:{statistics.median(times)}')

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    # Get file_path for dataset to analyze
    file_path = sys.argv[1]

    main(spark, file_path)
