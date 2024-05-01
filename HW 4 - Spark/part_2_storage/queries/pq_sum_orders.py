#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client pq_sum_orders.py <file_path>
'''


# Import command line arguments and helper functions
import sys
import bench

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
import statistics


def pq_sum_orders(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will compute the total orders grouped by zipcode

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the Parquet-backed file, e.g.,
        `hdfs:/user/{YOUR NETID}/peopleSmall.parquet

    Returns
    df_sum_orders:
        Uncomputed dataframe of total orders grouped by zipcode
    '''
    summed_order = spark.read.parquet(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')

    summed_order.createOrReplaceTempView('summed_order')
    
    res1 = spark.sql('SELECT COUNT(orders) FROM summed_order GROUP BY zipcode;')
    #res=res1.repartition("zipcode")
    #res1.explain()
    #summed_order.groupby(summed_order.zipcode).agg(count(summed_order.orders))

    return res1






def main(spark, file_path):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    which_dataset : string, size of dataset to be analyzed
    '''
    times = bench.benchmark(spark, 25, pq_sum_orders, file_path)

    print(f'Times to run Basic Query 5 times on {file_path}')
    print(times)
    print(f'Maximum Time taken to Sum Orders Query 25 times on {file_path}:{max(times)}')
    print(f'Minimum Time taken to Sum Orders Query 25 times on {file_path}:{min(times)}')
    print(f'Median Time taken to run Sum Orders Query 25 times on {file_path}:{statistics.median(times)}')

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    # Get file_path for dataset to analyze
    file_path = sys.argv[1]

    main(spark, file_path)
