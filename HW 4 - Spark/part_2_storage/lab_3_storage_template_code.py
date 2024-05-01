#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Template script to connect to Active Spark Session
Usage:
    $ spark-submit --deploy-mode client lab_3_storage_template_code.py <any arguments you wish to add>
'''


# Import command line arguments and helper functions(if necessary)
import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession



def main(spark):
    '''Main routine for run for Storage optimization template.
    Parameters
    ----------
    spark : SparkSession object

    '''
    #####--------------YOUR CODE STARTS HERE--------------#####

    # Use this template to as much as you want for your parquet saving and optimizations!
    input_paths = [
        "hdfs:/user/pw44_nyu_edu/peopleSmall.csv",
        "hdfs:/user/pw44_nyu_edu/peopleModerate.csv",
        "hdfs:/user/pw44_nyu_edu/peopleBig.csv"
    ]

    # Corresponding output Parquet file paths
    output_paths = [
        "hdfs:/user/br2543_nyu_edu/homework-4-69/part_2_storage/people_small.parquet",
        "hdfs:/user/br2543_nyu_edu/homework-4-69/part_2_storage/people_moderate.parquet",
        "hdfs:/user/br2543_nyu_edu/homework-4-69/part_2_storage/people_big.parquet"
    ]

    # # Loop through each file, read CSV and write to Parquet
    for input_path, output_path in zip(input_paths, output_paths):
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        df.write.parquet(output_path)
    # spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

    #repartioning files for pq_sum_orders
    # sum_orders_small=spark.read.csv("hdfs:/user/pw44_nyu_edu/peopleSmall.csv", header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    # sum_orders_small.repartition("zipcode").write.parquet("hdfs:/user/ds7395_nyu_edu/people_small_sum_orders.parquet")

    # sum_orders_moderate=spark.read.csv("hdfs:/user/pw44_nyu_edu/peopleModerate.csv", header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    # sum_orders_moderate.repartition("zipcode").write.parquet("hdfs:/user/ds7395_nyu_edu/people_moderate_sum_orders.parquet")

    # sum_orders_big=spark.read.csv("hdfs:/user/pw44_nyu_edu/peopleBig.csv", header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    # sum_orders_big.repartition("zipcode").write.parquet("hdfs:/user/ds7395_nyu_edu/people_big_sum_orders.parquet")

    #repartioning files for pq_big_spender

    big_spender_small=spark.read.csv("hdfs:/user/pw44_nyu_edu/peopleSmall.csv", header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    big_spender_small.filter(big_spender_small['rewards']==False).repartition("first_name","last_name").write.parquet("hdfs:/user/br2543_nyu_edu/homework-4-69/part_2_storage/people_small_big_spender.parquet")

    big_spender_moderate=spark.read.csv("hdfs:/user/pw44_nyu_edu/peopleModerate.csv", header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    big_spender_moderate.filter(big_spender_moderate['rewards']==False).repartition("first_name","last_name").write.parquet("hdfs:/user/br2543_nyu_edu/homework-4-69/part_2_storage/people_moderate_big_spender.parquet")

    big_spender_big=spark.read.csv("hdfs:/user/pw44_nyu_edu/peopleBig.csv", header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    big_spender_big.filter(big_spender_big["rewards"] == False).repartition("first_name","last_name").write.parquet("hdfs:/user/br2543_nyu_edu/homework-4-69/part_2_storage/people_big_big_spender.parquet")


    # # repartioning files for pq_big_spender
    # brian_small=spark.read.csv("hdfs:/user/pw44_nyu_edu/peopleSmall.csv", header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    # brian_small.repartition(2 ,"loyalty").write.partitionBy("first_name").parquet("hdfs:/user/ds7395_nyu_edu/people_small_brian.parquet")

    # brian_moderate=spark.read.csv("hdfs:/user/pw44_nyu_edu/peopleModerate.csv", header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    # brian_moderate.repartition(2 ,"loyalty").write.partitionBy("first_name").parquet("hdfs:/user/ds7395_nyu_edu/people_moderate_brian.parquet")

    # # brian_big=spark.read.csv("hdfs:/user/pw44_nyu_edu/peopleBig.csv", header=True, schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    # # brian_big.repartition(2 ,"loyalty").write.partitionBy("first_name").parquet("hdfs:/user/ds7395_nyu_edu/people_big_brian.parquet")
    







# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    #If you wish to command line arguments, look into the sys library(primarily sys.argv)
    #Details are here: https://docs.python.org/3/library/sys.html
    #If using command line arguments, be sure to add them to main function

    main(spark)
    
