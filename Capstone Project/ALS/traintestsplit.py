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
from pyspark.sql import functions as F
from pyspark.sql import DataFrameStatFunctions as statFunc
from pyspark.sql import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import countDistinct
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import percent_rank



# # def main(spark, ratings):
# #     '''Main routine for run for Storage optimization template.
# #     Parameters
# #     ----------
# #     spark : SparkSession object



#     '''
#     #####--------------YOUR CODE STARTS HERE--------------#####

#     # # Input CSV file path
#     # input_paths = [
#     #     "hdfs:/user/ds7395_nyu_edu/ml-latest-small/ratings.csv",
#     #     "hdfs:/user/ds7395_nyu_edu/ml-latest/ratings.csv"
#     #     # "hdfs:/user/ds7395_nyu_edu/ml-latest-small/movies.csv",
#     #     # "hdfs:/user/ds7395_nyu_edu/ml-latest/movies.csv"
#     # ]

#     # # Corresponding output Parquet file base paths for train, validation, and test datasets
#     # output_base_paths = [
#     #     "hdfs:/user/ds7395_nyu_edu/ml-latest-small/ratings.parquet",
#     #     "hdfs:/user/ds7395_nyu_edu/ml-latest/ratings.parquet"
#     # ]
#     # #converting movies as parquet
#     # for input_path, output_path in zip(input_paths, output_base_paths):
#     #     df = spark.read.csv(input_path, header=True, inferSchema=True)
#     #     df.write.parquet(output_path)

#     # # Loop through each file, read CSV, split into train, validation, and test, and write to Parquet
#     # for input_path, base_path in zip(input_paths, output_base_paths):
#     #     df = spark.read.csv(input_path, header=True, inferSchema=True)

#     #     # Split the data into train, validation, and test sets (60%, 20%, 20% respectively)
#     #     (train_data, validation_data, test_data) = df.randomSplit([0.6, 0.2, 0.2], seed=42)

#     #     # Save each dataframe as a Parquet file
#     #     train_data.write.parquet(base_path + "train.parquet")
#     #     validation_data.write.parquet(base_path + "validation.parquet")
#     #     test_data.write.parquet(base_path + "test.parquet")

 
    







# # # Only enter this block if we're in main
# # if __name__ == "__main__":

# #     # Create the spark session object
# #     spark = SparkSession.builder.appName('traintestsplit').getOrCreate()

# #     #If you wish to command line arguments, look into the sys library(primarily sys.argv)
# #     #Details are here: https://docs.python.org/3/library/sys.html
# #     #If using command line arguments, be sure to add them to main function

# #     main(spark)

def main(spark, filepath):
    ratings = spark.read.parquet(filepath, inferSchema=True)
    ratings.printSchema()

    count_of_user = ratings.select('userId').distinct().count()

    print(count_of_user)

    temp = ratings.filter((col('userId') <= (2*count_of_user/3)) & (col('userId') >= (count_of_user/3)))
    temp_df = ratings.filter(col('userId') > (2*count_of_user/3))
    temp_df2 = ratings.filter(col('userId') < (count_of_user/3)) 

    temp.show(10)

    temp_df.show(10)

    temp_df2.show(10)

    window_1 = Window.partitionBy(temp_df['userId']).orderBy('timestamp')
    train_1 = temp_df.select('*', percent_rank().over(window_1).alias('rank')).filter(F.col('rank') <= .8).drop('rank')
    test = temp_df.select('*', percent_rank().over(window_1).alias('rank')).filter(F.col('rank') > .8).drop('rank')

    train_1.show(10)

    test.show(10)

    count_user_history2 = temp_df2.select('userId').groupby('userId').count()
    #Using the window function to partition each user into train and valid
    window_2 = Window.partitionBy(temp_df2['userId']).orderBy('timestamp')
    train_2 = temp_df2.select('*', percent_rank().over(window_2).alias('rank')).filter(F.col('rank')  <= .8).drop('rank')
    valid = temp_df2.select('*', percent_rank().over(window_2).alias('rank')).filter(F.col('rank') > .8).drop('rank')


    train_2.show(10)

    valid.show(10)


    dfs = [train_1, train_2, temp]

    # combining all the train dfs 
    train = reduce(DataFrame.unionAll, dfs)
    train = train.orderBy('userId')
    test = test.orderBy('userId')
    valid = valid.orderBy('userId')
    netID = 'ds7395'

    # Save the DataFrames as Parquet files with a single part file
    train.coalesce(1).write.mode('overwrite').parquet(f'hdfs:/user/ds7395_nyu_edu/ml-latest-small/trainsmall.parquet')
    valid.coalesce(1).write.mode('overwrite').parquet(f'hdfs:/user/ds7395_nyu_edu/ml-latest-small/validsmall.parquet')
    test.coalesce(1).write.mode('overwrite').parquet(f'hdfs:/user/ds7395_nyu_edu/ml-latest-small/testsmall.parquet')

    

    
if __name__ == "__main__":
    spark = SparkSession.builder.appName('Train test split').getOrCreate()
    filepath = sys.argv[1]  # Ensure this path is correctly passed as a command line argument
    main(spark, filepath)    
