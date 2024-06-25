"""
spark-submit --deploy-mode client   --executor-memory 40G   --driver-memory 10G   --executor-cores 10   --num-executors 50   --conf spark.executor.memoryOverhead=5G   --conf spark.dynamicAllocation.enabled=true   --conf spark.dynamicAllocation.minExecutors=5   --conf spark.dynamicAllocation.maxExecutors=200   --conf spark.sql.shuffle.partitions=400   als.py /user/ds7395_nyu_edu/ml-latest/trainlarge.parquet /user/ds7395_nyu_edu/ml-latest/validlarge.parquet /user/ds7395_nyu_edu/ml-latest/testlarge.parquet
"""
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, col
from pyspark.ml.recommendation import ALS
from pyspark.mllib.evaluation import RankingMetrics, RegressionMetrics
from pyspark import SparkContext
from pyspark.sql import SparkSession,Window
from pyspark.ml.evaluation import RegressionEvaluator,RankingEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import col,rank, explode
import pyspark.sql.functions as func
from pyspark.sql import SparkSession, Window, functions as F


def main(spark, train):
    """
    Main routine for processing MovieLens dataset using validation and test sets.

    Parameters
    ----------
    spark : SparkSession object
    val : str, path to the validation data parquet file
    test : str, path to the test data parquet file

    """
    # Reading parquet files
    training = spark.read.parquet(train ,inferSchema=True)
    #test = spark.read.parquet(test,inferSchema=True)
    #validation = spark.read.parquet(val,inferSchema=True)
    #ratingsTrain = ratingsTrain.withColumn('userId', col('userId').cast('integer')).withColumn('movieId', col('movieId').cast('integer')).withColumn('rating', col('rating').cast('float')).drop('timestamp')
    


    training = training.drop("timestamp")
    training.createOrReplaceTempView('training')
    # test = test.drop("timestamp")
    # test.createOrReplaceTempView('test')
    # test_ranking = test.withColumn("rn", F.row_number().over(Window.partitionBy("userId").orderBy(F.col("rating").desc()))).filter(f"rn <= {100}").groupBy("userId").agg(F.collect_list(F.col("movieId")).alias("movie_list"))
    # test_ranking.createOrReplaceTempView('test_ranking')

    start_time = time.time()

    als = ALS(rank=200, maxIter=15, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating", nonnegative=True ,coldStartStrategy="drop")
    print("Calling model")
    try:
        model = als.fit(training)
        print("Model training completed.")
    except Exception as e:
        print("Error during model training:", e)
        spark.stop()
        raise SystemExit("Exiting the application due to model training failure.")

    end_time = time.time()
    print("Total ALS model training time: {} seconds".format(end_time - start_time))

    model_path = "/user/ds7395_nyu_edu/ml-latest/models/als_model"
    model.write().overwrite().save(model_path)
    print(f"Model saved to {model_path}")

    # validation = validation.drop("timestamp")
    # validation.createOrReplaceTempView('validation')
    # print("Testing on validation set")
    # # tuning the hyperparameters by evaluating the model on validation dataset
    # users_valid = validation.select("userId").distinct()
    # predictions_valid = model.recommendForUserSubset(users_valid, 100)
    # predictions_valid = predictions_valid.withColumn("recommendations", explode(col("recommendations"))).select("userId", "recommendations.movieId", "recommendations.rating")
    # predicted_items_valid = predictions_valid.withColumn("rn", F.row_number().over(Window.partitionBy("userId").orderBy(F.col("rating").desc()))).groupBy("userId").agg(F.collect_list(F.col("movieId")).alias("predicted_movie_list"))
    # predictions_valid = predictions_valid.rdd.map(lambda r: ((r.userId, r.movieId), r.rating)).toDF(["user_movie", "predictions_rating"])

    # ratingsTuple_valid = validation.rdd.map(lambda r: ((r[0], r[1]), r[2])).toDF(["user_movie", "rating"])
    # ratingsTuple_valid.createOrReplaceTempView('ratingsTuple_v')
    # predictions_valid.createOrReplaceTempView('predictions_v')
    # scoreAndLabels_als_valid = spark.sql('SELECT rating, predictions_rating FROM predictions_v INNER JOIN ratingsTuple_v ON ratingsTuple_v.user_movie = predictions_v.user_movie').na.drop()

    # valid_top_user_movies = validation.withColumn("rn", F.row_number().over(Window.partitionBy("userId").orderBy(F.col("rating").desc()))).filter(f"rn <= {100}").groupBy("userId").agg(F.collect_list(F.col("movieId")).alias("movie_list"))

    # valid_top_user_movies.createOrReplaceTempView('valid_top_user_movies')
    # predicted_items_valid.createOrReplaceTempView('predicted_items_valid')
    # rating_metrics_tuple_valid = spark.sql('SELECT movie_list, predicted_movie_list FROM valid_top_user_movies INNER JOIN predicted_items_valid ON predicted_items_valid.userId = valid_top_user_movies.userId').na.drop()

    # # Instantiate ranking metrics object
    # ranking_metrics_als_valid = RankingMetrics(rating_metrics_tuple_valid.rdd)

    # print("ALS validation set Mean average precision at 100 = %s" % ranking_metrics_als_valid.meanAveragePrecisionAt(100))

    # print("ALS validation set baseline NDCG at 100 = %s" % ranking_metrics_als_valid.ndcgAt(100))

    
    # # Instantiate regression metrics to compare predicted and actual ratings
    # #metrics_als_valid = RegressionMetrics(scoreAndLabels_als_valid.rdd)

    # # Root mean squared error on validation set
    # #print("ALS Validation set RMSE = %s" % metrics_als_valid.rootMeanSquaredError)

    #print("Testing on test set")


    # # Evaluate the model by computing the RMSE on the test data
    # users = test.select("userId").distinct()
    # predictions = model.recommendForUserSubset(users, 100)
    # predictions = predictions.withColumn("recommendations", explode(col("recommendations"))).select("userId", "recommendations.movieId", "recommendations.rating")
    # predicted_items_test = predictions.withColumn("rn", F.row_number().over(Window.partitionBy("userId").orderBy(F.col("rating").desc()))).groupBy("userId").agg(F.collect_list(F.col("movieId")).alias("predicted_movie_list"))
    # predictions = predictions.rdd.map(lambda r: ((r.userId, r.movieId), r.rating)).toDF(["user_movie", "predictions_rating"])

    # ratingsTuple = test.rdd.map(lambda r: ((r[0], r[1]), r[2])).toDF(["user_movie", "rating"])
    # ratingsTuple.createOrReplaceTempView('ratingsTuple')
    # predictions.createOrReplaceTempView('predictions')
    # scoreAndLabels_als = spark.sql('SELECT rating, predictions_rating FROM predictions INNER JOIN ratingsTuple ON ratingsTuple.user_movie = predictions.user_movie').na.drop()

    # predicted_items_test.createOrReplaceTempView('predicted_items_test')
    # rating_metrics_tuple_test = spark.sql('SELECT movie_list, predicted_movie_list FROM test_ranking INNER JOIN predicted_items_test ON predicted_items_test.userId = test_ranking.userId').na.drop()
    # # Instantiate ranking metrics object
    # ranking_metrics_als_test = RankingMetrics(rating_metrics_tuple_test.rdd)

    # print("ALS test set Mean average precision at 100 = %s" % ranking_metrics_als_test.meanAveragePrecisionAt(100))

    # print("ALS test set baseline NDCG at 100 = %s" % ranking_metrics_als_test.ndcgAt(100))

    # # Instantiate regression metrics to compare predicted and actual ratings
    # #metrics_als = RegressionMetrics(scoreAndLabels_als.rdd)

    # # Root mean squared error
    # #print("Test set RMSE = %s" % metrics_als.rootMeanSquaredError)

   

    

if __name__ == "__main__":
    spark = SparkSession.builder.appName('ALS Model Training').getOrCreate()
    train = sys.argv[1]

    main(spark, train)

