import sys
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.ml.recommendation import ALSModel
from pyspark.mllib.evaluation import RankingMetrics
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

def main(spark, val, test):
    # Load the model
    model_path = "/user/ds7395_nyu_edu/ml-latest/models/als_model"
    model = ALSModel.load(model_path)
    print("Model loaded successfully.")

    # Load validation and test data
    validation = spark.read.parquet(val, inferSchema=True).drop("timestamp")
    test = spark.read.parquet(test, inferSchema=True).drop("timestamp")

    test = test.drop("timestamp")
    test.createOrReplaceTempView('test')
    test_ranking = test.withColumn("rn", F.row_number().over(Window.partitionBy("userId").orderBy(F.col("rating").desc()))).filter(f"rn <= {100}").groupBy("userId").agg(F.collect_list(F.col("movieId")).alias("movie_list"))
    test_ranking.createOrReplaceTempView('test_ranking')

    validation = validation.drop("timestamp")
    validation.createOrReplaceTempView('validation')
    print("Testing on validation set")
    # tuning the hyperparameters by evaluating the model on validation dataset
    users_valid = validation.select("userId").distinct()
    predictions_valid = model.recommendForUserSubset(users_valid, 100)
    predictions_valid = predictions_valid.withColumn("recommendations", explode(col("recommendations"))).select("userId", "recommendations.movieId", "recommendations.rating")
    predicted_items_valid = predictions_valid.withColumn("rn", F.row_number().over(Window.partitionBy("userId").orderBy(F.col("rating").desc()))).groupBy("userId").agg(F.collect_list(F.col("movieId")).alias("predicted_movie_list"))
    predictions_valid = predictions_valid.rdd.map(lambda r: ((r.userId, r.movieId), r.rating)).toDF(["user_movie", "predictions_rating"])

    ratingsTuple_valid = validation.rdd.map(lambda r: ((r[0], r[1]), r[2])).toDF(["user_movie", "rating"])
    ratingsTuple_valid.createOrReplaceTempView('ratingsTuple_v')
    predictions_valid.createOrReplaceTempView('predictions_v')
    scoreAndLabels_als_valid = spark.sql('SELECT rating, predictions_rating FROM predictions_v INNER JOIN ratingsTuple_v ON ratingsTuple_v.user_movie = predictions_v.user_movie').na.drop()

    valid_top_user_movies = validation.withColumn("rn", F.row_number().over(Window.partitionBy("userId").orderBy(F.col("rating").desc()))).filter(f"rn <= {100}").groupBy("userId").agg(F.collect_list(F.col("movieId")).alias("movie_list"))

    valid_top_user_movies.createOrReplaceTempView('valid_top_user_movies')
    predicted_items_valid.createOrReplaceTempView('predicted_items_valid')
    rating_metrics_tuple_valid = spark.sql('SELECT movie_list, predicted_movie_list FROM valid_top_user_movies INNER JOIN predicted_items_valid ON predicted_items_valid.userId = valid_top_user_movies.userId').na.drop()

    # Instantiate ranking metrics object
    ranking_metrics_als_valid = RankingMetrics(rating_metrics_tuple_valid.rdd)

    print("ALS validation set Mean average precision at 100 = %s" % ranking_metrics_als_valid.meanAveragePrecisionAt(100))

    print("ALS validation set baseline NDCG at 100 = %s" % ranking_metrics_als_valid.ndcgAt(100))

    print("ALS validation set precision at 100 = %s" % ranking_metrics_als_valid.precisionAt(100))

    
    # Instantiate regression metrics to compare predicted and actual ratings
    #metrics_als_valid = RegressionMetrics(scoreAndLabels_als_valid.rdd)

    # Root mean squared error on validation set
    #print("ALS Validation set RMSE = %s" % metrics_als_valid.rootMeanSquaredError)

    print("Testing on test set")


    # Evaluate the model by computing the RMSE on the test data
    users = test.select("userId").distinct()
    predictions = model.recommendForUserSubset(users, 100)
    predictions = predictions.withColumn("recommendations", explode(col("recommendations"))).select("userId", "recommendations.movieId", "recommendations.rating")
    predicted_items_test = predictions.withColumn("rn", F.row_number().over(Window.partitionBy("userId").orderBy(F.col("rating").desc()))).groupBy("userId").agg(F.collect_list(F.col("movieId")).alias("predicted_movie_list"))
    predictions = predictions.rdd.map(lambda r: ((r.userId, r.movieId), r.rating)).toDF(["user_movie", "predictions_rating"])

    ratingsTuple = test.rdd.map(lambda r: ((r[0], r[1]), r[2])).toDF(["user_movie", "rating"])
    ratingsTuple.createOrReplaceTempView('ratingsTuple')
    predictions.createOrReplaceTempView('predictions')
    scoreAndLabels_als = spark.sql('SELECT rating, predictions_rating FROM predictions INNER JOIN ratingsTuple ON ratingsTuple.user_movie = predictions.user_movie').na.drop()

    predicted_items_test.createOrReplaceTempView('predicted_items_test')
    rating_metrics_tuple_test = spark.sql('SELECT movie_list, predicted_movie_list FROM test_ranking INNER JOIN predicted_items_test ON predicted_items_test.userId = test_ranking.userId').na.drop()
    # Instantiate ranking metrics object
    ranking_metrics_als_test = RankingMetrics(rating_metrics_tuple_test.rdd)

    print("ALS test set Mean average precision at 100 = %s" % ranking_metrics_als_test.meanAveragePrecisionAt(100))

    print("ALS test set baseline NDCG at 100 = %s" % ranking_metrics_als_test.ndcgAt(100))

    print("ALS validation set precision at 100 = %s" % ranking_metrics_als_test.precisionAt(100))

    # Instantiate regression metrics to compare predicted and actual ratings
    #metrics_als = RegressionMetrics(scoreAndLabels_als.rdd)

    # Root mean squared error
    #print("Test set RMSE = %s" % metrics_als.rootMeanSquaredError)



if __name__ == "__main__":
    spark = SparkSession.builder.appName('ALS Model Inference').getOrCreate()
    val = sys.argv[1]
    test = sys.argv[2]
    main(spark, val, test)
    spark.stop()
