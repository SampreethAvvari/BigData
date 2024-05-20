import os
import sys


from pyspark.ml.linalg import Vectors

from pyspark.sql.functions import col
from pyspark.sql.functions import collect_set
from pyspark.ml.feature import MinHashLSH
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.sql.functions import udf, expr
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import col, collect_set, max as max_ , size
from pyspark.ml.feature import HashingTF
from pyspark.sql import functions as F
from pyspark.mllib.evaluation import RankingMetrics
from pyspark.mllib.evaluation import RankingMetrics
from pyspark.sql.functions import lit, array







def main(spark, train, val, test, movie, rating):
    """
    Main routine for processing MovieLens dataset.

    Parameters
    ----------
    spark : SparkSession object
    ratings_file : str, path to the ratings CSV file
    """

    training=spark.read.parquet(train, header=True, inferSchema=True)
    validation=spark.read.parquet(val, header=True, inferSchema=True)
    testing=spark.read.parquet(test, header=True, inferSchema=True)
    movies=spark.read.parquet(movie, header=True, inferSchema=True)
    ratings=spark.read.parquet(rating, header=True, inferSchema=True)

    beta=10000
    # Calculate movie popularity in the training set
    movie_popularity_train = training.groupBy("movieId").agg(
    F.sum("rating").alias("sum_ratings"),
    F.count("rating").alias("num_ratings")
    ).withColumn("P_i", F.col("sum_ratings") / (F.col("num_ratings") + beta))

    # Select the top 100 movies
    top_100_movies = movie_popularity_train.orderBy(F.desc("P_i")).limit(100)

    top_movies_with_titles = movie_popularity_train.join(movies, "movieId").select("movieId", "title").orderBy(F.desc("P_i")).limit(10)

    # Collect the top movies into a list within the driver program
    top_movies_list = top_movies_with_titles.collect()

    # Extract lists of movie IDs and titles
    movie_ids = [row['movieId'] for row in top_movies_list]
    movie_titles = [row['title'] for row in top_movies_list]


    # Get distinct users
    distinct_users = ratings.select("userId").distinct()

    # Create a DataFrame with repeated lists (broadcasted lists)
    

    # Convert lists to literals that can be added as new columns
    movie_ids_literal = array([lit(x) for x in movie_ids])
    movie_titles_literal = array([lit(x) for x in movie_titles])

    # Add these as columns to the distinct_users DataFrame
    user_recommendations = distinct_users.withColumn("movieIDs_recommended", movie_ids_literal).withColumn("titles_of_movies_recommended", movie_titles_literal)

    # Show the DataFrame
    user_recommendations.show()

    # Prepare the predictions to match the expected format of RankingMetrics
    # Here, we assume every user gets the same top 100 movies
    predicted_ranking = training.select("userId").distinct()\
        .withColumn("recommendations", F.array([lit(row.movieId) for row in top_100_movies.collect()]))

    # Prepare the ground truth data
    # Group the validation data by user and collect all the movies they've rated into a list
    ground_truth = validation.groupBy("userId").agg(
        F.collect_list("movieId").alias("truth")
    )

    # Join predictions with ground truth
    prediction_and_labels = predicted_ranking.join(ground_truth, "userId", "inner")\
        .rdd.map(lambda row: (row.recommendations, row.truth))

    # Compute ranking metrics
    metrics = RankingMetrics(prediction_and_labels)

    # Print evaluation metrics
    print("Mean Average Precision (MAP) for validation:", metrics.meanAveragePrecision)
    print("Precision at 100 for validation:", metrics.precisionAt(100))
    print("NDCG at 100 for validation:", metrics.ndcgAt(100))

    # Additional metrics
    # Since RankingMetrics does not directly provide all of these metrics, we may need custom computations

    # Mean Reciprocal Rank (MRR)
    reciprocal_rank = prediction_and_labels.map(lambda x: 1.0 / (1 + x[0].index(x[1][0])) if x[1][0] in x[0] else 0.0)
    print("Mean Reciprocal Rank (MRR) for validation:", reciprocal_rank.mean())



    # Average Precision
    # Average Precision can be computed similarly to MAP, though not directly available in Spark's RankingMetrics
    average_precision = metrics.meanAveragePrecision
    print("Average Precision for validation:", average_precision)


    #metrics for test data
    predicted_ranking_test = training.select("userId").distinct()\
        .withColumn("recommendations", F.array([lit(row.movieId) for row in top_100_movies.collect()]))

    # Prepare the ground truth data
    # Group the validation data by user and collect all the movies they've rated into a list
    ground_truth = testing.groupBy("userId").agg(
        F.collect_list("movieId").alias("truth")
    )

    # Join predictions with ground truth
    prediction_and_labels_test = predicted_ranking_test.join(ground_truth, "userId", "inner")\
        .rdd.map(lambda row: (row.recommendations, row.truth))

    # Compute ranking metrics
    metrics_test = RankingMetrics(prediction_and_labels_test)

    # Print evaluation metrics
    print("Mean Average Precision (MAP) for test:", metrics_test.meanAveragePrecision)
    print("Precision at 100 for test:", metrics_test.precisionAt(100))
    print("NDCG at 100 for test:", metrics_test.ndcgAt(100))

    # Additional metrics
    # Since RankingMetrics does not directly provide all of these metrics, we may need custom computations

    # Mean Reciprocal Rank (MRR)
    reciprocal_rank = prediction_and_labels_test.map(lambda x: 1.0 / (1 + x[0].index(x[1][0])) if x[1][0] in x[0] else 0.0)
    print("Mean Reciprocal Rank (MRR) for test:", reciprocal_rank.mean())



    # Average Precision
    # Average Precision can be computed similarly to MAP, though not directly available in Spark's RankingMetrics
    average_precision = metrics_test.meanAveragePrecision
    print("Average Precision for test:", average_precision)






   
if __name__ == "__main__":
    spark = SparkSession.builder.appName('Popularity Basline').getOrCreate()
    train = sys.argv[1]
    val=sys.argv[2]
    test=sys.argv[3]
    movie=sys.argv[4]
    rating=sys.argv[5]
      
    main(spark, train, val, test, movie, rating)