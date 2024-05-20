import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, col, rank
from pyspark.ml.recommendation import ALS
from pyspark.mllib.evaluation import RankingMetrics
from pyspark.sql import Window
from pyspark.ml.evaluation import RegressionEvaluator, RankingEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import pyspark.sql.functions as func

def main(spark, val, test):
    """
    Main routine for processing MovieLens dataset using validation and test sets.

    Parameters
    ----------
    spark : SparkSession object
    val : str, path to the validation data parquet file
    test : str, path to the test data parquet file

    """
    # Reading parquet files
    ratingsTrain = spark.read.parquet(val, inferSchema=True)
    ratingsTest = spark.read.parquet(test)
    ratingsTrain = ratingsTrain.withColumn('userId', col('userId').cast('integer')).withColumn('movieId', col('movieId').cast('integer')).withColumn('rating', col('rating').cast('float')).drop('timestamp')

    # Hyperparameters
    hyper_param_reg = [0.01, 0.1, 1]
    hyper_param_rank = [10, 50, 100, 200]
    best_metric = float('inf')
    best_params = {'regParam': None, 'rank': None}
    als = ALS(maxIter=5, userCol="userId", itemCol="movieId", ratingCol="rating", nonnegative=True, implicitPrefs=False, coldStartStrategy="drop")
    param_grid = ParamGridBuilder().addGrid(als.rank, hyper_param_rank).addGrid(als.regParam, hyper_param_reg).build()
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

    all_metrics = []

    for params in param_grid:
        model = als.fit(ratingsTrain, params)
        test_pred = model.transform(ratingsTest)
        rmse = evaluator.evaluate(test_pred)

        window = Window.partitionBy(test_pred['userId']).orderBy(test_pred['prediction'].desc())
        test_pred = test_pred.withColumn('rank', rank().over(window)).filter(col('rank') <= 100).groupby("userId").agg(func.collect_list(test_pred['movieId'].cast('double')).alias('pred_movies'))

        window = Window.partitionBy(ratingsTest['userId']).orderBy(ratingsTest['rating'].desc())
        df_mov = ratingsTest.withColumn('rank', rank().over(window)).filter(col('rank') <= 100).groupby("userId").agg(func.collect_list(ratingsTest['movieId'].cast('double')).alias('movies'))

        test_pred = test_pred.join(df_mov, test_pred.userId == df_mov.userId).drop('userId')

        metrics = {
            'rank': params[als.rank],
            'regParam': params[als.regParam],
            'rmse': rmse,
            'meanAveragePrecision': RankingEvaluator(predictionCol='pred_movies', labelCol='movies', metricName='meanAveragePrecision').evaluate(test_pred),
            'meanAveragePrecisionAtK': RankingEvaluator(predictionCol='pred_movies', labelCol='movies', metricName='meanAveragePrecisionAtK').evaluate(test_pred),
            'precisionAtK': RankingEvaluator(predictionCol='pred_movies', labelCol='movies', metricName='precisionAtK').evaluate(test_pred),
            'ndcgAtK': RankingEvaluator(predictionCol='pred_movies', labelCol='movies', metricName='ndcgAtK').evaluate(test_pred),
            'recallAtK': RankingEvaluator(predictionCol='pred_movies', labelCol='movies', metricName='recallAtK').evaluate(test_pred)
        }

        all_metrics.append(metrics)

        if rmse < best_metric:
            best_metric = rmse
            best_params = params
            best_model_metrics = metrics

    for metrics in all_metrics:
        print(f"Model - Rank: {metrics['rank']}, RegParam: {metrics['regParam']}, RMSE: {metrics['rmse']}, MAP: {metrics['meanAveragePrecision']}, MAP@K: {metrics['meanAveragePrecisionAtK']}, Precision@K: {metrics['precisionAtK']}, NDCG@K: {metrics['ndcgAtK']}, Recall@K: {metrics['recallAtK']}")

    print("Best Model - Rank:", best_model_metrics['rank'], " RegParam:", best_model_metrics['regParam'])
    print("Best Model Metrics:", best_model_metrics)


if __name__ == "__main__":
    spark = SparkSession.builder.appName('ALS Model Evaluation').getOrCreate()
    val = sys.argv[1]
    test = sys.argv[2]
    main(spark, val, test)
