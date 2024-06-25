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


def main(spark, filepath):
        ratings = spark.read.parquet(filepath, header=True, inferSchema=True)
        ratings = ratings.repartition("userId")
        

        user_sets = ratings.groupBy("userId").agg(collect_set("movieId").alias("movieId"), size(collect_set("movieId"))).filter(size("movieId") > 100)    
        print("User sets with all")
        user_sets.show()
        print("User sets with all count: ", user_sets.count())
        print("------------------------------------------------------------------------------------------------")
        # user_sets = user_sets.sample(False, 0.1)
        hashingTF = HashingTF(inputCol="movieId", outputCol="features", numFeatures=100)
        df_featurized = hashingTF.transform(user_sets)
        df_featurized.show()
        minHash = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
        model = minHash.fit(df_featurized)
        print(f"count of feautrized {df_featurized.count()}")
        df_featurized = df_featurized
        sorted_jaccard = model.approxSimilarityJoin(df_featurized, df_featurized, 0.9, distCol="JaccardDistance")
        sorted_jaccard.show()
        # sorted_jaccard = sorted_jaccard.orderBy('JaccardDistance',ascending=False)
        # sorted_jaccard.select("datasetA.userId", "datasetB.userId", "JaccardDistance").show(100)
        sorted_jaccard = sorted_jaccard.filter(col("datasetA.userId") < col("datasetB.userId")) \
                             .orderBy("JaccardDistance", ascending=False) \
                             .limit(100)
        # df_minhashed.unpersist()
        sorted_jaccard = sorted_jaccard.select(
    col("datasetA.userId").alias("userId1"),
    col("datasetB.userId").alias("userId2"),
    col("JaccardDistance")
)
        sorted_jaccard.show()
        sorted_jaccard.write.parquet("hdfs:/user/br2543_nyu_edu/capstone-project-g-69/ml-latest-small/sorted_jaccard.parquet")
        sorted_jaccard.explain()
        print(f"count of feautrized {sorted_jaccard.count()}")
        sorted_jaccard.show()


if __name__ == "__main__":
    spark = SparkSession.builder.appName('MovieLens Segmentation jaccard').getOrCreate()
    hashed_ratings_file = sys.argv[1]  # Ensure this path is correctly passed as a command line argument
    main(spark, hashed_ratings_file)