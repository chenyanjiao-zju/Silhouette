from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
import time
import sys

partition = float(sys.argv[1])
num_parts = int(sys.argv[2])
input_file = sys.argv[3]

if __name__ == "__main__":
    spark = SparkSession.builder.appName("LogisticRegressionWithElasticNet").getOrCreate()

    # $example on$
    # Load training data
    # s3://ernest-data/rcv1_test_256.binary
    # training = spark.read.parquet(input_file)
    training = spark.read.format("libsvm").load(input_file)
    # training.write.save("a.parquet", format="parquet")
    # training = training.where("label > 0")
    #     "label",
    #     when(
    #         col("label") <= 0,
    #         0.5
    #     ).when(
    #         col("label") >= 1,
    #         0.9
    #     ).otherwise(col("label"))
    # )
    training = training.sample(False, partition).coalesce(num_parts)
    training.cache().count()
    lr = LogisticRegression(maxIter=10, elasticNetParam=0.8)

    # Fit the model
    start = time.time()
    lrModel = lr.fit(training)
    end = time.time()
    run_time = end - start
    print("time: " + str(run_time))
    print("====================================")
    print("LR sample: ", partition, " took ", run_time)
    print("====================================")


