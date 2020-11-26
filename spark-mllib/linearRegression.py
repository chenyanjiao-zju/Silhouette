from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
import time
import sys

partition = float(sys.argv[1])
num_parts = int(sys.argv[2])
input_file = sys.argv[3]

if __name__ == "__main__":
    spark = SparkSession.builder.appName("LinearRegressionWithElasticNet").getOrCreate()

    # $example on$
    # Load training data
    # s3://ernest-data/rcv1_test_256.binary
    training = spark.read.format("libsvm").load(input_file)
    training = training.sample(False, partition).coalesce(num_parts)
    training.cache().count()
    lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Fit the model
    start = time.time()
    lrModel = lr.fit(training)
    end = time.time()
    run_time = end - start
    print("time: " + str(run_time))
    print("====================================")
    print("LR sample: ", partition, " took ", run_time)
    print("====================================")


