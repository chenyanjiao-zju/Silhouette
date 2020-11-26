#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
An example for computing correlation matrix.
Run with:
  bin/spark-submit examples/src/main/python/ml/correlation_example.py
"""
from __future__ import print_function

# $example on$
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
# $example off$
from pyspark.sql import SparkSession
import time
import sys


partition = float(sys.argv[1])
num_parts = int(sys.argv[2])
input_file = sys.argv[3]

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("CorrelationExample") \
        .getOrCreate()

    # $example on$
    df = spark.read.format("libsvm").load(input_file)

    df = df.sample(False, partition).coalesce(num_parts)

    start = time.time()
    r1 = Correlation.corr(df, "features").head()
    end = time.time()
    run_time = end - start
    print("time: " + str(run_time))
    print("Pearson correlation matrix:\n" + str(r1[0]))


    # $example off$

    spark.stop()
