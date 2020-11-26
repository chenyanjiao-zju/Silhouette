# Author: Xu Wang (wangxu298@whu.edu.cn)
# A sample demo for running the experiment

# global
master_url="spark://ec2-18-208-187-96.compute-1.amazonaws.com:7077"
enable_s3="--packages org.apache.hadoop:hadoop-aws:2.8.5"
dataset="/input/regression.binary"
algo_path="linearRegression.py"
spark_submit_binary="/home/ec2-user/spark/bin/spark-submit"

function run_step {
  mcs=$1
  shift
  scale=$1
  shift
  num_parts=$1
  shift
  spark_script=$1
  echo -n "Cores $mcs"
  ${spark_submit_binary} --master ${master_url} --total-executor-cores $mcs ${spark_script} $scale ${num_parts} ${dataset} 2>&1 | grep "time:" >> "./result/${mcs}-${scale}-${spark_script}"
}

for i in `seq 1 3`
do
        run_step 16 0.069271 256 ${algo_path}
        run_step 12 0.047135 256 ${algo_path}
        run_step 16 0.064844 256 ${algo_path}
        run_step 10 0.038281 256 ${algo_path}
        run_step 2 0.011719 256 ${algo_path}
        run_step 4 0.016146 256 ${algo_path}
        run_step 2 0.029427 256 ${algo_path}
        run_step 4 0.020573 256 ${algo_path}
        run_step 8 0.029427 256 ${algo_path}
        run_step 10 0.042708 256 ${algo_path}
        run_step 16 0.073698 256 ${algo_path}
        run_step 2 0.078125 256 ${algo_path}
        run_step 4 0.078125 256 ${algo_path}

        run_step 8 1 256 ${algo_path}
        run_step 16 1 256 ${algo_path}
        run_step 32 1 256 ${algo_path}
        run_step 64 1 256 ${algo_path}
        run_step 128 1 256 ${algo_path}
done
