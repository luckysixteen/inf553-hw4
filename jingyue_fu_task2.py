from pyspark import SparkContext
import os, sys
import time
from operator import add

timeStart = time.time()

THRESHOLD = sys.argv[1]
INPUT_CSV = sys.argv[2]
OUTPUT1 = sys.argv[3]
OUTPUT2 = sys.argv[4]

# Data Process: Creat baskets
sc = SparkContext('local[*]', 'CD_GF')
rawData = sc.textFile(INPUT_CSV, None, False)
header = rawData.first()
rawData = rawData.filter(lambda x: x != header).map(lambda x: x.split(','))
rateData = rawData.map(lambda x: (abs(hash(x[0])) % (10**9), [x[1]])
                       ).reduceByKey(lambda x, y: x + y).sortByKey()
# print rateData.take(10)

timeEnd = time.time()
print "Duration: %f sec" % (timeEnd - timeStart)

# bin/spark-submit \
# --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.xml" \
# --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.xml" \
# ../inf553-hw4/jingyue_fu_task2.py ../inf553-hw4/input/ub_sample_data.csv ../inf553-hw3/output/jingyue_fu_task2_betweeness.txt ../inf553-hw3/output/jingyue_fu_task2_community.txt