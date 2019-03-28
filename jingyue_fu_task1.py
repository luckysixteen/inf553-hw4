from pyspark import SparkContext
from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *
import os, sys
import time
from operator import add

timeStart = time.time()

THRESHOLD = sys.argv[1]
INPUT_CSV = sys.argv[2]
OUTPUT = sys.argv[3]

# Data Process: Creat baskets
sc = SparkContext('local[*]', 'CD_GF')
rawData = sc.textFile(INPUT_CSV, None, False)
header = rawData.first()
rawData = rawData.filter(lambda x: x != header).map(lambda x: x.split(','))
rateData = rawData.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x,y: x+y).sortByKey()
print rateData.take(10)


timeEnd = time.time()
print "Duration: %f sec" % (timeEnd - timeStart)

# bin/spark-submit \
# --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.xml" \
# --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.xml" \
# --packages graphframes:graphframes:0.4.0-spark2.1-s_2.11 \
# ../inf553-hw4/jingyue_fu_task1.py 7 ../inf553-hw4/input/ub_sample_data.csv ../inf553-hw3/output/jingyue_fu_task1.txt
