from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark import sql
from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *
import os, sys
import time
from operator import add

timeStart = time.time()

THRESHOLD = int(sys.argv[1])
INPUT_CSV = sys.argv[2]
OUTPUT = sys.argv[3]

# Data Process: Creat baskets
sc = SparkContext('local[*]', 'CD_GF')
sqlContext = sql.SQLContext(sc)
rawData = sc.textFile(INPUT_CSV, None, False)
header = rawData.first()
rawData = rawData.filter(lambda x: x != header).map(lambda x: x.split(','))
rawData = rawData.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x+y).sortByKey()
allPairs = rawData.cartesian(rawData).filter(lambda x: x[0][0] != x[1][0])
edgesData = allPairs.map(lambda x: ((x[0][0],x[1][0]),set(x[0][1]).intersection(set(x[1][1])))).filter(lambda x: len(x[1]) >= THRESHOLD ).map(lambda x: x[0])
verticesData = edgesData.flatMap(lambda x: [x[0], x[1]]).distinct().map(lambda x: [x])

# Build GraphFrames
vertices = verticesData.toDF(["id"])
edges = edgesData.toDF(["src", "dst"])
g = GraphFrame(vertices, edges)

# LPA find Community
result = g.labelPropagation(maxIter=5)
sortedResult = result.select('label', 'id').rdd.map(lambda x: (x[0], [str(x[1])])).reduceByKey(lambda x, y: x+y).map(lambda x: sorted(x[1], key=str.lower)).collect()
sortedResult = sorted(
    sorted(sortedResult, key = lambda x: x[0].lower), key=lambda x: len(x), reverse=False)

# Sort and Print
fileOfOutput = open(OUTPUT, 'w')
outputStr = ""
for usr in sortedResult[0]:
    outputStr += "'"
    outputStr += usr
    outputStr += "', "
outputStr = outputStr[:-2]
for i in range(1, len(sortedResult)):
    outputStr += '\n'
    for usr in sortedResult[i]:
        outputStr += "'"
        outputStr += usr
        outputStr += "', "
    outputStr = outputStr[:-2]
fileOfOutput.write(outputStr)

timeEnd = time.time()
print "Duration: %f sec" % (timeEnd - timeStart)

# bin/spark-submit \
# --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.xml" \
# --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.xml" \
# --packages graphframes:graphframes:0.4.0-spark2.1-s_2.11 \
# ../inf553-hw4/jingyue_fu_task1.py 7 ../inf553-hw4/input/ub_sample_data.csv ../inf553-hw4/output/jingyue_fu_task1.txt
