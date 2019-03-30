from pyspark import SparkContext
import os, sys
import time
from operator import add

timeStart = time.time()

THRESHOLD = int(sys.argv[1])
INPUT_CSV = sys.argv[2]
OUTPUT1 = sys.argv[3]
OUTPUT2 = sys.argv[4]

# Data Process: Creat baskets
sc = SparkContext('local[*]', 'CD_GF')
rawData = sc.textFile(INPUT_CSV, None, False)
header = rawData.first()
rawData = rawData.filter(lambda x: x != header).map(lambda x: x.split(','))
rawData = rawData.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).sortByKey()
allPairs = rawData.cartesian(rawData).filter(lambda x: x[0][0] != x[1][0])
edgesData = allPairs.map(lambda x: ((x[0][0],[x[1][0]]),set(x[0][1]).intersection(set(x[1][1])))).filter(lambda x: len(x[1]) >= THRESHOLD ).map(lambda x: x[0])
verticesData = edgesData.flatMap(lambda x: [x[0], x[1][0]]).distinct()
graphDict = edgesData.reduceByKey(lambda x,y: x+y).collectAsMap()

# Task 2: Calculate Betweenness
def GirvanNewman(root):
    # Step 1: BFS from root
    traversalQueue = [root]
    record = set(traversalQueue)
    levelDict = {root: 0}
    parentDict = dict()
    inx = 0
    while inx < len(traversalQueue):
        head = traversalQueue[inx]
        children = graphDict[head]
        for child in children:
            if child not in record:
                traversalQueue.append(child)
                record.add(child)
                levelDict[child] = levelDict[head] + 1
                parentDict[child] = [head]
            else:
                if levelDict[child] == levelDict[head] + 1:
                    parentDict[child].append(head)
        inx += 1

    # Step 2: Calculate betweenness
    nodeCreditDict = dict()
    for i in range(len(traversalQueue) - 1, -1, -1):
        node = traversalQueue[i]
        if node not in nodeCreditDict:
            nodeCreditDict[node] = 1
        if node != root:
            parents = parentDict[node]
            parentNum = len(parents)
            for parent in parents:
                between = float(nodeCreditDict[node]) / parentNum
                if parent not in nodeCreditDict:
                    nodeCreditDict[parent] = 1
                nodeCreditDict[parent] += between
                edge = tuple(sorted([node, parent]))
                yield(edge, between / 2)


betweenness = verticesData.flatMap(lambda x: GirvanNewman(x)).reduceByKey(add).collect()
betweenness = sorted(sorted(betweenness, key=lambda x: x[0][0]), key=lambda x: x[1], reverse = True)
# print betweenness

betweennessText = open(OUTPUT1, 'w')
if betweenness:
    outputStr = ""
    for b in betweenness:
        outputStr += "('"
        outputStr += b[0][0]
        outputStr += "', '"
        outputStr += b[0][1]
        outputStr += "'), "
        outputStr += str(b[1])
        outputStr += "\n"
    outputStr = outputStr[:-1]
    betweennessText.write(outputStr)
betweennessText.close()


# Task 3: Community Detection

timeEnd = time.time()
print "Duration: %f sec" % (timeEnd - timeStart)

# bin/spark-submit \
# --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.xml" \
# --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.xml" \
# ../inf553-hw4/jingyue_fu_task2.py 7 ../inf553-hw4/input/ub_sample_data.csv ../inf553-hw4/output/jingyue_fu_task2_betweenness.txt ../inf553-hw4/output/jingyue_fu_task2_community.txt