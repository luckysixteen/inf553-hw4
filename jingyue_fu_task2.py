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
rawData = rawData.filter(lambda x: x != header).map(lambda x: x.decode().split(','))
rawData = rawData.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).sortByKey()
allPairs = rawData.cartesian(rawData).filter(lambda x: x[0][0] != x[1][0])
edgesData = allPairs.map(lambda x: ((x[0][0],[x[1][0]]),set(x[0][1]).intersection(set(x[1][1])))).filter(lambda x: len(x[1]) >= THRESHOLD ).map(lambda x: x[0])
verticesData = edgesData.flatMap(lambda x: [x[0], x[1][0]]).distinct()
# graphDict: Dictionary  {node: [neighbor1, neighbor2]}
graphDict = edgesData.map(lambda x: (x[0], set(x[1]))).reduceByKey(lambda x,y: x | y).collectAsMap()


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
                edge = tuple(sorted([node, parent], key=lambda x: str.lower(x)))
                yield(edge, between / 2)


betweenness = verticesData.flatMap(lambda x: GirvanNewman(x)).reduceByKey(add)
betweennessList = betweenness.collect()
betweennessList = sorted(sorted(betweennessList, key=lambda x: str.lower(x[0][0])), key=lambda x: x[1], reverse = True)

betweennessText = open(OUTPUT1, 'w')
if betweennessList:
    outputStr = ""
    for b in betweennessList:
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
edgeNum = betweenness.count()
nodeNum = verticesData.count()
nodeList = verticesData.collect()
modularity = 0
numDict = dict()
for node in graphDict:
    numDict[node] = len(graphDict[node])

modulDict = dict()
for nodei in nodeList:
    for nodej in nodeList:
        if nodej in graphDict[nodei]:
            A = 1
        else:
            A = 0
        q = (A - 0.5 * numDict[nodei] * numDict[nodej] / edgeNum) / (2 * edgeNum)
        modulDict[(nodei, nodej)] = q
        modularity += q


def CalculateModularity(community):
    m = 0
    for i in community:
        for j in community:
            m += modulDict[(i, j)]
    return m


def dfs_connected(i, j, traverse):
    traverse.add(i)
    children = graphDict[i]
    for child in children:
        if child == j:
            return True
        if child not in traverse:
            if dfs_connected(child, j, traverse):
                return True
    return False


def dfs(node, community):
    community.add(node)
    children = graphDict[node]
    for child in children:
        if child not in community:
            dfs(child, community)


def getCommunity():
    vertices = set(nodeList)
    communities = list()
    while len(vertices) != 0:
        head = vertices.pop()
        community = set()
        dfs(head, community)
        communities.append(list(community))
        vertices = vertices - community
    return communities

bestModu = (modularity, getCommunity())

communityDict = dict()
for edge in betweennessList:
    i = edge[0][0]
    j = edge[0][1]
    graphDict[i].remove(j)
    graphDict[j].remove(i)
    modu = 0
    if dfs_connected(i, j, set([])):
        continue
    communities = getCommunity()
    for community in communities:
        modu += CalculateModularity(community)
    if bestModu[0] < modu:
        bestModu = (modu, communities)

# Sort and Print
sortedCommunity = list()
for c in bestModu[1]:
    c = sorted(c, key=lambda x: str.lower(x))
    sortedCommunity.append(c)
sortedResult = sorted(sorted(sortedCommunity, key = lambda x: str.lower(x[0])), key=lambda x: len(x))

fileOfOutput = open(OUTPUT2, 'w')
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
fileOfOutput.close()

timeEnd = time.time()
print ("Duration: %f sec" % (timeEnd - timeStart))

# bin/spark-submit \
# --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.xml" \
# --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.xml" \
# ../inf553-hw4/jingyue_fu_task2.py 7 ../inf553-hw4/input/ub_sample_data.csv ../inf553-hw4/output/jingyue_fu_task2_betweenness.txt ../inf553-hw4/output/jingyue_fu_task2_community.txt