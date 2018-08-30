#!/usr/bin/python

from pyspark import SparkContext, SparkConf
import math
conf = SparkConf().setAppName("wordCount").setMaster("local[1]")

sc = SparkContext(conf=conf)

wordSet = sc.textFile("hdfs://localhost/user/training/csv/*.csv")
wordCount = wordSet.map(lambda line: line.split(",")).map(lambda word:(word[3],float(word[5])))
Num = wordCount.countByKey()
Sum = wordCount.reduceByKey(lambda a,b:a+b)
mean = Sum.map(lambda word: (word[0],(word[1]/Num[word[0]])))
meansq = Sum.map(lambda word: (word[0],(word[1]/Num[word[0]])**2))
Sumsq = wordCount.map(lambda word: (word[0],word[1]*word[1])).reduceByKey(lambda a,b:a+b).map(lambda word: (word[0],word[1]/Num[word[0]]))
SD = Sumsq.join(meansq).map(lambda word: (word[0], math.sqrt(word[1][0]-word[1][1])))
FraudCase = mean.join(SD).map(lambda word : (word[0],word[1][0]+(2*word[1][1]))).sortByKey(True)

# sd = sqrt(sumsq/n - meansq)

FraudCase.collect()
FraudCase.saveAsTextFile("hdfs://localhost/user/training/sparks1out")


