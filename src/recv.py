#!/usr/bin/python

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import json
import pprint
import re

import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import sys

def alert(x):
	# email alert function
	recipients = ['example@example.com']
	emaillist = [elem.strip().split(',') for elem in recipients]
	msg = MIMEMultipart()
	msg['Subject'] = "Hey!! Somthing wrong with your withdrawal"
	msg['From'] = 'example@example.com'
	msg['Reply-to'] = 'example@example.com'
	msg.preamble = 'Multipart massage.\n'
	part = MIMEText("We found some strange transaction from your account\n\nuserid: " + str(x[0]) + "\namount: " + str(x[1]) + " THB\n location: " + str(x[2]) + "\n action: " + str(x[3]) + "\nid: " + str(x[4]) + "\nchannel: " + str(x[5]))
	msg.attach(part)

	server = smtplib.SMTP("smtp.gmail.com:587")
	server.ehlo()
	server.starttls()
	server.login("example@example.com", "password")

	server.sendmail(msg['From'], emaillist, msg.as_string())

	return x

conf = SparkConf().setAppName("wordCount").setMaster("local[1]")
sc = SparkContext(conf=conf)
fraudFile = sc.textFile("hdfs://localhost/user/training/sparks1out/*")
fraudData = fraudFile.map(lambda x: re.sub("[\(u' )]","", x)).map(lambda x: x.split(",")).map(lambda x: (x[0],x[1])).collectAsMap()
sc.stop()

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("localhost", 3222)


counts = lines.map(lambda x: json.loads(x)).map(lambda x: (x["userid"],x["amount"],x["location"],x["action"],x["id"],x["channel"])).filter(lambda x: x[1] >= float(fraudData[x[0]])).map(lambda x: alert(x))
counts.pprint()

ssc.start()
ssc.awaitTermination()


