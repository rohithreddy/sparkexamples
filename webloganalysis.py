# imports

import os,findspark
os.environ['SPARK_HOME'] = '/home/rohith/work/spark-1.6.1-bin-without-hadoop'
findspark.init()
import flask

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
import re

import sys
print ("version:",sys.version)


APP_NAME = "Logs Spark"
conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster("local[*]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


logFiles = sc.textFile("file:/var/log/nginx/access.log")
NGINX_LOGPATT = '''^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\]  "(\S+) (\S+) (\S+)" (\S+) (\S+) "(\S+)" "(\w+\/\S+ \S+ \S+ \S+ \S+ \S+ \S+)" "(\S+)"'''

def parse_nginx_log(logline):
    match = re.search(NGINX_LOGPATT, logline)
    if match is None:
        raise Exception("Invalid logline : %s" % logline)
    return Row(
        ip_address = match.group(1),
        remote_user = match.group(3),
        time_stamp = match.group(4),
        request_type = match.group(5),
        end_point = match.group(6),
        protocol = match.group(7),
        http_status_code = match.group(8),
        content_size = int(match.group(9)),
        referer = match.group(10),
        user_agent = match.group(11),
        cookie_user = match.group(12)
    )
access_logs = logFiles.map(parse_nginx_log).cache()
content_sizes = access_logs.map(lambda log: log.content_size).cache()
print "Content Size Avg: %i, Min: %i, Max: %s" % (
    content_sizes.reduce(lambda a, b : a + b) / content_sizes.count(),
    content_sizes.min(),
    content_sizes.max()
    )

# Response Code to Count
responseCodeToCount = (access_logs.map(lambda log: (log.http_status_code, 1))
                       .reduceByKey(lambda a, b : a + b)
                       .take(100))
print "Response Code Counts: %s" % (responseCodeToCount)

# Any IPAddress that has accessed the server more than 10 times.
ipAddresses = (access_logs
               .map(lambda log: (log.ip_address, 1))
               .reduceByKey(lambda a, b : a + b)
               .filter(lambda s: s[1] > 10)
               .map(lambda s: s[0])
               .take(100))
print "IpAddresses that have accessed more then 10 times: %s" % (ipAddresses)

# Top Endpoints
topEndpoints = (access_logs
                .map(lambda log: (log.end_point, 1))
                .reduceByKey(lambda a, b : a + b)
                .takeOrdered(10, lambda s: -1 * s[1]))
print "Top Endpoints: %s" % (topEndpoints)
