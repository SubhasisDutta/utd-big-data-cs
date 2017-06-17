from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from TwitterSentiment import TwitterSentiment
from elasticsearch import Elasticsearch
import json
import time
import datetime

def getResponseforES(input, response):
    res = {}
    res['id'] = input[0]
    res['location'] = input[1]
    lat_long = input[2].replace("(","").replace(")","").split(",")
    res['latitude'] = float(lat_long[0])
    res['longitude'] = float(lat_long[1])
    res['original_tweet'] = input[3]
    res['sentimtnt_tweet'] = response[0]['text']
    res['label'] = response[0]['label']
    res['hash_tag'] = input[5]
    res['created_time'] = time.mktime(datetime.datetime.strptime(input[4], "%a %b %d %H:%M:%S +0000 %Y").timetuple())
    # send results to elastic search
    es_obj = {'latitude':res['latitude'],
              'longitude':res['longitude'],
              'lavel':res['label'],
              'hash_tag':res['hash_tag'],
              'created_time':res['created_time'],
              '_id':res['id'],
              'tweet':res['original_tweet']}

    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    #es.index(index='tweetfeed-',doc_type='tweet', id=res['id'], body= es_obj)
    return es_obj

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingKafkaConsumer")
    ts = TwitterSentiment()

    ssc = StreamingContext(sc, 1)

    topic = sys.argv[1]
    kvs = KafkaUtils.createStream(ssc, "localhost:2181", "spark-streaming-consumer", {topic: 1})
    results = kvs.map(lambda x: x[1]).map(lambda record: record.split("#$#"))\
        .map(lambda fields: getResponseforES(fields,ts.classifyTweet(fields[3],'naivebayes')))

    results.pprint()



    ssc.start()
    ssc.awaitTermination()