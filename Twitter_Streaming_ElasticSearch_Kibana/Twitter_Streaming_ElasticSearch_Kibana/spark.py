from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pycorenlp import StanfordCoreNLP
import json
import os
from elasticsearch import Elasticsearch
import datetime
os.environ['PYSPARK_SUBMIT_ARGS']  =  "--jars elasticsearch-hadoop-6.2.4.jar pyspark-shell"

TCP_IP = 'localhost'
TCP_PORT = 9006

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from spark context with interval size 2 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")
#read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)
#dataStream.pprint()

es = Elasticsearch()
es_write_conf = { "es.nodes" : 'localhost',"es.port" : '9200',"es.resource" : "twitter/tweet21","es.input.json" : "true"}
#mapping={"mappings":{"mytweet":{"properties":{"timestamp":{"type":"date","format":"yyyy-MM-dd HH:mm:ss.SSSSSS"},"coordinates":{"type":"geo_point"},"tweet":{"type": "text"},"sentiment":{"type":"text"}}}}}
#es.index(index="tweet17",doc_type="tweet17",body=mapping)
nlp = StanfordCoreNLP('http://localhost:9000')

def sentimentAnlys(text):
    res = nlp.annotate(text[1],
                   properties={
                       'annotators': 'sentiment',
                       'outputFormat': 'json',
                       
                   })
    
    for s in res["sentences"]:
        sentiment= s["sentiment"]
        data=[text[0],text[1],sentiment]
        print(data)
        return data
 
def formatChange(line):
    lat=line[0].split('@')[0]
    lon=line[0].split('@')[1]
    text=line[1]
    sentiment=line[2]
    ts = datetime.datetime.now()
    return (('timestamp',str(ts)),('coordinates',lat+","+lon),('tweet' , text) , ('sentiment' , sentiment))

sentimentData=dataStream.map(lambda w : w.split('#')).map(sentimentAnlys)
formatData=sentimentData.map(formatChange)
#formatData.pprint()
jsonData=formatData.map(lambda p : dict(p)).map(lambda x:json.dumps(x))
jsonData1=jsonData.map(lambda x:('key',x))
jsonData1.pprint()
jsonData1.foreachRDD(lambda x : x.saveAsNewAPIHadoopFile(
             path="-", 
             outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
             keyClass="org.apache.hadoop.io.NullWritable", 
             valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
             conf=es_write_conf))

ssc.start()
ssc.awaitTermination()

