#saprk Streaming

import org.apache.spark.streaming._

import org.apache.spark.SparkConf

sc.stop

val conf = new SparkConf().setAppName("Streaming").setMaster("yarn-client")

val ssc = new StreamingContext(conf, Seconds(10))


#Striming word count
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

conf = SparkConf(). \
setAppName("streaming word count"). \
setMaster("yarn-client")

sc = SparkContext(conf=conf)

ssc = StreamingContext(sc, 15)

lines = ssc.socketTextStream("gw03.itversity.com", 29999)
words = lines.flatMap(lambda l: l.split())
words_tuple = words.map(lambda w: (w, 1))
word_count = words_tuple.reduceByKey(lambda x, y : x + y)

word_count.pprint()

ssc.start()
ssc.awaitTermination()

spark-submit  --master yarn \
--conf spark.ui.port=12890 \
src/main/python/streaming_word_count.py

#string department count
# src/main/python/streaming_department_count.py
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from operator import add
import sys

host_name = sys.argv[1]
port      = int(sys.argv[2])

conf = SparkConf(). \
setAppName("streaming_department_count"). \
setMaster("yarn-client")

sc = SparkContext(conf= conf)
ssc = StreamingContext(sc, 30)

messages = ssc.socketTextStream(host_name, port)

department_msg = messages. \
filter(lambda msg: msg.split(" ")[6].split("/")[1] == "department")

department_names = department_msg. \
map(lambda msg: (msg.split(" ")[6].split("/")[2], 1))

department_conut = department_names. \
reduceByKey(add)

output_prefix = sys.argv[3]
department_conut.saveAsTextFiles(output_prefix)

ssc.start()
ssc.awaitTermination()

spark-submit  --master yarn \
--conf spark.ui.port=12890 \
src/main/python/streaming_department_count.py \


#streaming department count wiht spark streaming and flume 
# src/main/python/streaming_flume_department_count.py
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

from operator import add
import sys

host_name = sys.argv[1]
port      = int(sys.argv[2])

conf = SparkConf(). \
setAppName("streaming_department_count"). \
setMaster("yarn-client")

sc = SparkContext(conf= conf)
ssc = StreamingContext(sc, 30)

agents = [(host_name, port)]
polling_stream = FlumeUtils.createPollingStream(ssc, agents)
messages = polling_stream.map(lambda msg: msg[1])

department_msg = messages. \
filter(lambda msg: msg.split(" ")[6].split("/")[1] == "department")

department_names = department_msg. \
map(lambda msg: (msg.split(" ")[6].split("/")[2], 1))

department_conut = department_names. \
reduceByKey(add)

output_prefix = sys.argv[3]
department_conut.saveAsTextFiles(output_prefix)

ssc.start()
ssc.awaitTermination()

spark-submit  --master yarn \
--conf spark.ui.port=12890 \
--jars "/usr/hdp/2.5.0.0-1245/spark/lib/spark-streaming-flume_2.10-1.6.2.jar,/usr/hdp/2.5.0.0-1245/spark/lib/spark-streaming-flume-sink_2.10-1.6.2.jar,/usr/hdp/2.5.0.0-1245/flume/lib/flume-ng-sdk-1.5.2.2.5.0.0-1245.jar"  \
src/main/python/streaming_flume_department_count.py \
gw03.itversity.com 19999 /user/selvamsandeep/streaming_flume_department_count/cnt



#streaming department count wiht spark streaming and kafka integration
# src/main/python/streaming_flume_department_count.py
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from operator import add
import sys


conf = SparkConf(). \
setAppName("streaming_department_count"). \
setMaster("yarn-client")

sc = SparkContext(conf= conf)
ssc = StreamingContext(sc, 30)

topic = [fkdemoselva]
broker_list = {"metadata.broker.list":  nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667}
direct_kafak_stream = KafkaUtils.createDirectStream(ssc, topic, broker_list )

messages = direct_kafak_stream.map(lambda msg: msg[1])

department_msg = messages. \
filter(lambda msg: msg.split(" ")[6].split("/")[1] == "department")

department_names = department_msg. \
map(lambda msg: (msg.split(" ")[6].split("/")[2], 1))

department_conut = department_names. \
reduceByKey(add)

output_prefix = sys.argv[1]
department_conut.saveAsTextFiles(output_prefix)

ssc.start()
ssc.awaitTermination()