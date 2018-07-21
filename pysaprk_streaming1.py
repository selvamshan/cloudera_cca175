pyspark --master yarn --conf spark.ui.port=12456

#for streaming object
import pyspark.streaming._


#16
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext 

conf = SparkConf(). \
setAppName("Streaming Word Count"). \
setMaster("yarn-client")

sc = SparkContext(conf= conf)

ssc = StreamingContext(sc, 15)

lines = ssc.socketTextStream('gw03.itversity.com', 19999)
word_count = lines. \
flatMap(lambda line: line.split(" ")). \
map(lambda word: (word, 1)). \
reduceByKey(lambda x, y: x + y)

word_count.pprint()
ssc.start()
ssc.awaitTermination()

#17

spark-submit --master yarn \
    --conf spark.ui.port=12890 \
    src/main/python/streaming_word_count.py


#20 department vist count    
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys
from operator import add

hostname = sys.argv[1]
port = int(sys.argv[2])

conf = SparkConf(). \
setAppName("Streaming department Conut"). \
setMaster("yarn-client")

sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 30)

message = ssc.socketTextStream(hostname, port)

department_count = message. \
filter(lambda msg: msg.split(" ")[6].split("/")[1] == "department"). \
map(lambda msg: (msg.split(" ")[6].split("/")[2], 1)). \
reduceByKey(add)

output_prefix = sys.argv[3]
department_count.saveAsTextFiles(output_prefix)

ssc.start()
ssc.awaitTermination()

spark-submit --master yarn \
    --conf spark.ui.port=12890 \
    src/main/python/streaming_department_count.py \
    gw03.itversity.com 19999 /user/selvamsandeep/streaming_department_count/cnt1

/opt/gen_logs/tail_logs.sh | nc -lk gw03.itversity.com 19999

#24

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
import sys
from operator import add

hostname = sys.argv[1]
port = int(sys.argv[2])

conf = SparkConf(). \
setAppName("Streaming department Conut"). \
setMaster("yarn-client")

sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 30)

agents = [(hostname, port)]
polling_stream = FlumeUtils.createPollingStream(ssc, agents)
message = polling_stream.map(lambda msg: msg[1])

department_count = message. \
filter(lambda msg: msg.split(" ")[6].split("/")[1] == "department"). \
map(lambda msg: (msg.split(" ")[6].split("/")[2], 1)). \
reduceByKey(add)

output_prefix = sys.argv[3]
department_count.saveAsTextFiles(output_prefix)

ssc.start()
ssc.awaitTermination()

spark-submit --master yarn \
    --conf spark.ui.port=12890 \
    --jars "/usr/hdp/2.5.0.0-1245/spark/lib/spark-streaming-flume-sink_2.10-1.6.2.jar,/usr/hdp/2.5.0.0-1245/spark/lib/spark-streaming-flume_2.10-1.6.2.jar,/usr/hdp/2.5.0.0-1245/flume/lib/flume-ng-sdk-1.5.2.2.5.0.0-1245.jar"  \
    src/main/python/streaming_flume_dept_count.py \
    gw03.itversity.com 8123 /user/selvamsandeep/streaming_department_count/cnt1-


#29    
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys
from operator import add



conf = SparkConf(). \
setAppName("Streaming department Conut"). \
setMaster("yarn-client")

sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 30)

topics = ["fkdemoselva"]
broker_list = {"metadata.broker.list": 
    "nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667"}
direct_kafka_stream = KafkaUtils. \
createDirectStream(ssc, topics, broker_list)

message = direct_kafka_stream.map(lambda msg: msg[1])

department_count = message. \
filter(lambda msg: msg.split(" ")[6].split("/")[1] == "department"). \
map(lambda msg: (msg.split(" ")[6].split("/")[2], 1)). \
reduceByKey(add)

output_prefix = sys.argv[1]
department_count.saveAsTextFiles(output_prefix)

ssc.start()
ssc.awaitTermination()


spark-submit --master yarn \
    --conf spark.ui.port=12890 \
    --jars "/usr/hdp/2.5.0.0-1245/kafka/libs/spark-streaming-kafka_2.10-1.6.2.jar,/usr/hdp/2.5.0.0-1245/kafka/libs/kafka_2.10-0.8.2.1.jar,/usr/hdp/2.5.0.0-1245/kafka/libs/metrics-core-2.2.0.jar"  \
    src/main/python/streaming_kafka_dept_count.py \
    /user/selvamsandeep/streaming_department_count/cnt2

 /usr/hdp/2.5.0.0-1245/kafka/libs/metrics-core-2.2.0.jar