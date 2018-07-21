# example.conf: A single-node Flume configuration

# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type = netcat
a1.sources.r1.bind = localhost
a1.sources.r1.port = 44444

# Describe the sink
a1.sinks.k1.type = logger

# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1


flume-ng agent \
--name a1  \
--conf-file /home/selvamsandeep/flume_demo1/example.conf
#3
#4 
tail -F /opt/gen_logs/logs/access.log

#5
%s/a1/wh

flume-ng agent \
--name wh  \
--conf-file /home/selvamsandeep/flume_demo1/wslogstohdfs/wshdfs.conf

#6
# Name the components on this agent
wh.sources = ws
wh.sinks = hd
wh.channels = mem

# Describe/configure the source
wh.sources.ws.type = exec
wh.sources.ws.command= tail -F /opt/gen_logs/logs/access.log


# Describe the sink
wh.sinks.hd.type = hdfs
wh.sinks.hd.hdfs.path = hdfs://nn01.itversity.com:8020/user/selvamsandeep/flume_demo
wh.sinks.hd.hdfs.filePrefix = flume_demo
wh.sinks.hd.hdfs.fileSuffix = .txt
wh.sinks.hd.hdfs.rollInterval =120
wh.sinks.hd.hdfs.rollSize = 1048576
wh.sinks.hd.hdfs.rollCount = 100
wh.sinks.hd.hdfs.fileType = DataStream
# Use a channel which buffers events in memory
wh.channels.mem.type = memory
wh.channels.mem.capacity = 1000
wh.channels.mem.transactionCapacity = 100

# Bind the source and sink to the channel
wh.sources.ws.channels = mem
wh.sinks.hd.channel = mem


flume-ng agent \
--name wh  \
--conf-file /home/selvamsandeep/flume_demo1/wslogstohdfs/wshdfs.conf

#9

#11
#22
#streaming department  count flume multiplex
sdc.sources = ws
sdc.sinks = hd spark
sdc.channels = hdmem sparkmem

# Describe/configure the source
sdc.sources.ws.type = exec
sdc.sources.ws.command= tail -F /opt/gen_logs/logs/access.log


# Describe the sink
sdc.sinks.hd.type = hdfs
sdc.sinks.hd.hdfs.path = hdfs://nn01.itversity.com:8020/user/selvamsandeep/flume_demo
sdc.sinks.hd.hdfs.filePrefix = flume_demo
sdc.sinks.hd.hdfs.fileSuffix = .txt
sdc.sinks.hd.hdfs.rollInterval =120
sdc.sinks.hd.hdfs.rollSize = 1048576
sdc.sinks.hd.hdfs.rollCount = 100
sdc.sinks.hd.hdfs.fileType = DataStream

sdc.sinks.spark.type = org.apache.spark.streaming.flume.sink.SparkSink
sdc.sinks.spark.hostname = gw03.itversity.com
sdc.sinks.spark.port = 8123


# Use a channel sdcich buffers events in memory
sdc.channels.hdmem.type = memory
sdc.channels.hdmem.capacity = 1000
sdc.channels.hdmem.transactionCapacity = 100

sdc.channels.sparkmem.type = memory
sdc.channels.sparkmem.capacity = 1000
sdc.channels.sparkmem.transactionCapacity = 100

#
# Bind the source and sink to the channel
sdc.sources.ws.channels = hdmem  saprkmem
sdc.sinks.hd.channel = hdmem
sdc.sinks.spark.channel = sparkmem

flume-ng agent \
--name sdc \
--conf-file sdc.conf
                                                                                             
                                                         
flume-ng agent -n sdc -f sdc.conf   


#flume integration with kafka sinks

# Name the components on this agent
wk.sources = ws
wk.sinks = kafka
wk.channels = mem

# Describe/configure the source
wk.sources.ws.type = exec
wk.sources.ws.command= tail -F /opt/gen_logs/logs/access.log


# Describe the sink
wk.sinks.kafka.type = org.apache.flume.sink.kafka.KafkaSink
wk.sinks.kafka.brokerList = nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667
wk.sinks.kafka.topic = fkdemoselva

# Use a channel wkich buffers events in memory
wk.channels.mem.type = memory
wk.channels.mem.capacity = 1000
wk.channels.mem.transactionCapacity = 100

# Bind the source and sink to the channel
wk.sources.ws.channels = mem
wk.sinks.kafka.channel = mem

flume-ng agent \
--name wk \
--conf-file wskafka.conf

flume-ng agent -n wk -f wskafka.conf

kafka-console-consumer.sh  \
    --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 \
    --topic fkdemoselva  \
    --from-beginning   

#29    
