kafka-topics.sh --create \
    --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 \
    --replication-factor 1 \
    --partitions 1 \
    --topic kafkademoselva1

kafka-topics.sh --list \
     --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 \
     --topic kafkademoselva1

kafka-console-producer.sh  \
    --broker-list nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667 \
    --topic kafkademoselva1    


kafka-console-consumer.sh  \
    --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 \
    --topic kafkademoselva1  \
    --from-beginning   


#12       