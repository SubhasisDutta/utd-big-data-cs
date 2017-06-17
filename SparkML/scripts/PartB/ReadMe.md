### Modify the kafka config file for zookeeper
```
kafka/config/zookeeper.properties
```

# STEP 1
## Start the zookeeper
1. Go to kafka bin folder
2. Start zookeeper
```
Kafka$ bin/zookeeper-server-start.sh config/zookeeper.properties
```

### check zookeeper running [Optional]
```
$telnet localhost 2181
>stat - runs in standalone mode
```

# STEP 2
## Start atleast one broker
```
kafka$ bin/kafka-server-start.sh config/server.properties
```

# STEP 3
# Create Kafka topic
```
kafka$ bin/kafka-topics.sh --create --topic tweet_topic --zookeeper localhost:2181 --replication-factor 1 --partitions 1
```

### Check status of topics present [Optional]
```
$ bin/kafka-topics.sh --list --zookeeper localhost:2181
```

### Console connect to topic - producer [Optional]
```
$ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic tweet_topic
```

### Console connect to consumer [Optional]
```
$ bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic tweet_topic --from-beginning
```

# STEP 4
## Run the Scrapper With Kafka Producer
```
PartB$ python3 scrapper.py trump obama
```



# STEP 5

### Keep the Elasticsearch and Kibana running
    1. Start Elastic Search
    
        a. Increase Memory map
        ```
        sudo sysctl -w vm.max_map_count=262144
        ```
        
        b. Start Service
        ```
        ./bin/elasticsearch
        ```
        c. Test
        ```
        curl http://localhost:9200
        ```
    
    2. Start Kibana
    
        a. Start Kibana Service
        ```
        sudo service kibana start
        ```
        b. Open in browser
        ```
        localhost:5601
        ```
    

## Run the spark streaming with Kafka Consumer and insert it into Elasticsearch

```
$ spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 streamProcessor.py tweet_topic
```




