# create a topic
kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic tweets --partitions 6 --replication-factor 1

# consuming
# kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic tweets