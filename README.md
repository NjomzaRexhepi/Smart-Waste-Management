# Smart-Waste-Management

## Start the Zookeeper
bin\windows\zookeeper-server-start.bat config\zookeeper.properties

## Start the Kafka
bin\windows\kafka-server-start.bat config\server.properties

## Create the kafka-topic 
bin\windows\kafka-topics.bat --create --topic waste-sensor-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

## Consume messages from Kafka
bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic waste-sensor-data --from-beginning

## Start the simulator
python data_simulator\simulator.py --kafka-brokers localhost:9092 --kafka-topic waste-sensor-data
