# Smart-Waste-Management

This project presents the design and simulation of an IoT-based Smart Waste Management System. It focuses on the data pipeline, architectural model, and analytical capabilities of a system intended to optimize urban waste collection. Using simulated sensor data, this project demonstrates how real-time monitoring, data processing, and decision-making can improve operational efficiency and promote environmental sustainability in a smart city context.

## Start the Zookeeper
bin\windows\zookeeper-server-start.bat config\zookeeper.properties

## Start the Kafka
bin\windows\kafka-server-start.bat config\server.properties

## Start the simulator
python data_simulator\simulator.py --kafka-brokers localhost:9092 --kafka-topic waste-sensor-data

## Start the spark streaming

## Start cassandra and create table
docker start cassandra

docker exec -it cassandra cqlsh

cqlsh> create keyspace wastebin with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

cqlsh> use wastebin;

cqlsh:wastebin> create table sensor_data (bin_id text, timestamp text, sensor_type text, measurement text, primary key(bin_id, timestamp));

cqlsh:wastebin> select * from sensor_data;

Camera Sensor dataset
https://www.kaggle.com/datasets/hussainmanasi/trash-bins
