# Përshkrimi i projektit për menaxhimin e mbetjeve përmes IoT

Ky projekt paraqet dizajnin dhe simulimin e një Sistemi të Menaxhimit të Mbeturinave të Zgjuara bazuar në IoT. Ai fokusohet në rrjedhën e të dhënave, modelin arkitekturor dhe aftësitë analitike të një sistemi të destinuar për të optimizuar mbledhjen e mbeturinave urbane. Duke përdorur të dhëna të simuluara nga sensorët, ky projekt demonstron se si monitorimi në kohë reale, përpunimi i të dhënave dhe marrja e vendimeve mund të përmirësojnë efikasitetin operacional dhe të promovojnë qëndrueshmërinë mjedisore në kontekstin e një qyteti të zgjuar.

## Udhëzimet për Setup

Sigurohuni që keni të instaluara Docker, Zookeeper, Kafka, Cassandra dhe Spark.

1. Nis Zookeeper dhe Kafka (në Docker ose lokalisht)

    `docker-compose up -d`

2. Nise Simulatorin: Fajlli simulator.py do të gjenerojë të dhëna nga sensorët e mbeturinave dhe do t’i dërgojë në Kafka.

3. Nis Cassandra (në Docker)

    `docker start cassandra`

    `docker exec -it cassandra cqlsh`

Brenda cqlsh, krijo keyspace dhe tabelën:

`create keyspace wastebin with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};`

`use wastebin;`

`create table sensor_data (
    bin_id text,
    timestamp text,
    sensor_type text,
    measurement text,
    primary key (bin_id, timestamp)
);`

`select * from sensor_data;`

5. Nis Spark Streaming: Pasi Cassandra të jetë duke punuar dhe tabela të jetë krijuar, lësho punën e Spark Streaming për të lexuar të dhënat nga Kafka dhe për t’i ruajtur në Cassandra.

Camera Sensor dataset
https://www.kaggle.com/datasets/hussainmanasi/trash-bins
