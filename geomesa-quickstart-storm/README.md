Streaming Analysis of Open Street Map using GeoMesa and Storm

This sample project demonstrates how to analyze OSM using GeoMesa.

To ingest: 

1. Build the project
   ```
   mvn clean install
   ```

2. Submit topology
   ```
   $STORM_HOME/bin/storm jar geomesa-examples-storm/target/geomesa-examples-storm-${version}.jar com.example.geomesa.storm.OSMIngest -instanceId <instanceId> -zookeepers <zookeepers> -user <user> -password <password> -auths <auths> -tableName <tableName> -featureName <featureName> -topic <kafka topic name>
   ```

3. Create kafka topic
   ```
   $KAFKA_HOME/bin/kafka-create-topic.sh --zookeeper <zookeepers> --replica 3 --partition 10 --topic <kafka topic name>
   ```

4. Produce kafka messages from ingest file
   ```
   java -cp geomesa-examples-storm/target/geomesa-examples-storm-${version}.jar com.example.geomesa.storm.OSMIngestProducer -ingestFile <ingestFile> -topic <kafka topic name> -brokers <kafka broker list>
   ```
