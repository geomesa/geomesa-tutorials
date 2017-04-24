/***********************************************************************
 * Copyright (c) 2016-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.example.geomesa.kafka09;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import org.apache.commons.cli.*;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.kafka.KafkaDataStoreHelper;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.text.WKTUtils$;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KafkaLoadTester {
    public static final String KAFKA_BROKER_PARAM = "brokers";
    public static final String ZOOKEEPERS_PARAM = "zookeepers";
    public static final String ZK_PATH = "zkPath";
    public static final String PARTITIONS = "partitions";
    public static final String REPLICATION = "replication";
    public static final String VISIBILITY = "visibility";
    public static final String LOAD = "count";
    public static final String DELAY = "delay";

    public static final String[] KAFKA_CONNECTION_PARAMS = new String[] {
            KAFKA_BROKER_PARAM,
            ZOOKEEPERS_PARAM,
            ZK_PATH,
            PARTITIONS,
            REPLICATION
    };

    // reads and parse the command line args
    public static Options getCommonRequiredOptions() {
        Options options = new Options();

        Option kafkaBrokers = OptionBuilder.withArgName(KAFKA_BROKER_PARAM)
                .hasArg()
                .isRequired()
                .withDescription("The comma-separated list of Kafka brokers, e.g. localhost:9092")
                .create(KAFKA_BROKER_PARAM);
        options.addOption(kafkaBrokers);

        Option zookeepers = OptionBuilder.withArgName(ZOOKEEPERS_PARAM)
                .hasArg()
                .isRequired()
                .withDescription("The comma-separated list of Zookeeper nodes that support your Kafka instance, e.g.: zoo1:2181,zoo2:2181,zoo3:2181")
                .create(ZOOKEEPERS_PARAM);
        options.addOption(zookeepers);

        Option zkPath = OptionBuilder.withArgName(ZK_PATH)
                .hasArg()
                .withDescription("Zookeeper's discoverable path for metadata, defaults to /geomesa/ds/kafka")
                .create(ZK_PATH);
        options.addOption(zkPath);

        Option partitions = OptionBuilder.withArgName(PARTITIONS)
                .hasArg()
                .withDescription("Number of partitions to use in Kafka topics")
                .create(PARTITIONS);
        options.addOption(partitions);

        Option replication = OptionBuilder.withArgName(REPLICATION)
                .hasArg()
                .withDescription("Replication factor to use in Kafka topics")
                .create(REPLICATION);
        options.addOption(replication);

        Option load = OptionBuilder.withArgName(LOAD)
                .hasArg()
                .withDescription("Number of entities to simulate.")
                .create(LOAD);
        options.addOption(load);

        Option visibility = OptionBuilder.withArgName(VISIBILITY)
                .hasArg()
                .withDescription("Visibilities to set on each feature created")
                .create(VISIBILITY);
        options.addOption(visibility);

        Option delay = OptionBuilder.withArgName(DELAY)
                .hasArg()
                .withDescription("Delay (in ms) between each write of features")
                .create(DELAY);
        options.addOption(delay);

        return options;
    }

    // construct connection parameters for the DataStoreFinder
    public static Map<String, String> getKafkaDataStoreConf(CommandLine cmd) {
        Map<String, String> dsConf = new HashMap<>();
        for (String param : KAFKA_CONNECTION_PARAMS) {
            String value = cmd.getOptionValue(param);
            if (value != null)
                dsConf.put(param, value);
        }
        return dsConf;
    }

    public static SimpleFeature createFeature(SimpleFeatureBuilder builder, int i, String visibility) {
        final String[] PEOPLE_NAMES = {"James", "John", "Peter", "Hannah", "Claire", "Gabriel"};
        final Random random = new Random();

        Double lat = random.nextDouble() * 180 - 90;

        builder.reset();
        builder.add(PEOPLE_NAMES[i % PEOPLE_NAMES.length]); // name

        builder.add((int) Math.round(random.nextDouble()*110)); // age
        builder.add(random.nextDouble());
        builder.add(lat);
        builder.add(new Date()); // dtg
        builder.add(WKTUtils$.MODULE$.read("POINT(" + -180.0 + " " + lat + ")")); // geom
        SimpleFeature feat = builder.buildFeature(Integer.toString(i));
        if (visibility != null) {
            feat.getUserData().put("geomesa.feature.visibility", visibility);
        }
        return feat;
    }

    // prints out attribute values for a SimpleFeature
    public static void printFeature(SimpleFeature f) {
        Iterator<Property> props = f.getProperties().iterator();
        int propCount = f.getAttributeCount();
        System.out.print("fid:" + f.getID());
        for (int i = 0; i < propCount; i++) {
            Name propName = props.next().getName();
            System.out.print(" | " + propName + ":" + f.getAttribute(propName));
        }
        System.out.println();
    }

    public static void main(String[] args) throws Exception {
        // read command line args for a connection to Kafka
        CommandLineParser parser = new BasicParser();
        Options options = getCommonRequiredOptions();
        CommandLine cmd = parser.parse(options, args);
        String visibility = getVisibility(cmd);
        Integer delay = getDelay(cmd);

        if (visibility == null) {
            System.out.println("visibility: null");
        } else {
            System.out.println("visibility: '"+visibility+"'");
        }

        // create the producer and consumer KafkaDataStore objects
        Map<String, String> dsConf = getKafkaDataStoreConf(cmd);
        System.out.println("KDS config: "+dsConf);
        dsConf.put("isProducer", "true");
        DataStore producerDS = DataStoreFinder.getDataStore(dsConf);
        dsConf.put("isProducer", "false");
        DataStore consumerDS = DataStoreFinder.getDataStore(dsConf);

        // verify that we got back our KafkaDataStore objects properly
        if (producerDS == null) {
            throw new Exception("Null producer KafkaDataStore");
        }
        if (consumerDS == null) {
            throw new Exception("Null consumer KafkaDataStore");
        }

        // create the schema which creates a topic in Kafka
        // (only needs to be done once)
        final String sftName = "KafkaStressTest";
        final String sftSchema = "name:String,age:Int,step:Double,lat:Double,dtg:Date,*geom:Point:srid=4326";
        SimpleFeatureType sft = SimpleFeatureTypes.createType(sftName, sftSchema);
        // set zkPath to default if not specified
        String zkPath = (dsConf.get(ZK_PATH) == null) ? "/geomesa/ds/kafka" : dsConf.get(ZK_PATH);
        SimpleFeatureType preppedOutputSft = KafkaDataStoreHelper.createStreamingSFT(sft, zkPath);
        // only create the schema if it hasn't been created already
        if (!Arrays.asList(producerDS.getTypeNames()).contains(sftName))
            producerDS.createSchema(preppedOutputSft);

        System.out.println("Register KafkaDataStore in GeoServer (Press enter to continue)");
        System.in.read();

        // the live consumer must be created before the producer writes features
        // in order to read streaming data.
        // i.e. the live consumer will only read data written after its instantiation
        SimpleFeatureStore producerFS = (SimpleFeatureStore) producerDS.getFeatureSource(sftName);
        SimpleFeatureSource consumerFS = consumerDS.getFeatureSource(sftName);

        // creates and adds SimpleFeatures to the producer every 1/5th of a second
        System.out.println("Writing features to Kafka... refresh GeoServer layer preview to see changes");

        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);

        Integer numFeats = getLoad(cmd);

        System.out.println("Building a list of " + numFeats + " SimpleFeatures.");
        List<SimpleFeature> features = IntStream.range(1, numFeats).mapToObj(i -> createFeature(builder, i, visibility)).collect(Collectors.toList());

        // set variables to estimate feature production rate
        Long startTime = null;
        Long featuresSinceStartTime = 0L;
        int cycle = 0;
        int cyclesToSkip = 50000/numFeats; // collect enough features
                                           // to get an accurate rate estimate

        while (true) {
            // write features
            features.forEach( feat -> {
                        try {
                            DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
                            featureCollection.add(feat);
                            producerFS.addFeatures(featureCollection);
                        } catch (Exception e) {
                            System.out.println("Caught an exception while writing features.");
                            e.printStackTrace();
                        }
                        updateFeature(feat);
                    }
            );

            // count features written
            Integer consumerSize = consumerFS.getFeatures().size();
            cycle++;
            featuresSinceStartTime += consumerSize;
            System.out.println("At " + new Date() + " wrote "+consumerSize+" features");

            // if we've collected enough features, calculate the rate
            if (cycle >= cyclesToSkip || startTime == null) {
                Long endTime = System.currentTimeMillis();
                if ( startTime != null ) {
                    Long diffTime = endTime - startTime;
                    Double rate = (featuresSinceStartTime.doubleValue() * 1000.0)/diffTime.doubleValue();
                    System.out.printf("%.1f feats/sec (%d/%d)\n", rate, featuresSinceStartTime, diffTime);
                }
                cycle = 0;
                startTime = endTime;
                featuresSinceStartTime = 0L;
            }

            // sleep before next write
            if (delay != null) {
                System.out.printf("Sleeping for %d ms\n", delay);
                Thread.sleep(delay);
            }
        }
    }

    public static void updateFeature(SimpleFeature feature) {
        Point point = (Point) feature.getDefaultGeometry();
        Double step = (Double) feature.getAttribute("step");

        Double newLong = nudgeLong(point.getX() + step);

        Geometry newPoint = WKTUtils$.MODULE$.read("POINT(" + newLong + " " + point.getY() + ")");

        feature.setAttribute("dtg", new Date());
        feature.setDefaultGeometry(newPoint);
    }

    public static Double nudgeLong(Double preLong) {
        if (preLong < -180) {
            return (preLong + 360);
        } else if (preLong > 180) {
            return (preLong - 360);
        } else {
            return preLong;
        }
    }

    public static Integer getLoad(CommandLine cmd) {
        String count = cmd.getOptionValue(LOAD, "1000");
        return Integer.parseInt(count);
    }

    public static String getVisibility(CommandLine cmd) {
        return cmd.getOptionValue(VISIBILITY);
    }

    public static Integer getDelay(CommandLine cmd) {
        String delay = cmd.getOptionValue(DELAY);
        if (delay != null) {
            return Integer.parseInt(delay);
        } else {
            return null;
        }
    }
}
