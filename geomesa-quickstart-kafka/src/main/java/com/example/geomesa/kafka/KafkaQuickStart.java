/***********************************************************************
 * Copyright (c) 2016-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.example.geomesa.kafka;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.text.WKTUtils$;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class KafkaQuickStart {

    // reads and parse the command line args
    public static Options getCommonRequiredOptions() {
        Options options = new Options();

        Option kafkaBrokers = OptionBuilder.withArgName("brokers")
                .hasArg()
                .isRequired()
                .withDescription("The comma-separated list of Kafka brokers, e.g. localhost:9092")
                .create("brokers");
        options.addOption(kafkaBrokers);

        Option zookeepers = OptionBuilder.withArgName("zookeepers")
                .hasArg()
                .isRequired()
                .withDescription("The comma-separated list of Zookeeper nodes that support your Kafka instance, e.g.: zoo1:2181,zoo2:2181,zoo3:2181")
                .create("zookeepers");
        options.addOption(zookeepers);

        Option zkPath = OptionBuilder.withArgName("zkPath")
                .hasArg()
                .withDescription("Zookeeper's discoverable path for metadata, defaults to /geomesa/ds/kafka")
                .create("zkPath");
        options.addOption(zkPath);

        Option visibility = OptionBuilder.withArgName("visibility")
                .hasArg()
                .create("visibility");
        options.addOption(visibility);

        Option automated = OptionBuilder.withArgName("automated")
                .create("automated");
        options.addOption(automated);

        return options;
    }

    // construct connection parameters for the DataStoreFinder
    public static Map<String, String> getKafkaDataStoreConf(CommandLine cmd) {
        Map<String, String> dsConf = new HashMap<>();
        dsConf.put("kafka.brokers", cmd.getOptionValue("brokers"));
        dsConf.put("kafka.zookeepers", cmd.getOptionValue("zookeepers"));
        dsConf.put("kafka.zk.path", cmd.getOptionValue("zkPath"));
        return dsConf;
    }

    // add a SimpleFeature to the producer every half second
    public static void addSimpleFeatures(SimpleFeatureType sft, FeatureStore producerFS, String visibility)
            throws InterruptedException, IOException {
        final int MIN_X = -180;
        final int MAX_X = 180;
        final int MIN_Y = -90;
        final int MAX_Y = 90;
        final int DX = 2;
        final int DY = 1;
        final String[] PEOPLE_NAMES = {"James", "John", "Peter", "Hannah", "Claire", "Gabriel"};
        final long SECONDS_PER_YEAR = 365L * 24L * 60L * 60L;
        final Random random = new Random();
        final ZonedDateTime MIN_DATE = ZonedDateTime.of(2015, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();

        // creates and updates two SimpleFeatures.
        // the first time this for loop runs the two SimpleFeatures are created.
        // in the subsequent iterations of the for loop, the two SimpleFeatures are updated.
        int numFeatures = (MAX_X - MIN_X) / DX;
        for (int i = 1; i <= numFeatures; i++) {
            builder.add(PEOPLE_NAMES[i % PEOPLE_NAMES.length]); // name
            builder.add((int) Math.round(random.nextDouble()*110)); // age
            builder.add(Date.from(MIN_DATE.plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR)).toInstant())); // dtg
            builder.add(WKTUtils$.MODULE$.read("POINT(" + (MIN_X + DX * i) + " " + (MIN_Y + DY * i) + ")")); // geom
            SimpleFeature feature1 = builder.buildFeature("1");
            feature1.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);

            builder.add(PEOPLE_NAMES[(i+1) % PEOPLE_NAMES.length]); // name
            builder.add((int) Math.round(random.nextDouble()*110)); // age
            builder.add(Date.from(MIN_DATE.plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR)).toInstant())); // dtg
            builder.add(WKTUtils$.MODULE$.read("POINT(" + (MIN_X + DX * i) + " " + (MAX_Y - DY * i) + ")")); // geom
            SimpleFeature feature2 = builder.buildFeature("2");
            feature2.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);

            if (visibility != null) {
                feature1.getUserData().put("geomesa.feature.visibility", visibility);
                feature2.getUserData().put("geomesa.feature.visibility", visibility);
            }

            // write the SimpleFeatures to Kafka
            featureCollection.add(feature1);
            featureCollection.add(feature2);
            producerFS.addFeatures(featureCollection);
            featureCollection.clear();

            // wait 100 ms in between updating SimpleFeatures to simulate a stream of data
            Thread.sleep(100);
        }
    }

    public static void addDeleteNewFeature(SimpleFeatureType sft, FeatureStore producerFS)
            throws InterruptedException, IOException {
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
        final Random random = new Random();

        String id = "1000";

        builder.add("Antoninus"); // name
        builder.add((int) Math.round(random.nextDouble()*110)); // age
        builder.add(new Date()); // dtg
        builder.add(WKTUtils$.MODULE$.read("POINT(-1 -1)")); // geom
        SimpleFeature feature = builder.buildFeature(id);

        featureCollection.add(feature);
        producerFS.addFeatures(featureCollection);

        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        Filter idFilter = ff.id(ff.featureId(id));
        producerFS.removeFeatures(idFilter);
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

        // create the producer and consumer KafkaDataStore objects
        Map<String, String> dsConf = getKafkaDataStoreConf(cmd);
        dsConf.put("kafka.consumer.count", "0");
        DataStore producerDS = DataStoreFinder.getDataStore(dsConf);
        dsConf.put("kafka.consumer.count", "1");
        DataStore consumerDS = DataStoreFinder.getDataStore(dsConf);

        // verify that we got back our KafkaDataStore objects properly
        if (producerDS == null) {
            throw new Exception("Null producer KafkaDataStore");
        }
        if (consumerDS == null) {
            throw new Exception("Null consumer KafkaDataStore");
        }

        try {
            // create the schema which creates a topic in Kafka
            // (only needs to be done once)
            final String sftName = "KafkaQuickStart";
            final String sftSchema = "name:String,age:Int,dtg:Date,*geom:Point:srid=4326";
            SimpleFeatureType sft = SimpleFeatureTypes.createType(sftName, sftSchema);
            producerDS.createSchema(sft);

            if (!cmd.hasOption("automated")) {
                System.out.println("Register KafkaDataStore in GeoServer (Press enter to continue)");
                System.in.read();
            }

            // the live consumer must be created before the producer writes features
            // in order to read streaming data.
            // i.e. the live consumer will only read data written after its instantiation
            SimpleFeatureSource consumerFS = consumerDS.getFeatureSource(sftName);
            SimpleFeatureStore producerFS = (SimpleFeatureStore) producerDS.getFeatureSource(sftName);

            // creates and adds SimpleFeatures to the producer every 1/5th of a second
            System.out.println("Writing features to Kafka... refresh GeoServer layer preview to see changes");
            long replayStart = System.currentTimeMillis();

            String vis = cmd.getOptionValue("visibility");
            if(vis != null) System.out.println("Writing features with " + vis);
            addSimpleFeatures(sft, producerFS, vis);
            long replayEnd = System.currentTimeMillis();

            // read from Kafka after writing all the features.
            // LIVE CONSUMER - will obtain the current state of SimpleFeatures
            System.out.println("\nConsuming with the live consumer...");
            SimpleFeatureCollection featureCollection = consumerFS.getFeatures();
            System.out.println(featureCollection.size() + " features were written to Kafka");

            addDeleteNewFeature(sft, producerFS);

            // read from Kafka after writing all the features.
            // LIVE CONSUMER - will obtain the current state of SimpleFeatures
            System.out.println("\nConsuming with the live consumer...");
            featureCollection = consumerFS.getFeatures();
            System.out.println(featureCollection.size() + " features were written to Kafka");

            // the state of the two SimpleFeatures is real time here
            System.out.println("Here are the two SimpleFeatures that were obtained with the live consumer:");
            SimpleFeatureIterator featureIterator = featureCollection.features();
            SimpleFeature feature1 = featureIterator.next();
            SimpleFeature feature2 = featureIterator.next();
            featureIterator.close();
            printFeature(feature1);
            printFeature(feature2);

            if (System.getProperty("clear") != null) {
                // Run Java command with -Dclear=true
                // This will cause a 'clear'
                producerFS.removeFeatures(Filter.INCLUDE);
            }
        } finally {
            producerDS.dispose();
            consumerDS.dispose();
        }

        System.exit(0);
    }
}
