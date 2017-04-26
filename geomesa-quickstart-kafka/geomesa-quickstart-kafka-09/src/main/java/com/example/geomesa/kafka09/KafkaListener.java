/***********************************************************************
 * Copyright (c) 2016-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.example.geomesa.kafka09;

import org.geotools.data.simple.SimpleFeatureSource;
import org.locationtech.geomesa.kafka.KafkaFeatureEvent;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.Name;
import org.apache.commons.cli.*;
import org.geotools.data.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class KafkaListener {
    public static final String KAFKA_BROKER_PARAM = "brokers";
    public static final String ZOOKEEPERS_PARAM = "zookeepers";
    public static final String ZK_PATH = "zkPath";

    public static final String[] KAFKA_CONNECTION_PARAMS = new String[] {
            KAFKA_BROKER_PARAM,
            ZOOKEEPERS_PARAM,
            ZK_PATH
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

        return options;
    }

    // construct connection parameters for the DataStoreFinder
    public static Map<String, String> getKafkaDataStoreConf(CommandLine cmd) {
        Map<String, String> dsConf = new HashMap<>();
        for (String param : KAFKA_CONNECTION_PARAMS) {
            dsConf.put(param, cmd.getOptionValue(param));
        }
        return dsConf;
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

        // create the consumer KafkaDataStore object
        Map<String, String> dsConf = getKafkaDataStoreConf(cmd);
        dsConf.put("isProducer", "false");
        DataStore consumerDS = DataStoreFinder.getDataStore(dsConf);

        // verify that we got back our KafkaDataStore object properly
        if (consumerDS == null) {
            throw new Exception("Null consumer KafkaDataStore");
        }

        // create the schema which creates a topic in Kafka
        // (only needs to be done once)
        // TODO: This should be rolled into the Command line options to make this more general.
        registerListeners(consumerDS);

        while (true) {
            // Wait for user to terminate with ctrl-C.
        }
    }

    private static void registerListeners(DataStore consumerDS) throws IOException {
        for (String typename : consumerDS.getTypeNames()) {
            registerListenerForFeature(consumerDS, typename);
        }
    }

    // the live consumer must be created before the producer writes features
    // in order to read streaming data.
    // i.e. the live consumer will only read data written after its instantiation
    private static void registerListenerForFeature(DataStore consumerDS, final String sftName) throws IOException {
        SimpleFeatureSource consumerFS = consumerDS.getFeatureSource(sftName);
        System.out.println("Registering a feature listener for type " + sftName + ".");

        consumerFS.addFeatureListener(new FeatureListener() {
            @Override
            public void changed(FeatureEvent featureEvent) {
                System.out.println("Received FeatureEvent from layer " + sftName + " of Type: " + featureEvent.getType());

                if (featureEvent.getType() == FeatureEvent.Type.CHANGED &&
                        featureEvent instanceof KafkaFeatureEvent) {
                    printFeature(((KafkaFeatureEvent) featureEvent).feature());
                }

                if (featureEvent.getType() == FeatureEvent.Type.REMOVED) {
                    System.out.println("Received Delete for filter: " + featureEvent.getFilter());
                }
            }
        });
    }
}
