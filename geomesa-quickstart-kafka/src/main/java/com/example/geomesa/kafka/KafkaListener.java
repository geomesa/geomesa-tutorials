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
import org.geotools.data.FeatureEvent;
import org.geotools.data.FeatureListener;
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent.KafkaFeatureChanged;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.Name;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class KafkaListener {

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
        DataStore consumerDS = DataStoreFinder.getDataStore(dsConf);

        // verify that we got back our KafkaDataStore object properly
        if (consumerDS == null) {
            throw new Exception("Null consumer KafkaDataStore");
        }

        Map<String, FeatureListener> listeners = new HashMap<>();

        try {
            for (String typeName: consumerDS.getTypeNames()) {
                System.out.println("Registering a feature listener for type " + typeName + ".");
                FeatureListener listener = new FeatureListener() {
                    @Override
                    public void changed(FeatureEvent featureEvent) {
                        System.out.println("Received FeatureEvent from layer " + typeName + " of Type: " + featureEvent.getType());
                        if (featureEvent.getType() == FeatureEvent.Type.CHANGED &&
                            featureEvent instanceof KafkaFeatureChanged) {
                            printFeature(((KafkaFeatureChanged) featureEvent).feature());
                        } else if (featureEvent.getType() == FeatureEvent.Type.REMOVED) {
                            System.out.println("Received Delete for filter: " + featureEvent.getFilter());
                        }
                    }
                };
                consumerDS.getFeatureSource(typeName).addFeatureListener(listener);
                listeners.put(typeName, listener);
            }

            while (true) {
                // Wait for user to terminate with ctrl-C.
            }
        } finally {
            for (Entry<String, FeatureListener> entry: listeners.entrySet()) {
                consumerDS.getFeatureSource(entry.getKey()).removeFeatureListener(entry.getValue());
            }
            consumerDS.dispose();
        }
    }
}
