/***********************************************************************
 * Copyright (c) 2016-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.example.geomesa.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.commons.cli.*;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.HashMap;
import java.util.Map;

public class OSMIngest {

    static final String TOPIC = "topic";
    static final String INSTANCE_ID = "instanceId";
    static final String ZOOKEEPERS = "zookeepers";
    static final String USER = "user";
    static final String PASSWORD = "password";
    static final String AUTHS = "auths";
    static final String TABLE_NAME = "tableName";
    static final String FEATURE_NAME = "featureName";

    static final String[] ACCUMULO_CONNECTION_PARAMS =
        new String[] {
            INSTANCE_ID, ZOOKEEPERS, USER, PASSWORD, AUTHS, TABLE_NAME
        };

    static Options getCommonRequiredOptions() {
        Options options = new Options();
        Option instanceIdOpt =
            OptionBuilder.withArgName(INSTANCE_ID)
                .hasArg()
                .isRequired()
                .withDescription("accumulo connection parameter instanceId")
                .create(INSTANCE_ID);
        Option zookeepersOpt =
            OptionBuilder.withArgName(ZOOKEEPERS)
                .hasArg()
                .isRequired()
                .withDescription("accumulo connection parameter zookeepers")
                .create(ZOOKEEPERS);
        Option userOpt =
            OptionBuilder.withArgName(USER)
                .hasArg()
                .isRequired()
                .withDescription("accumulo connection parameter user")
                .create(USER);
        Option passwordOpt =
            OptionBuilder.withArgName(PASSWORD)
                .hasArg()
                .isRequired()
                .withDescription("accumulo connection parameter password")
                .create(PASSWORD);
        Option authsOpt =
            OptionBuilder.withArgName(AUTHS)
                .hasArg()
                .withDescription("accumulo connection parameter auths")
                .create(AUTHS);
        Option tableNameOpt =
            OptionBuilder.withArgName(TABLE_NAME)
                .hasArg()
                .isRequired()
                .withDescription("accumulo connection parameter tableName")
                .create(TABLE_NAME);
        Option featureNameOpt =
            OptionBuilder.withArgName(FEATURE_NAME)
                .hasArg()
                .isRequired()
                .withDescription("name of feature in accumulo table")
                .create(FEATURE_NAME);
        Option topicOpt =
            OptionBuilder.withArgName(TOPIC)
                .hasArg()
                .isRequired()
                .withDescription("name of kafka topic")
                .create(TOPIC);
        options.addOption(instanceIdOpt);
        options.addOption(zookeepersOpt);
        options.addOption(userOpt);
        options.addOption(passwordOpt);
        options.addOption(authsOpt);
        options.addOption(tableNameOpt);
        options.addOption(featureNameOpt);
        options.addOption(topicOpt);
        return options;
    }

    public static Map<String, String> getAccumuloDataStoreConf(CommandLine cmd) {
        Map<String , String> dsConf = new HashMap<String , String>();
        for (String param : ACCUMULO_CONNECTION_PARAMS) {
            dsConf.put(param, cmd.getOptionValue(param));
        }
        if (dsConf.get(AUTHS) == null) dsConf.put(AUTHS, "");
        return dsConf;
    }

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static int run(String[] args) throws Exception {
        CommandLineParser parser = new BasicParser();
        Options options = getCommonRequiredOptions();

        CommandLine cmd = parser.parse( options, args);
        Map<String, String> dsConf = getAccumuloDataStoreConf(cmd);

        String featureName = cmd.getOptionValue(FEATURE_NAME);
        SimpleFeatureType featureType = DataUtilities.createType(featureName, "geom:Point:srid=4326");

        DataStore ds = DataStoreFinder.getDataStore(dsConf);
        ds.createSchema(featureType);
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        String topic = cmd.getOptionValue(TOPIC);
        String groupId = topic;
        dsConf.put(OSMIngest.FEATURE_NAME, featureName);
        OSMKafkaSpout OSMKafkaSpout = new OSMKafkaSpout(dsConf, groupId, topic);
        topologyBuilder.setSpout("Spout", OSMKafkaSpout, 10).setNumTasks(10);
        OSMKafkaBolt OSMKafkaBolt = new OSMKafkaBolt(dsConf, groupId, topic);
        topologyBuilder.setBolt("Bolt", OSMKafkaBolt, 20).shuffleGrouping("Spout");
        Config stormConf = new Config();
        stormConf.setNumWorkers(10);
        stormConf.setDebug(true);
        StormSubmitter.submitTopology(topic, stormConf, topologyBuilder.createTopology());
        return 0;
    }
}
