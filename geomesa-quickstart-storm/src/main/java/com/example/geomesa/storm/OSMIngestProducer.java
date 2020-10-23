/***********************************************************************
 * Copyright (c) 2016-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.example.geomesa.storm;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class OSMIngestProducer {

    private static final Logger log = Logger.getLogger(OSMIngestProducer.class);
    static String INGEST_FILE = "ingestFile";
    static String TOPIC = "topic";
    static String BROKER_LIST = "brokers";

    public static Options getRequiredOptions() {
        Options options = new Options();
        Option ingestFileOpt = OptionBuilder.withArgName(INGEST_FILE)
            .hasArg()
            .isRequired()
            .withDescription("ingest csv file")
            .create(INGEST_FILE);
        Option topicOpt = OptionBuilder.withArgName(TOPIC)
            .hasArg()
            .isRequired()
            .withDescription("name of kafka topic")
            .create(TOPIC);
        Option brokersOpt = OptionBuilder.withArgName(BROKER_LIST)
            .hasArg()
            .isRequired()
            .withDescription("kafka metadata brokers list")
            .create(BROKER_LIST);
        options.addOption(ingestFileOpt);
        options.addOption(topicOpt);
        options.addOption(brokersOpt);
        return options;
    }

    public static void main(String[] args)
          throws FileNotFoundException, ParseException {

        CommandLineParser parser = new BasicParser();
        Options options = getRequiredOptions();
        CommandLine cmd = null;
        Producer<String, String> producer = null;
        String topic = null;
        try {
            cmd = parser.parse(options, args);
            topic = cmd.getOptionValue(TOPIC);
            final Properties props = new Properties();
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("metadata.broker.list", cmd.getOptionValue(BROKER_LIST));
            props.put("producer.type", "async");
            producer = new KafkaProducer<>(props);
        } catch (ParseException e) {
            log.error("Error parsing command line args", e);
            throw e;
        }

        BufferedReader bufferedReader = null;
        try {
            FileReader fileReader = new FileReader(cmd.getOptionValue(INGEST_FILE));
            bufferedReader = new BufferedReader(fileReader);
        } catch (FileNotFoundException e) {
            log.error("File not found", e);
            throw e;
        }

        //to assign messages to different partitions using default partitioner, need random key
        Random rnd = new Random();
        try {
            for (String x = bufferedReader.readLine();
                x != null;
                x = bufferedReader.readLine()) {
                producer.send(new ProducerRecord<>(topic, String.valueOf(rnd.nextInt()), x));
            }
        } catch (IOException e) {
            log.error("Error reading lines from file", e);
        }
    }
}