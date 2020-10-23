/***********************************************************************
 * Copyright (c) 2016-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.example.geomesa.storm;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class OSMKafkaSpout extends BaseRichSpout {

    private static final Logger log = Logger.getLogger(OSMKafkaSpout.class);
    SpoutOutputCollector _collector = null;
    Map<String, String> conf;
    String groupId;
    String topic;
    Consumer<String, String> consumer;

    public OSMKafkaSpout(Map<String, String> conf, String groupId, String topic) throws IOException {
        this.conf = conf;
        this.groupId = groupId;
        this.topic = topic;
    }

    public void nextTuple() {
        Iterator<ConsumerRecord<String, String>> records = consumer.poll(Duration.ofMillis(10)).iterator();
        if (records.hasNext()) {
            List<Object> messages = new ArrayList<>();
            messages.add(records.next());
            while (records.hasNext()) {
                messages.add(records.next().value());
            }
            _collector.emit(messages);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("field"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        Properties props = new Properties();
        props.put("zookeeper.connect", conf.get(OSMIngest.ZOOKEEPERS));
        props.put("group.id", groupId);
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(topic), new NoOpConsumerRebalanceListener());
    }
}
