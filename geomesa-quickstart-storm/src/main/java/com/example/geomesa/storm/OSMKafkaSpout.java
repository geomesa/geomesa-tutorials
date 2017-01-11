package com.example.geomesa.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class OSMKafkaSpout extends BaseRichSpout {

    private static final Logger log = Logger.getLogger(OSMKafkaSpout.class);
    ConsumerIterator<String, String> kafkaIterator = null;
    SpoutOutputCollector _collector = null;
    Map<String, String> conf;
    String groupId;
    String topic;

    public OSMKafkaSpout(Map<String, String> conf, String groupId, String topic) throws IOException {
        this.conf = conf;
        this.groupId = groupId;
        this.topic = topic;
    }

    public void nextTuple() {
        if(kafkaIterator.hasNext()) {
            List<Object> messages = new ArrayList<Object>();
            messages.add(kafkaIterator.next().message());
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
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, new StringDecoder(new VerifiableProperties()), new StringDecoder(new VerifiableProperties()));
        List<KafkaStream<String, String>> streams = consumerMap.get(topic);
        KafkaStream<String, String> stream = null;
        if (streams.size() == 1) {
            stream = streams.get(0);
        } else {
            log.error("Streams should be of size 1");
        }
        kafkaIterator = stream.iterator();
    }
}
