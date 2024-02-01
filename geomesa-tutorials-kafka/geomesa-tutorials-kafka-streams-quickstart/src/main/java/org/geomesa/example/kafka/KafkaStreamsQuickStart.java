/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.kafka;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.geomesa.example.data.CvilleRICData;
import org.geomesa.example.data.TutorialData;
import org.geomesa.example.quickstart.GeoMesaQuickStart;
import org.geotools.api.data.DataAccessFactory.Param;
import org.geotools.api.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.api.data.Query;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.data.SimpleFeatureStore;
import org.geotools.feature.visitor.BoundsVisitor;
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory;
import org.locationtech.geomesa.kafka.jstreams.GeoMesaStreamsBuilder;
import org.locationtech.geomesa.kafka.streams.GeoMesaMessage;
import org.locationtech.jts.geom.Envelope;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.*;

public class KafkaStreamsQuickStart extends GeoMesaQuickStart {

    private DataStore consumer = null;
    private GeoMesaStreamsBuilder builder = null;

    private boolean wait = true;

    // uses t-dive streaming data
    public KafkaStreamsQuickStart(String[] args) throws ParseException {
        super(args, new KafkaDataStoreFactory().getParametersInfo(), new CvilleRICData());
    }

    @Override
    public Options createOptions(Param[] parameters) {
        Options options = super.createOptions(parameters);
        options.addOption(Option.builder().longOpt("automated").build());
        return options;
    }

    @Override
    public void initializeFromOptions(CommandLine command) {
        super.initializeFromOptions(command);
        // TODO
        wait = !Boolean.parseBoolean(command.getOptionValue("automated", "false"));
    }

    @Override
    public DataStore createDataStore(Map<String, String> params) throws IOException {
        // use geotools service loading to get a datastore instance
        // we load two data stores - one is a producer, that writes features to kafka
        // the second is a consumer, that reads them from kafka

        params.put("kafka.consumer.count", "0");
        DataStore producer = super.createDataStore(params);

        builder = GeoMesaStreamsBuilder.create(params, Topology.AutoOffsetReset.LATEST);

        params.put("kafka.consumer.count", "1");
        consumer = super.createDataStore(params);

        return producer;
    }

    static File tempDir() {
        File file;
        try {
            file = Files.createTempDirectory("kafka-streams-quickstart").toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        file.deleteOnExit();
        return file;
    }

    @Override
    public void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        // the live consumer must be created before the producer writes features
        // in order to read streaming data.
        // i.e. the live consumer will only read data written after its instantiation
        SimpleFeatureSource consumerFS = consumer.getFeatureSource(sft.getTypeName());
        SimpleFeatureStore producerFS = (SimpleFeatureStore) datastore.getFeatureSource(sft.getTypeName());

        // Configure and start the streams thread
        setupStreams(sft);

        if (wait) {
            BoundsVisitor visitor = new BoundsVisitor();
            for (SimpleFeature feature: features) {
                visitor.visit(feature);
            }
            Envelope env = visitor.getBounds();

            System.out.println("Feature type created - register the layer '" + sft.getTypeName() +
                               "' in geoserver with bounds: MinX[" + env.getMinX() + "] MinY[" +
                               env.getMinY() + "] MaxX[" + env.getMaxX() + "] MaxY[" +
                               env.getMaxY() + "]");
            System.out.println("Press <enter> to continue");
            System.in.read();
        }

        // creates and adds SimpleFeatures to the producer every few milliseconds to simulate a live stream
        // given our test data set, this will run for approximately 30 seconds
        System.out.println("Writing features to Kafka... refresh GeoServer layer preview to see changes");
        int n = 0;
        for (SimpleFeature feature: features) {
            producerFS.addFeatures(new ListFeatureCollection(sft, Collections.singletonList(feature)));

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
            if (++n % 50 == 0) {
                // LIVE CONSUMER - will obtain the current state of SimpleFeatures
                // Each poll should return 2 features, one for each entity (a, b) in
                // addition to every proximity event.
                try (SimpleFeatureIterator iterator = consumerFS.getFeatures().features()) {
                    System.out.println("Current consumer state:");
                    while (iterator.hasNext()) {
                        System.out.println(DataUtilities.encodeFeature(iterator.next()));
                    }
                }
            }
        }

        try {
            // Let streams thread finish processing
            Thread.sleep(5*1000);
            producerFS.removeFeatures(Filter.INCLUDE);
        } catch (InterruptedException e) {
            return;
        }
        System.out.println();
    }

    private void setupStreams(SimpleFeatureType sft) {
        System.out.println("Configuring Streams Topology");

        String typeName = sft.getTypeName();
        // Serde for handling the data specific to our sft type
        Serde<GeoMesaMessage> serde = builder.serde(typeName);

        // Static variables used in topology
        Integer defaultGeomIndex = sft.indexOf(sft.getGeometryDescriptor().getLocalName());
        Short numbits = 2;
        String proximityId = "proximity";
        Integer proximityDistanceMeters = 1;

        // Stream in the GeoMesa topic
        KStream<String, GeoMesaMessage> input = builder.stream(typeName);

        // Re-key and repartition the data geospatially
        KStream<String, GeoMesaMessage> geoPartioned = input
            // Filter empty and proximity messages
            .filter((k, v) -> !Objects.equals(getFID(v), "") && !getFID(v).startsWith(proximityId))
            // Re-key and re-partition the data spatially
            .selectKey(new GeoPartitioner(numbits, defaultGeomIndex));

        // Join records with others in their GeoSpatial proximity.
        KStream<String, GeoMesaMessage> proximities = geoPartioned
            .join(geoPartioned,
                (left, right) -> new Proximity(left, right, defaultGeomIndex),
                JoinWindows.of(Duration.ofMinutes(2)),
                StreamJoined.with(Serdes.String(), serde, serde))
            .filter((k, v) -> v.areDifferent() && v.getDistance() < proximityDistanceMeters)
            .mapValues(Proximity::toGeoMesaMessage)
            .selectKey((k, v) -> proximityId + UUID.randomUUID());

        // Stream the output to the same input topic for simplicity
        builder.to(typeName, proximities);

        // Build the stream topology
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfig());

        // Start the streams threads
        streams.cleanUp();
        streams.start();

        // Close streams threads when quickstart finishes
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private String getFID(GeoMesaMessage message) {
        // The FID (entityId) is the first attribute in this schema
        List<Object> attributes = message.asJava();
        if (attributes.size() > 0) {
            return attributes.get(0).toString();
        } else {
            return "";
        }
    }

    private Properties streamsConfig() {
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "test-client");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, params.get("kafka.brokers"));

        // These ensure data isn't buffered for the quickstart and shouldn't be used in production code
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // Temp directories shouldn't be used in production code
        properties.put(StreamsConfig.STATE_DIR_CONFIG, tempDir().toPath().toString());

        return properties;
    }

    @Override
    public List<Query> getTestQueries(TutorialData data) {
        // features are queried as they are written
        return Collections.emptyList();
    }

    public static void main(String[] args) {
        try {
            new KafkaStreamsQuickStart(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
