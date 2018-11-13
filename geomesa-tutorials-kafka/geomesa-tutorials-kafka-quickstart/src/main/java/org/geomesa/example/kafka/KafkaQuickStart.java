/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.kafka;

import org.locationtech.jts.geom.Envelope;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geomesa.example.data.TDriveData;
import org.geomesa.example.data.TutorialData;
import org.geomesa.example.quickstart.GeoMesaQuickStart;
import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.Query;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.visitor.BoundsVisitor;
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaQuickStart extends GeoMesaQuickStart {

    private DataStore consumer = null;
    private boolean wait = true;

    // uses t-dive streaming data
    public KafkaQuickStart(String[] args) throws ParseException {
        super(args, new KafkaDataStoreFactory().getParametersInfo(), new TDriveData());
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

        params.put("kafka.consumer.count", "1");
        consumer = super.createDataStore(params);

        return producer;
    }

    @Override
    public void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        // the live consumer must be created before the producer writes features
        // in order to read streaming data.
        // i.e. the live consumer will only read data written after its instantiation
        SimpleFeatureSource consumerFS = consumer.getFeatureSource(sft.getTypeName());
        SimpleFeatureStore producerFS = (SimpleFeatureStore) datastore.getFeatureSource(sft.getTypeName());

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
                Thread.sleep(15);
            } catch (InterruptedException e) {
                return;
            }
            if (++n % 200 == 0) {
                // LIVE CONSUMER - will obtain the current state of SimpleFeatures
                // there should only be a single feature at one time
                try (SimpleFeatureIterator iterator = consumerFS.getFeatures().features()) {
                    System.out.println("Current consumer state:");
                    while (iterator.hasNext()) {
                        System.out.println(DataUtilities.encodeFeature(iterator.next()));
                    }
                }
            }
        }

        System.out.println();
    }

    @Override
    public List<Query> getTestQueries(TutorialData data) {
        // features are queried as they are written
        return Collections.emptyList();
    }

    public static void main(String[] args) {
        try {
            new KafkaQuickStart(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
