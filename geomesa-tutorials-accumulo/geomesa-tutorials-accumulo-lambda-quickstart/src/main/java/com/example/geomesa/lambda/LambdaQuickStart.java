/*
 * Copyright (c) 2016-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package com.example.geomesa.lambda;

import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Envelope;
import org.apache.commons.cli.ParseException;
import org.geomesa.example.data.TDriveData;
import org.geomesa.example.data.TutorialData;
import org.geomesa.example.quickstart.GeoMesaQuickStart;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.feature.visitor.BoundsVisitor;
import org.geotools.filter.identity.FeatureIdImpl;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
import org.locationtech.geomesa.lambda.data.LambdaDataStore;
import org.locationtech.geomesa.lambda.data.LambdaDataStoreFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LambdaQuickStart extends GeoMesaQuickStart {

    // uses t-dive streaming data
    public LambdaQuickStart(String[] args) throws ParseException {
        super(args, new LambdaDataStoreFactory().getParametersInfo(), new TDriveData());
    }

    @Override
    public void createSchema(DataStore datastore, SimpleFeatureType sft) throws IOException {
        String typeName = sft.getTypeName();
        if (datastore.getSchema(typeName) != null) {
            System.out.println("'" + typeName + "' feature type already exists - deleting existing schema");
            datastore.removeSchema(typeName);
            System.out.println("Please re-run quick start");
            throw new RuntimeException("Deleted existing scheme, please re-run quick start");
        }
        super.createSchema(datastore, sft);
    }

    @Override
    public void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features)
          throws IOException {
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

        // creates and adds SimpleFeatures to the producer every few milliseconds to simulate a live stream
        // given our test data set, this will run for approximately one minute
        System.out.println("Writing features to Kafka... refresh GeoServer layer preview to see changes");

        // track the number of unique features
        Set<String> ids = new HashSet<>();

        // use try-with-resources to ensure the writer is closed
        try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                   datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
            for (SimpleFeature feature : features) {
                // using a geotools writer, you have to get a feature, modify it, then commit it
                // appending writers will always return 'false' for haveNext, so we don't need to bother checking
                SimpleFeature toWrite = writer.next();
                // copy attributes
                toWrite.setAttributes(feature.getAttributes());
                // updating the feature ID requires casting to an implementation class
                // alternatively, you can use the PROVIDED_FID hint in the user data
                ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
                ids.add(feature.getID());
                // make sure to copy the user data, if there is any
                toWrite.getUserData().putAll(feature.getUserData());
                // write the feature
                writer.write();

                try {
                    Thread.sleep(15);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
        System.out.println("Wrote " + features.size() + " features");
        System.out.println();

        System.out.println("Waiting for expiry and persistence...");

        int count = ids.size();
        LambdaDataStore ds = (LambdaDataStore) datastore;
        long total = 0, persisted = 0;
        do {
            long newTotal = (long) ds.stats().getCount(sft, Filter.INCLUDE, true, new Hints()).get();
            long newPersisted =
                  (long) ((AccumuloDataStore) ds.persistence()).stats().getCount(sft, Filter.INCLUDE, true, new Hints()).get();
            if (newTotal != total || newPersisted != persisted) {
                total = newTotal;
                persisted = newPersisted;
                System.out.println("Total features: " + total + ", features persisted to Accumulo: " + persisted);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
        } while (persisted < count || total > count);
    }

    @Override
    public List<Query> getTestQueries(TutorialData data) {
        // this tutorial doesn't cover querying, but it would be the same as with any datastore
        return Collections.emptyList();
    }

    public static void main(String[] args) {
        // sets the delay between receiving an update and persisting it to Accumulo
        System.setProperty("geomesa.lambda.persist.interval", "2s");
        try {
            new LambdaQuickStart(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
