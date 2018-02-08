/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.quickstart;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.sort.SortBy;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class GeoMesaQuickStart implements Runnable {

    private Map<String, String> params;
    private boolean cleanup = false;

    public GeoMesaQuickStart(Map<String, String> params) {
        this.params = params;
    }

    public GeoMesaQuickStart(String[] args, Param[] parameters) throws ParseException {
        // parse the data store parameters from the command line
        Options options = new Options();
        for (Param p: parameters) {
            if (!p.isDeprecated()) {
                Option opt = Option.builder(null)
                                   .longOpt(p.getName())
                                   .argName(p.getName())
                                   .hasArg()
                                   .desc(p.getDescription().toString())
                                   .required(p.isRequired())
                                   .build();
                options.addOption(opt);
            }
        }
        options.addOption(Option.builder().longOpt("cleanup").desc("Delete tables after running").build());
        addCustomOptions(options);

        CommandLine cmd;
        try {
            cmd = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(getClass().getName(), options);
            throw e;
        }

        params = new HashMap<>();
        // noinspection unchecked
        for (Option opt: options.getOptions()) {
            String value = cmd.getOptionValue(opt.getLongOpt());
            if (value != null) {
                params.put(opt.getArgName(), value);
            }
        }

        cleanup = cmd.hasOption("cleanup");
    }

    public abstract QuickStartData getData();

    protected void addCustomOptions(Options options) {
        // default implementation does nothing
    }

    @Override
    public void run() {
        DataStore datastore = null;
        try {
            QuickStartData data = getData();

            datastore = createDataStore(params);
            SimpleFeatureType sft = createSchema(datastore, data);

            System.out.println("Generating test data");
            List<SimpleFeature> features = data.getTestData();
            System.out.println();

            writeFeatures(datastore, sft, features);

            queryFeatures(datastore, data.getTestQueries());
        } catch (Exception e) {
            throw new RuntimeException("Error running quickstart:", e);
        } finally {
            cleanup(datastore, cleanup);
        }
    }

    public DataStore createDataStore(Map<String, String> params) throws IOException {
        System.out.println("Loading datastore");

        // use geotools service loading to get a datastore instance
        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        System.out.println();
        return datastore;
    }

    public SimpleFeatureType createSchema(DataStore datastore, QuickStartData data) throws IOException {
        SimpleFeatureType sft = data.getSimpleFeatureType();
        System.out.println("Creating schema: " + DataUtilities.encodeType(sft));
        datastore.createSchema(sft);
        System.out.println();
        return sft;
    }

    public void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        System.out.println("Writing test data");
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
                // make sure to copy the user data, if there is any
                toWrite.getUserData().putAll(feature.getUserData());
                // write the feature
                writer.write();
            }
        }
        System.out.println("Wrote " + features.size() + " features");
        System.out.println();
    }

    public void queryFeatures(DataStore datastore, List<Query> queries) throws IOException {
        System.out.println("Running test queries");
        for (Query query : queries) {
            System.out.println("Running query " + ECQL.toCQL(query.getFilter()));
            if (query.getPropertyNames() != null) {
                System.out.println("Returning attributes " + Arrays.asList(query.getPropertyNames()));
            }
            if (query.getSortBy() != null) {
                SortBy sort = query.getSortBy()[0];
                System.out.println("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
            }
            // submit the query, and get back an iterator over matching features
            // use try-with-resources to ensure the reader is closed
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                     datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                // loop through all results, only print out the first 10
                int n = 0;
                while (reader.hasNext()) {
                    SimpleFeature feature = reader.next();
                    if (n++ < 10) {
                        // use geotools data utilities to get a printable string
                        System.out.println(String.format("%02d", n) + " " + DataUtilities.encodeFeature(feature));
                    } else if (n == 10) {
                        System.out.println("...");
                    }
                }
                System.out.println();
                System.out.println("Returned " + n + " total features");
                System.out.println();
            }
        }
    }

    public void cleanup(DataStore datastore, Boolean cleanup) {
        if (datastore != null) {
            try {
                if (cleanup) {
                    if (datastore instanceof GeoMesaDataStore) {
                        System.out.println("Cleaning up test data");
                        ((GeoMesaDataStore) datastore).delete();
                    } else {
                        System.out.println("Can't cleanup datastore of type " + datastore.getClass().getName());
                    }
                }
            } finally {
                // make sure that we dispose of the datastore when we're done with it
                datastore.dispose();
            }
        }
        System.out.println("Done");
    }
}
