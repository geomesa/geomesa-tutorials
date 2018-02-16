/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.kafka;

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
import org.geotools.data.FeatureEvent;
import org.geotools.data.FeatureListener;
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory;
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent.KafkaFeatureChanged;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class KafkaListener implements Runnable {

    private Map<String, String> params;

    public KafkaListener(Map<String, String> params) {
        this.params = params;
    }

    public KafkaListener(String[] args) throws ParseException {
        // parse the data store parameters from the command line
        Options options = new Options();
        for (Param p: new KafkaDataStoreFactory().getParametersInfo()) {
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
    }

    @Override
    public void run() {
        DataStore datastore = null;
        Map<String, FeatureListener> listeners = new HashMap<>();

        try {
            // this instance is a consumer
            params.put("kafka.consumer.count", "1");
            datastore = createDataStore(params);

            for (String typeName: datastore.getTypeNames()) {
                System.out.println("Registering a feature listener for schema '" + typeName + "'");
                FeatureListener listener = featureEvent -> {
                    System.out.println("Received FeatureEvent from schema '" + typeName + "' of type '" + featureEvent.getType() + "'");
                    if (featureEvent.getType() == FeatureEvent.Type.CHANGED &&
                        featureEvent instanceof KafkaFeatureChanged) {
                        System.out.println(DataUtilities.encodeFeature(((KafkaFeatureChanged) featureEvent).feature()));
                    } else if (featureEvent.getType() == FeatureEvent.Type.REMOVED) {
                        System.out.println("Received Delete for filter: " + featureEvent.getFilter());
                    }
                };
                datastore.getFeatureSource(typeName).addFeatureListener(listener);
                listeners.put(typeName, listener);
            }

            while (true) {
                // Wait for user to terminate with ctrl-c
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            System.out.println("Done");
        } catch (Exception e) {
            throw new RuntimeException("Error running listener:", e);
        } finally {
            if (datastore != null) {
                // make sure that we unregister any listeners
                for (Entry<String, FeatureListener> entry: listeners.entrySet()) {
                    try {
                        datastore.getFeatureSource(entry.getKey()).removeFeatureListener(entry.getValue());
                    } catch (IOException e) {
                        System.err.println("Exception removing feature listener: " + e.toString());
                    }
                }
                datastore.dispose();
            }
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

    public static void main(String[] args) {
        try {
            new KafkaListener(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
