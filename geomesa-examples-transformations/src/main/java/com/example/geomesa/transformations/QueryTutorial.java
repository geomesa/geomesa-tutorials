/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.example.geomesa.transformations;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.Feature;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class QueryTutorial {

    private static final String FEATURE_NAME_ARG = "featureName";

    /**
     * Creates a base filter that will return a small subset of our results. This can be tweaked to
     * return different results if desired. Currently it should return 16 results.
     *
     * @return
     *
     * @throws CQLException
     * @throws IOException
     */
    static Filter createBaseFilter()
            throws CQLException, IOException {

        // Get a FilterFactory2 to build up our query
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();

        // We are going to query for events in Ukraine during the
        // civil unrest.

        // We'll start by looking at a particular day in February of 2014
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(Calendar.YEAR, 2014);
        calendar.set(Calendar.MONTH, Calendar.FEBRUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 2);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        Date start = calendar.getTime();

        calendar.set(Calendar.HOUR_OF_DAY, 23);
        Date end = calendar.getTime();

        Filter timeFilter =
                ff.between(ff.property(GdeltFeature.Attributes.SQLDATE.getName()),
                           ff.literal(start),
                           ff.literal(end));

        // We'll bound our query spatially to Ukraine
        Filter spatialFilter =
                ff.bbox(GdeltFeature.Attributes.geom.getName(),
                        22.1371589,
                        44.386463,
                        40.228581,
                        52.379581,
                        "EPSG:4326");

        // we'll also restrict our query to only articles about the US, UK or UN
        Filter attributeFilter = ff.like(ff.property(GdeltFeature.Attributes.Actor1Name.getName()),
                                         "UNITED%");

        // Now we can combine our filters using a boolean AND operator
        Filter conjunction = ff.and(Arrays.asList(timeFilter, spatialFilter, attributeFilter));

        return conjunction;
    }

    /**
     * Executes a basic bounding box query without any projections.
     *
     * @param simpleFeatureTypeName
     * @param featureSource
     *
     * @throws IOException
     * @throws CQLException
     */
    static void basicQuery(String simpleFeatureTypeName, FeatureSource featureSource)
            throws IOException, CQLException {

        System.out.println("Submitting basic query with no projections\n");

        // start with our basic filter to narrow the results
        Filter cqlFilter = createBaseFilter();

        // use the 2-arg constructor for the query - this will not restrict the attributes returned
        Query query = new Query(simpleFeatureTypeName, cqlFilter);

        // execute the query
        FeatureCollection results = featureSource.getFeatures(query);

        // loop through all results
        FeatureIterator iterator = results.features();
        try {
            printResults(iterator);
        } finally {
            iterator.close();
        }
    }

    /**
     * Executes a query that restricts the attributes coming back.
     *
     * @param simpleFeatureTypeName
     * @param featureSource
     *
     * @throws IOException
     * @throws CQLException
     */
    static void basicProjectionQuery(String simpleFeatureTypeName, FeatureSource featureSource)
            throws IOException, CQLException {
        System.out.println("Submitting basic projection query");

        // start with our basic filter to narrow the results
        Filter cqlFilter = createBaseFilter();

        // define the properties (attributes) that we want returned as a string array
        // each element of the array is a property name we want returned
        String[] properties = new String[] {GdeltFeature.Attributes.Actor1Name.getName(),
                                            GdeltFeature.Attributes.geom.getName()};

        // create the query - we use the extended constructor to pass in our projection
        Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

        // execute the query
        FeatureCollection results = featureSource.getFeatures(query);

        // loop through all results
        FeatureIterator iterator = results.features();
        try {
            printResults(iterator);
        } finally {
            iterator.close();
        }
    }

    /**
     * Executes a query that transforms the results coming back to say 'hello' to each result.
     *
     * @param simpleFeatureTypeName
     * @param featureSource
     *
     * @throws IOException
     * @throws CQLException
     */
    static void basicTransformationQuery(String simpleFeatureTypeName, FeatureSource featureSource)
            throws IOException, CQLException {
        System.out.println("Submitting basic tranformation query");

        // start with our basic filter to narrow the results
        Filter cqlFilter = createBaseFilter();

        // define the properties that we want returned
        // this also allows us to manipulate properties using various GeoTools transforms.
        // In this case, we are using a string concatenation to say 'hello' to our results. We
        // are overwriting the existing field with the results of the transform.
        String[] properties = new String[] {GdeltFeature.Attributes.Actor1Name.getName() + "=strConcat('hello '," +
                                            "" + GdeltFeature.Attributes.Actor1Name.getName() +
                                            ")", GdeltFeature.Attributes.geom.getName()};

        // create the query - we use the extended constructor to pass in our transform
        Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

        // execute the query
        FeatureCollection results = featureSource.getFeatures(query);

        // loop through all results
        FeatureIterator iterator = results.features();
        try {
            printResults(iterator);
        } finally {
            iterator.close();
        }
    }

    /**
     * Executes a query that returns a new dynamic field name created by transforming a field.
     *
     * @param simpleFeatureTypeName
     * @param featureSource
     *
     * @throws IOException
     * @throws CQLException
     */
    static void renamedTransformationQuery(String simpleFeatureTypeName,
                                           FeatureSource featureSource)
            throws IOException, CQLException {
        System.out.println("Submitting renaming tranformation query");

        // start with our basic filter to narrow the results
        Filter cqlFilter = createBaseFilter();

        // define the properties that we want returned
        // this also allows us to manipulate properties using various GeoTools transforms.
        // In this case, we are using a string concatenation to say 'hello' to our results. We are
        // storing the result of the transform in a new dynamic field, called 'derived'. We also
        // return the original attribute unchanged.
        String[] properties =
                new String[] {GdeltFeature.Attributes.Actor1Name.getName(),
                              "derived=strConcat('hello '," +
                              GdeltFeature.Attributes.Actor1Name + ")",
                              GdeltFeature.Attributes.geom.getName()};

        // create the query - we use the extended constructor to pass in our transform
        Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

        // execute the query
        FeatureCollection results = featureSource.getFeatures(query);

        // loop through all results
        FeatureIterator iterator = results.features();
        try {
            printResults(iterator, "derived");
        } finally {
            iterator.close();
        }
    }

    /**
     * Executes a query with a transformation on multiple fields.
     *
     * @param simpleFeatureTypeName
     * @param featureSource
     *
     * @throws IOException
     * @throws CQLException
     */
    static void mutliFieldTransformationQuery(String simpleFeatureTypeName,
                                              FeatureSource featureSource)
            throws IOException, CQLException {
        System.out.println("Submitting mutli-field tranformation query");

        // start with our basic filter to narrow the results
        Filter cqlFilter = createBaseFilter();

        // define the properties that we want returned
        // this also allows us to manipulate properties using various GeoTools transforms.
        // In this case, we are concatenating two different attributes.
        String[] properties = new String[] {"derived=strConcat(strConcat(" +
                                            GdeltFeature.Attributes.Actor1Name + ",' - ')," + GdeltFeature.Attributes.Actor1Geo_FullName +
                                            ")", GdeltFeature.Attributes.geom.getName()};

        // create the query - we use the extended constructor to pass in our transform
        Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

        // execute the query
        FeatureCollection results = featureSource.getFeatures(query);

        // loop through all results
        FeatureIterator iterator = results.features();
        try {
            printResults(iterator, "derived");
        } finally {
            iterator.close();
        }
    }

    /**
     * Executes a query that performs a geometric function transform on the result set.
     *
     * @param simpleFeatureTypeName
     * @param featureSource
     *
     * @throws IOException
     * @throws CQLException
     */
    static void geometricTransformationQuery(String simpleFeatureTypeName,
                                             FeatureSource featureSource)
            throws IOException, CQLException {
        System.out.println("Submitting geometric tranformation query");

        // start with our basic filter to narrow the results
        Filter cqlFilter = createBaseFilter();

        // define the properties that we want returned
        // this also allows us to manipulate properties using various GeoTools transforms.
        // In this case, we are buffering the point to create a polygon. The transformed field gets
        // renamed to 'derived'.
        String[] properties = new String[] {GdeltFeature.Attributes.geom.getName(),
                                            "derived=buffer(" + GdeltFeature.Attributes.geom.getName() +
                                            ", 2)"};

        // create the query - we use the extended constructor to pass in our transform
        Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);

        // execute the query
        FeatureCollection results = featureSource.getFeatures(query);

        // loop through all results
        FeatureIterator iterator = results.features();
        try {
            printResults(iterator, "derived");
        } finally {
            iterator.close();
        }
    }

    /**
     * Iterates through the given iterator and prints out the properties (attributes) for each entry.
     *
     * @param iterator
     */
    private static void printResults(FeatureIterator iterator, String... derivedAttributes) {

        if (iterator.hasNext()) {
            System.out.println("Results:");
        } else {
            System.out.println("No results");
        }
        int n = 0;
        while (iterator.hasNext()) {
            Feature feature = iterator.next();
            StringBuilder result = new StringBuilder();
            result.append(++n);

            for (GdeltFeature.Attributes attribute : GdeltFeature.Attributes.values()) {
                try {
                    Property property = feature.getProperty(attribute.getName());
                    appendResult(result, property);
                } catch (Exception e) {
                    // GEOMESA-280 - currently asking for non-existing properties throws an NPE
                }
            }
            for (String derivedAttibute : derivedAttributes) {
                Property property = feature.getProperty(derivedAttibute);
                appendResult(result, property);
            }
            System.out.println(result.toString());
        }
        System.out.println();
    }

    /**
     * Append the property to the result
     *
     * @param string
     * @param property
     */
    private static void appendResult(StringBuilder string, Property property) {
        if (property != null) {
            string.append("|")
                  .append(property.getName())
                  .append('=')
                  .append(property.getValue());
        }
    }

    /**
     * Main entry point. Executes queries against an existing GDELT dataset.
     *
     * @param args
     *
     * @throws Exception
     */
    public static void main(String[] args)
            throws Exception {
        // read command line options - this contains the connection to accumulo and the table to query
        CommandLineParser parser = new BasicParser();
        Options options = SetupUtil.getCommonRequiredOptions();
        options.addOption(OptionBuilder.withArgName(FEATURE_NAME_ARG).hasArg().isRequired()
                                       .withDescription(
                                               "the FeatureTypeName used to store the GDELT data, e.g.:  gdelt")
                                       .create(FEATURE_NAME_ARG));
        CommandLine cmd = parser.parse(options, args);

        // verify that we can see this Accumulo destination in a GeoTools manner
        Map<String, String> dsConf = SetupUtil.getAccumuloDataStoreConf(cmd);
        //Disable states collection
        dsConf.put("collectStats", "false");
        DataStore dataStore = DataStoreFinder.getDataStore(dsConf);
        assert dataStore != null;

        // create the simple feature type for our test
        String simpleFeatureTypeName = cmd.getOptionValue(FEATURE_NAME_ARG);
        SimpleFeatureType simpleFeatureType = GdeltFeature.buildGdeltFeatureType(
                simpleFeatureTypeName);

        // get the feature store used to query the GeoMesa data
        FeatureStore featureStore = (FeatureStore) dataStore.getFeatureSource(simpleFeatureTypeName);

        // execute some queries
        basicQuery(simpleFeatureTypeName, featureStore);
        basicProjectionQuery(simpleFeatureTypeName, featureStore);
        basicTransformationQuery(simpleFeatureTypeName, featureStore);
        renamedTransformationQuery(simpleFeatureTypeName, featureStore);
        mutliFieldTransformationQuery(simpleFeatureTypeName, featureStore);
        geometricTransformationQuery(simpleFeatureTypeName, featureStore);

        // the list of available transform functions is available here:
        // http://docs.geotools.org/latest/userguide/library/main/filter.html - scroll to 'Function List'
    }


}