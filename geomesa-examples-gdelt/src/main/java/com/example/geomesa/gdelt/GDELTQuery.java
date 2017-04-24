/***********************************************************************
 * Copyright (c) 2014-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.example.geomesa.gdelt;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * GDELTQuery demonstrates how to use ECQL to query
 * the GDELT data stored in GeoMesa
 */
public class GDELTQuery {

    public static void main(String[] args) {
        try {
            queryGdelt(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void queryGdelt(String[] args) throws Exception {
        CommandLineParser parser = new BasicParser();
        Options options = GDELTIngest.getCommonRequiredOptions();

        CommandLine cmd = parser.parse( options, args);
        Map<String, String> dsConf = GDELTIngest.getAccumuloDataStoreConf(cmd);

        String featureName = cmd.getOptionValue(GDELTIngest.FEATURE_NAME);

        // First, we get a handle to the data store and feature source
        DataStore ds = DataStoreFinder.getDataStore(dsConf);
        SimpleFeatureSource fs = ds.getFeatureSource(featureName);

        // Next, we form a query.  We need a FilterFactory2 to build
        // up our query
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();

        // We want to query for events in Ukraine during the
        // civil unrest.  We'll start by looking at January and February
        // of 2014
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date start = df.parse("2014-01-01");
        Date end = df.parse("2014-02-28");
        Filter timeFilter = ff.between(ff.property("SQLDATE"), ff.literal(start), ff.literal(end));

        // We need to bound our query spatially to Ukraine
        Filter spatialFilter = ff.bbox("geom", 22.1371589, 44.386463, 40.228581, 52.379581, "EPSG:4326");

        // Now we can combine our time filter and our spatial filter using a boolean and operator
        Filter conjunction = ff.and(timeFilter, spatialFilter);

        // Let's constrain the results to just the date/time, location, and Actor names
        Query query = new Query("gdelt", conjunction, new String[]{ "SQLDATE", "geom", "Actor1Name", "Actor2Name" });

        // Finally, we submit our query to GeoMesa
        SimpleFeatureCollection results = fs.getFeatures(query);

        System.out.println("Number of results = " + results.size());

        // We can add an attribute filter
        Filter actor1Protest = ff.like(ff.property("Actor1Name"), "PROTEST%");
        Filter spatialTemporalAttribute = ff.and(conjunction, actor1Protest);
        Query query2 = new Query("gdelt", spatialTemporalAttribute, new String[]{ "SQLDATE", "geom", "Actor1Name", "Actor2Name" });
        SimpleFeatureCollection results2 = fs.getFeatures(query2);

        System.out.println("Number of results = " + results2.size());

        Filter eventRootCodeThreaten =  ff.equal(ff.property("EventRootCode"), ff.literal(13), false);
        Filter spatialTemporalAttribute2 = ff.and(conjunction, eventRootCodeThreaten);
        Query query3 = new Query("gdelt", spatialTemporalAttribute2, new String[]{ "SQLDATE", "geom", "Actor1Name", "Actor2Name", "EventRootCode", "EventBaseCode" });
        SimpleFeatureCollection results3 = fs.getFeatures(query3);

        System.out.println("Number of results = " + results3.size());
    }
}
