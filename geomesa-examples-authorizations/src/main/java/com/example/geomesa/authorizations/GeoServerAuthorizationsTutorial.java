package com.example.geomesa.authorizations;

import com.example.geomesa.authorizations.GdeltFeature.Attributes;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.Query;
import org.geotools.data.wfs.WFSDataStore;
import org.geotools.data.wfs.WFSDataStoreFactory;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.Feature;
import org.opengis.feature.Property;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.referencing.FactoryException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright 2014 Commonwealth Computer Research, Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class GeoServerAuthorizationsTutorial {

    /**
     * Creates a base filter that will return a small subset of our results. This can be tweaked to
     * return different results if desired. Currently it should return 16 results.
     *
     * @return
     *
     * @throws org.geotools.filter.text.cql2.CQLException
     * @throws java.io.IOException
     */
    static Filter createBaseFilter()
            throws CQLException, IOException {

        // Get a FilterFactory2 to build up our query
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();

        // We are going to query for events in Ukraine during the
        // civil unrest.

        // We'll start by looking at a particular time frame
        Filter timeFilter =
                ff.between(ff.property(Attributes.SQLDATE.getName()),
                           ff.literal("2013-01-01T05:00:00.000Z"),
                           ff.literal("2014-04-30T23:00:00.000Z"));

        // We'll bound our query spatially to Ukraine
        Filter spatialFilter =
                ff.bbox(Attributes.geom.getName(),
                        31.6, 44, 37.4, 47.75,
                        "EPSG:4326");

        // we'll also restrict our query to only articles about the US, UK or UN
        Filter attributeFilter = ff.like(ff.property(Attributes.Actor1Name.getName()), "UNITED%");

        // Now we can combine our filters using a boolean AND operator
        Filter conjunction = ff.and(Arrays.asList(timeFilter, spatialFilter, attributeFilter));

        return conjunction;
    }

    /**
     * Executes a basic bounding box query
     *
     * @param typeName
     * @param dataStore
     *
     * @throws java.io.IOException
     * @throws org.geotools.filter.text.cql2.CQLException
     */
    static void executeQuery(String typeName, DataStore dataStore)
            throws IOException, CQLException, FactoryException {

        // start with our basic filter to narrow the results
        Filter cqlFilter = createBaseFilter();

        // use the 2-arg constructor for the query - this will not restrict the attributes returned
        Query query = new Query(typeName, cqlFilter, new String[] {"geom"});

        // restrict the max features coming back for performance
        query.setMaxFeatures(10);

        // get the feature store used to query the GeoMesa data
        FeatureSource featureSource = dataStore.getFeatureSource(
                typeName);

        // execute the query
        FeatureCollection results = featureSource.getFeatures(query);

        // loop through all results
        FeatureIterator iterator = results.features();
        try {
            printResults(iterator, Attributes.geom.getName());
        } finally {
            iterator.close();
        }
    }

    /**
     * Iterates through the given iterator and prints out the properties (attributes) for each entry.
     *
     * @param iterator
     */
    private static void printResults(FeatureIterator iterator, String... attributes) {

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

            for (String attribute : attributes) {
                Property property = feature.getProperty(attribute);
                result.append("|")
                      .append(property.getName())
                      .append('=')
                      .append(property.getValue());
            }
            System.out.println(result.toString());
        }
        System.out.println();
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
        // read command line options - this contains the path to geoserver and the data store to query
        CommandLineParser parser = new BasicParser();
        Options options = SetupUtil.getWfsOptions();
        CommandLine cmd = parser.parse(options, args);

        String geoserverHost = cmd.getOptionValue(SetupUtil.GEOSERVER_URL);
        if (!geoserverHost.endsWith("/")) {
            geoserverHost += "/";
        }

        // create the URL to GeoServer. Note that we need to point to the 'GetCapabilities' request,
        // and that we are using WFS version 1.0.0
        String geoserverUrl = geoserverHost + "wfs?request=GetCapabilities&version=1.0.0";

        // create the geotools configuration for a WFS data store
        Map<String, String> configuration = new HashMap<String, String>();
        configuration.put(WFSDataStoreFactory.URL.key, geoserverUrl);
        configuration.put(WFSDataStoreFactory.WFS_STRATEGY.key, "geoserver");
        configuration.put(WFSDataStoreFactory.TIMEOUT.key, cmd.getOptionValue(SetupUtil.TIMEOUT, "99999"));

        System.out.println("Executing query against '" + geoserverHost +
                           "' with client keystore '" + System.getProperty("javax.net.ssl.keyStore") +
                           "'");

        // verify we have gotten the correct datastore
        WFSDataStore wfsDataStore = (WFSDataStore) DataStoreFinder.getDataStore(configuration);
        assert wfsDataStore != null;

        // the geoserver data store to query
        String geoserverDataStore = cmd.getOptionValue(SetupUtil.FEATURE_STORE);

        executeQuery(geoserverDataStore, wfsDataStore);
    }


}