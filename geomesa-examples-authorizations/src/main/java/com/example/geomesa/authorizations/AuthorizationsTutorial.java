package com.example.geomesa.authorizations;

import com.example.geomesa.authorizations.GdeltFeature.Attributes;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureStore;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.filter.text.cql2.CQLException;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
import org.locationtech.geomesa.security.AuthorizationsProvider;
import org.locationtech.geomesa.security.DefaultAuthorizationsProvider;
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

public class AuthorizationsTutorial {

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
        calendar.set(Calendar.YEAR, 2013);
        calendar.set(Calendar.MONTH, Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        Date start = calendar.getTime();

        calendar.set(Calendar.YEAR, 2014);
        calendar.set(Calendar.MONTH, Calendar.APRIL);
        calendar.set(Calendar.DAY_OF_MONTH, 30);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        Date end = calendar.getTime();
//        2013-01-01T00:00:00.000Z/2014-04-30T23:00:00.000Z
        Filter timeFilter =
                ff.between(ff.property(GdeltFeature.Attributes.SQLDATE.getName()),
                           ff.literal(start),
                           ff.literal(end));

        // We'll bound our query spatially to Ukraine
        Filter spatialFilter =
                ff.bbox(GdeltFeature.Attributes.geom.getName(),
                        31.6, 44, 37.4, 47.75,
                        "EPSG:4326");

        // we'll also restrict our query to only articles about the US, UK or UN
        Filter attributeFilter = ff.like(ff.property(GdeltFeature.Attributes.Actor1Name.getName()),
                                         "UNITED%");

        // Now we can combine our filters using a boolean AND operator
        Filter conjunction = ff.and(Arrays.asList(timeFilter, spatialFilter, attributeFilter));

        return conjunction;
    }

    /**
     * Executes a basic bounding box query
     *
     * @param simpleFeatureTypeName
     * @param dataStore
     *
     * @throws IOException
     * @throws CQLException
     */
    static void executeQuery(String simpleFeatureTypeName, DataStore dataStore)
            throws IOException, CQLException {

        // start with our basic filter to narrow the results
        Filter cqlFilter = createBaseFilter();

        // use the 2-arg constructor for the query - this will not restrict the attributes returned
        Query query = new Query(simpleFeatureTypeName, cqlFilter);

        // get the feature store used to query the GeoMesa data
        FeatureStore featureStore = (FeatureStore) dataStore.getFeatureSource(simpleFeatureTypeName);

        // execute the query
        FeatureCollection results = featureStore.getFeatures(query);

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
        // read command line options - this contains the connection to accumulo and the table to query
        CommandLineParser parser = new BasicParser();
        Options options = SetupUtil.getGeomesaDataStoreOptions();
        CommandLine cmd = parser.parse(options, args);

        // verify that we can see this Accumulo destination in a GeoTools manner
        Map<String, String> dsConf = SetupUtil.getAccumuloDataStoreConf(cmd);

        // get an instance of the data store that uses the default authorizations provider, which will use whatever auths the connector has available
        System.setProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY,
                           DefaultAuthorizationsProvider.class.getName());
        DataStore authDataStore = DataStoreFinder.getDataStore(dsConf);
        assert authDataStore != null;

        // get another instance of the data store that uses our authorizations provider that always returns empty auths
        System.setProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY,
                           EmptyAuthorizationsProvider.class.getName());
        DataStore noAuthDataStore = DataStoreFinder.getDataStore(dsConf);

        // create the simple feature type for our test
        String simpleFeatureTypeName = cmd.getOptionValue(SetupUtil.FEATURE_NAME);
        SimpleFeatureType simpleFeatureType = GdeltFeature.buildGdeltFeatureType(
                simpleFeatureTypeName);

        // execute the query, with and without visibilities
        System.out.println("\nExecuting query with AUTHORIZED data store: auths are '"
                           + ((AccumuloDataStore) authDataStore).config().authProvider()
                                                                .getAuthorizations() + "'");
        executeQuery(simpleFeatureTypeName, authDataStore);
        System.out.println("Executing query with UNAUTHORIZED data store: auths are '"
                           + ((AccumuloDataStore) noAuthDataStore).config().authProvider()
                                                                  .getAuthorizations() + "'");
        executeQuery(simpleFeatureTypeName, noAuthDataStore);
    }


}