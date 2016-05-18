package com.example.geomesa.hbase;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.cli.*;
import org.geotools.data.*;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.locationtech.geomesa.utils.text.WKTUtils$;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Copyright 2016 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class HBaseQuickStart {
    static String TABLE_NAME = "bigtable.table.name".replace(".", "_");

    // sub-set of parameters that are used to create the HBase DataStore
    static String[] HBASE_CONNECTION_PARAMS = new String[]{
            TABLE_NAME
    };

    /**
     * Creates a common set of command-line options for the parser.  Each option
     * is described separately.
     */
    static Options getCommonRequiredOptions() {
        Options options = new Options();

        Option tableNameOpt = OptionBuilder.withArgName(TABLE_NAME)
                .hasArg()
                .isRequired()
                .withDescription("table name")
                .create(TABLE_NAME);
        options.addOption(tableNameOpt);

        return options;
    }

    static Map<String, Serializable> getHBaseDataStoreConf(CommandLine cmd) {
        Map<String , Serializable> dsConf = new HashMap<>();
        for (String param : HBASE_CONNECTION_PARAMS) {
            dsConf.put(param.replace("_", "."), cmd.getOptionValue(param));
        }
        return dsConf;
    }

    static SimpleFeatureType createSimpleFeatureType(String simpleFeatureTypeName)
            throws SchemaException {

        // list the attributes that constitute the feature type
        List<String> attributes = Lists.newArrayList(
                "Who:String",
                "What:java.lang.Long",     // some types require full qualification (see DataUtilities docs)
                "When:Date",               // a date-time field is optional, but can be indexed
                "*Where:Point:srid=4326",  // the "*" denotes the default geometry (used for indexing)
                "Why:String"               // you may have as many other attributes as you like...
        );

        // create the bare simple-feature type
        String simpleFeatureTypeSchema = Joiner.on(",").join(attributes);
        SimpleFeatureType simpleFeatureType =
                DataUtilities.createType(simpleFeatureTypeName, simpleFeatureTypeSchema);

        return simpleFeatureType;
    }

    static FeatureCollection createNewFeatures(SimpleFeatureType simpleFeatureType, int numNewFeatures) {
        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();

        String id;
        Object[] NO_VALUES = {};
        String[] PEOPLE_NAMES = {"Addams", "Bierce", "Clemens"};
        Long SECONDS_PER_YEAR = 365L * 24L * 60L * 60L;
        Random random = new Random(5771);
        DateTime MIN_DATE = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"));
        Double MIN_X = -78.0;
        Double MIN_Y = -39.0;
        Double DX = 2.0;
        Double DY = 2.0;

        for (int i = 0; i < numNewFeatures; i ++) {
            // create the new (unique) identifier and empty feature shell
            id = "Observation." + Integer.toString(i);
            SimpleFeature simpleFeature = SimpleFeatureBuilder.build(simpleFeatureType, NO_VALUES, id);

            // be sure to tell GeoTools explicitly that you want to use the ID you provided
            simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

            // populate the new feature's attributes

            // string value
            simpleFeature.setAttribute("Who", PEOPLE_NAMES[i % PEOPLE_NAMES.length]);

            // long value
            simpleFeature.setAttribute("What", i);

            // location:  construct a random point within a 2-degree-per-side square
            double x = MIN_X + random.nextDouble() * DX;
            double y = MIN_Y + random.nextDouble() * DY;
            Geometry geometry = WKTUtils$.MODULE$.read("POINT(" + x + " " + y + ")");

            // date-time:  construct a random instant within a year
            simpleFeature.setAttribute("Where", geometry);
            DateTime dateTime = MIN_DATE.plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR));
            simpleFeature.setAttribute("When", dateTime.toDate());

            // another string value
            // "Why"; left empty, showing that not all attributes need values

            // accumulate this new feature in the collection
            featureCollection.add(simpleFeature);
        }

        return featureCollection;
    }

    static void insertFeatures(String simpleFeatureTypeName,
                               DataStore dataStore,
                               FeatureCollection featureCollection)
            throws IOException {

        FeatureStore featureStore = (FeatureStore)dataStore.getFeatureSource(simpleFeatureTypeName);
        featureStore.addFeatures(featureCollection);
    }

    static Filter createFilter(String geomField, double x0, double y0, double x1, double y1,
                               String dateField, String t0, String t1,
                               String attributesQuery)
            throws CQLException, IOException {

        // there are many different geometric predicates that might be used;
        // here, we just use a bounding-box (BBOX) predicate as an example.
        // this is useful for a rectangular query area
        String cqlGeometry = "BBOX(" + geomField + ", " +
                x0 + ", " + y0 + ", " + x1 + ", " + y1 + ")";

        // there are also quite a few temporal predicates; here, we use a
        // "DURING" predicate, because we have a fixed range of times that
        // we want to query
        String cqlDates = "(" + dateField + " DURING " + t0 + "/" + t1 + ")";

        // there are quite a few predicates that can operate on other attribute
        // types; the GeoTools Filter constant "INCLUDE" is a default that means
        // to accept everything
        String cqlAttributes = attributesQuery == null ? "INCLUDE" : attributesQuery;

        String cql = cqlGeometry + " AND " + cqlDates  + " AND " + cqlAttributes;
        return CQL.toFilter(cql);
    }

    static void queryFeatures(String simpleFeatureTypeName,
                              DataStore dataStore,
                              String geomField, double x0, double y0, double x1, double y1,
                              String dateField, String t0, String t1,
                              String attributesQuery)
            throws CQLException, IOException {

        // construct a (E)CQL filter from the search parameters,
        // and use that as the basis for the query
        Filter cqlFilter = createFilter(geomField, x0, y0, x1, y1, dateField, t0, t1, attributesQuery);
        Query query = new Query(simpleFeatureTypeName, cqlFilter);

        // submit the query, and get back an iterator over matching features
        FeatureSource featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
        FeatureIterator featureItr = featureSource.getFeatures(query).features();

        // loop through all results
        int n = 0;
        while (featureItr.hasNext()) {
            Feature feature = featureItr.next();
            System.out.println((++n) + ".  " +
                    feature.getProperty("Who").getValue() + "|" +
                    feature.getProperty("What").getValue() + "|" +
                    feature.getProperty("When").getValue() + "|" +
                    feature.getProperty("Where").getValue() + "|" +
                    feature.getProperty("Why").getValue());
        }
        featureItr.close();
    }

    public static void main(String[] args) throws Exception {
        // find out where -- in HBase -- the user wants to store data
        CommandLineParser parser = new BasicParser();
        Options options = getCommonRequiredOptions();
        CommandLine cmd = parser.parse( options, args);

        // verify that we can see this HBase destination in a GeoTools manner
        Map<String, Serializable> dsConf = getHBaseDataStoreConf(cmd);
        DataStore dataStore = DataStoreFinder.getDataStore(dsConf);
        assert dataStore != null;

        // establish specifics concerning the SimpleFeatureType to store
        String simpleFeatureTypeName = "QuickStart";
        SimpleFeatureType simpleFeatureType = createSimpleFeatureType(simpleFeatureTypeName);

        // write Feature-specific metadata to the destination table in HBase
        // (first creating the table if it does not already exist); you only need
        // to create the FeatureType schema the *first* time you write any Features
        // of this type to the table
        System.out.println("Creating feature-type (schema):  " + simpleFeatureTypeName);
        dataStore.createSchema(simpleFeatureType);

        // create new features locally, and add them to this table
        System.out.println("Creating new features");
        FeatureCollection featureCollection = createNewFeatures(simpleFeatureType, 1000);
        System.out.println("Inserting new features");
        insertFeatures(simpleFeatureTypeName, dataStore, featureCollection);

        // query a few Features from this table
        System.out.println("Submitting query");
        queryFeatures(simpleFeatureTypeName, dataStore,
                "Where", -77.5, -37.5, -76.5, -36.5,
                "When", "2014-07-01T00:00:00.000Z", "2014-09-30T23:59:59.999Z",
                "(Who = 'Bierce')");
    }
}
