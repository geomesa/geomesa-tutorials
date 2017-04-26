/***********************************************************************
 * Copyright (c) 2014-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.example.geomesa.avro;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Geometry;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.locationtech.geomesa.features.avro.AvroDataFileReader;
import org.locationtech.geomesa.features.avro.AvroDataFileWriter;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Random;

public class AvroExample {

    public static void main(String[] args) throws Exception {

        String simpleFeatureTypeName = "AvroExample";
        SimpleFeatureType simpleFeatureType = createSimpleFeatureType(simpleFeatureTypeName);

        System.out.println("Creating 10 features.");
        SimpleFeatureCollection featureCollection = createNewFeatures(simpleFeatureType, 10);

        System.out.println("Writing features to Avro binary format.");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        AvroDataFileWriter writer = new AvroDataFileWriter(baos, simpleFeatureType, -1);
        writer.append(featureCollection);
        writer.flush();

        System.out.println("Reading features from Avro binary format.");
        InputStream inputStream = new ByteArrayInputStream(baos.toByteArray());
        AvroDataFileReader reader = new AvroDataFileReader(inputStream);

        int n = 0;
        while (reader.hasNext()) {
            SimpleFeature feature = reader.next();
            System.out.println((++n) + ".  " +
                    feature.getProperty("Who").getValue() + "|" +
                    feature.getProperty("What").getValue() + "|" +
                    feature.getProperty("When").getValue() + "|" +
                    feature.getProperty("Where").getValue() + "|" +
                    feature.getProperty("Why").getValue());

        }
    }

    static SimpleFeatureType createSimpleFeatureType(String simpleFeatureTypeName)
            throws SchemaException {

        // list the attributes that constitute the feature type
        List<String> attributes = Lists.newArrayList(
                "Who:String:index=full",
                "What:java.lang.Long",     // some types require full qualification (see DataUtilities docs)
                "When:Date",               // a date-time field is optional, but can be indexed
                "*Where:Point:srid=4326",  // the "*" denotes the default geometry (used for indexing)
                "Why:String"               // you may have as many other attributes as you like...
        );

        // create the bare simple-feature type
        String simpleFeatureTypeSchema = Joiner.on(",").join(attributes);
        SimpleFeatureType simpleFeatureType =
                SimpleFeatureTypes.createType(simpleFeatureTypeName, simpleFeatureTypeSchema);

        return simpleFeatureType;
    }

    static SimpleFeatureCollection createNewFeatures(SimpleFeatureType simpleFeatureType, int numNewFeatures) {
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
            Geometry geometry = WKTUtils.read("POINT(" + x + " " + y + ")");

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
}
