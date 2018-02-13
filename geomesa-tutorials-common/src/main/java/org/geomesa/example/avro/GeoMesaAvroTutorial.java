/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.avro;

import org.apache.commons.cli.ParseException;
import org.geomesa.example.data.GDELTData;
import org.geomesa.example.data.TutorialData;
import org.geomesa.example.quickstart.GeoMesaQuickStart;
import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.locationtech.geomesa.features.avro.AvroDataFileReader;
import org.locationtech.geomesa.features.avro.AvroDataFileWriter;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public abstract class GeoMesaAvroTutorial extends GeoMesaQuickStart {

    public GeoMesaAvroTutorial(String[] args, Param[] parameters) throws ParseException {
        super(args, parameters, new GDELTData(),true);
    }

    @Override
    public List<Query> getTestQueries(TutorialData data) {
        // we return the entire data set
        return Collections.singletonList(new Query(data.getTypeName()));
    }

    @Override
    public void queryFeatures(DataStore datastore, List<Query> queries) throws IOException {
        for (Query query: queries) {
            int n = 0;

            // write to in-memory bytes - alternatively we could write to a file, etc
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            // create an avro writer based on the simple feature type
            SimpleFeatureType sft = datastore.getSchema(query.getTypeName());
            try (AvroDataFileWriter writer = new AvroDataFileWriter(out, sft, -1)) {
                System.out.println("Querying data store and writing features to Avro binary format");

                // read from the underlying data store and write to the output stream
                try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                           datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                    while (reader.hasNext()) {
                        writer.append(reader.next());
                        n++;
                    }
                    writer.flush();
                }
            }

            byte[] bytes = out.toByteArray();

            System.out.println("Wrote " + n + " features as " + bytes.length + " bytes");

            System.out.println("Reading features back from Avro binary format");

            n = 0;

            // create an avro reader - the schema is self-describing, so we don't need to know
            // the SimpleFeatureType up front
            try (AvroDataFileReader reader = new AvroDataFileReader(new ByteArrayInputStream(bytes))) {
                while (reader.hasNext()) {
                    SimpleFeature feature = reader.next();
                    if (n++ < 10) {
                        // use geotools data utilities to get a printable string
                        System.out.println(String.format("%02d", n) + " " + DataUtilities.encodeFeature(feature));
                    } else if (n == 10) {
                        System.out.println("...");
                    }
                }
            }
            System.out.println();
            System.out.println("Read back " + n + " total features");
            System.out.println();
        }
    }
}