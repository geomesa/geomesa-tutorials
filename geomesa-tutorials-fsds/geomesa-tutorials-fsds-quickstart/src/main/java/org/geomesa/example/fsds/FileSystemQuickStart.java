/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.fsds;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.geomesa.example.data.GDELTData;
import org.geomesa.example.data.TutorialData;
import org.geomesa.example.quickstart.GeoMesaQuickStart;
import org.geotools.data.DataStore;
import org.locationtech.geomesa.fs.data.FileSystemDataStore;
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory;
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage;
import org.locationtech.geomesa.fs.storage.api.PartitionScheme;
import org.locationtech.geomesa.fs.storage.common.interop.ConfigurationUtils;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.Collections;

public class FileSystemQuickStart extends GeoMesaQuickStart {

    // use gdelt data
    public FileSystemQuickStart(String[] args) throws ParseException {
        super(args, new FileSystemDataStoreFactory().getParametersInfo(), new GDELTData());
    }

    @Override
    public SimpleFeatureType getSimpleFeatureType(TutorialData data) {
        SimpleFeatureType sft = super.getSimpleFeatureType(data);
        // For the FSDS we need to modify the SimpleFeatureType to specify the index scheme
        ConfigurationUtils.setScheme(sft, "daily,z2-2bit", Collections.emptyMap());
        return sft;
    }

    public static void main(String[] args) {
        try {
            new FileSystemQuickStart(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }

    @Override
    public void cleanup(DataStore datastore, String typeName, boolean cleanup) {
        if (datastore != null) {
            try {
                if (cleanup) {
                    FileSystemStorage fsStorage = ((FileSystemDataStore) datastore).storage(typeName);
                    Path fsPath = fsStorage.context().root();
                    try {
                        System.out.println("Cleaning up test data");
                        fsStorage.context().fc().delete(fsPath, true);
                    } catch (IOException e) {
                        System.out.println("Unable to delete '" + fsPath.toString() + "':" + e.toString());
                    }
                }
            } catch (Exception e) {
                System.out.println("Unable to cleanup datastore: " + e.toString());
            } finally {
                // make sure that we dispose of the datastore when we're done with it
                datastore.dispose();
            }
        }
    }
}