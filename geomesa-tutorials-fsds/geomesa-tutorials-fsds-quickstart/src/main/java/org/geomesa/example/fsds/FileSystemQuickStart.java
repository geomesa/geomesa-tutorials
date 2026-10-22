/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.fsds;

import org.apache.commons.cli.ParseException;
import org.geomesa.example.data.GDELTData;
import org.geomesa.example.data.TutorialData;
import org.geomesa.example.quickstart.GeoMesaQuickStart;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory;

public class FileSystemQuickStart extends GeoMesaQuickStart {

    // use gdelt data
    public FileSystemQuickStart(String[] args) throws ParseException {
        super(args, new FileSystemDataStoreFactory().getParametersInfo(), new GDELTData());
    }

    @Override
    public SimpleFeatureType getSimpleFeatureType(TutorialData data) {
        SimpleFeatureType sft = super.getSimpleFeatureType(data);
        // For the FSDS we need to modify the SimpleFeatureType to specify the index scheme
        sft.getUserData().put("geomesa.fs.scheme", "daily,z2-4bit");
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
}