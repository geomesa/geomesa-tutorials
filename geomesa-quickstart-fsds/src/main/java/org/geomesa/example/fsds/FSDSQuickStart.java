/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.fsds;

import org.apache.commons.cli.ParseException;
import org.geomesa.example.quickstart.GDELTData;
import org.geomesa.example.quickstart.GeoMesaQuickStart;
import org.geomesa.example.quickstart.QuickStartData;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.locationtech.geomesa.fs.FileSystemDataStoreFactory;
import org.locationtech.geomesa.fs.storage.common.*;
import org.locationtech.geomesa.fs.storage.api.PartitionScheme;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.Map;

public class FSDSQuickStart extends GeoMesaQuickStart {

    // use gdelt data
    private QuickStartData data = new GDELTData();

    public FSDSQuickStart(Map<String, String> params) { super(params); }

    public FSDSQuickStart(String[] args) throws ParseException {
        super(args, new FileSystemDataStoreFactory().getParametersInfo());
    }

    @Override
    public SimpleFeatureType createSchema(DataStore datastore, QuickStartData data) throws IOException {
        SimpleFeatureType sft = data.getSimpleFeatureType();
        PartitionScheme scheme = CommonSchemeLoader.build("daily,z2-2bit", sft);
        PartitionSchemeUtils.addToSft(sft, scheme);
        System.out.println("Creating schema: " + DataUtilities.encodeType(sft));
        datastore.createSchema(sft);
        System.out.println();
        return sft;
    }

    @Override
    public QuickStartData getData() {
        return data;
    }

    public static void main(String[] args) {
        try {
            new FSDSQuickStart(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
