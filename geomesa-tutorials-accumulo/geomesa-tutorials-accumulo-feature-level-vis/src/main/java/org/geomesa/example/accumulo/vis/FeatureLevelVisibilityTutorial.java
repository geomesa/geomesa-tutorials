/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.accumulo.vis;

import org.apache.commons.cli.ParseException;
import org.geomesa.example.data.GDELTData;
import org.geomesa.example.data.TutorialData;
import org.geomesa.example.quickstart.GeoMesaQuickStart;
import org.geotools.data.Query;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory;
import org.locationtech.geomesa.security.SecurityUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.Collections;
import java.util.List;

public class FeatureLevelVisibilityTutorial extends GeoMesaQuickStart {

    // use modified gdelt data
    private static TutorialData data = new GDELTData() {
        private SimpleFeatureType sft = null;

        @Override
        // overwrite the type name so we don't interfere with the regular quick start
        public String getTypeName() {
            return "gdelt-feature-level-visibility";
        }

        @Override
        // add a 'visibility' attribute for visualization
        public SimpleFeatureType getSimpleFeatureType() {
            if (sft == null) {
                SimpleFeatureType base = super.getSimpleFeatureType();
                SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
                builder.init(base);
                builder.add("visibility", String.class);
                sft = builder.buildFeatureType();
                // initializing a simple feature type builder doesn't copy user data, so do it ourselves
                sft.getUserData().putAll(base.getUserData());
            }
            return sft;
        }
    };

    public FeatureLevelVisibilityTutorial(String[] args)  throws ParseException {
        super(args, new AccumuloDataStoreFactory().getParametersInfo(), data);
    }

    @Override
    public List<SimpleFeature> getTestFeatures(TutorialData data) {
        List<SimpleFeature> features = super.getTestFeatures(data);
        int i = 0;
        while (i < features.size()) {
            SimpleFeature feature = features.get(i);
            String visibilities;
            if (i % 2 == 0) {
                visibilities = "admin";
            } else {
                visibilities = "user|admin";
            }
            // set the visibility as user data in the feature
            SecurityUtils.setFeatureVisibility(feature, visibilities);
            // also set as an attribute for visualization
            feature.setAttribute("visibility", visibilities);
            i++;
        }
        return features;
    }

    @Override
    public List<Query> getTestQueries(TutorialData data) {
        // we use geoserver for visualization
        return Collections.emptyList();
    }

    public static void main(String[] args) {
        try {
            new FeatureLevelVisibilityTutorial(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
