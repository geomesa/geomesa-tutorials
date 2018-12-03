/***********************************************************************
 * Copyright (c) 2016-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.example.geomesa.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class OSMKafkaBolt extends BaseRichBolt {
    private static final Logger log = Logger.getLogger(OSMKafkaBolt.class);
    Map<String, String> conf;
    String groupId;
    String topic;
    String featureName;
    Map<String , String> connectionParams;
    private FeatureWriter<SimpleFeatureType, SimpleFeature> featureWriter = null;
    private SimpleFeatureBuilder featureBuilder;
    private GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
    private static final int LATITUDE_COL_IDX  = 0;
    private static final int LONGITUDE_COL_IDX = 1;

    public OSMKafkaBolt(Map<String, String> conf, String groupId, String topic) {
        this.conf = conf;
        this.groupId = groupId;
        this.topic = topic;

        featureName = conf.get(OSMIngest.FEATURE_NAME);

        connectionParams = new HashMap<String , String>();
        connectionParams.put("instanceId", conf.get(OSMIngest.INSTANCE_ID));
        connectionParams.put("zookeepers", conf.get(OSMIngest.ZOOKEEPERS));
        connectionParams.put("user", conf.get(OSMIngest.USER));
        connectionParams.put("password", conf.get(OSMIngest.PASSWORD));
        connectionParams.put("auths", conf.get(OSMIngest.AUTHS));
        connectionParams.put("tableName", conf.get(OSMIngest.TABLE_NAME));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("field"));
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        final DataStore ds;
        try {
            ds = DataStoreFinder.getDataStore(connectionParams);
            SimpleFeatureType featureType = ds.getSchema(featureName);
            featureBuilder = new SimpleFeatureBuilder(featureType);
            featureWriter = ds.getFeatureWriterAppend(featureName, Transaction.AUTO_COMMIT);
        } catch (IOException e) {
            throw new RuntimeException("Unable to initialize feature writer", e);
        }
    }

    public void execute(Tuple tuple) {

        final String[] attributes = tuple.getValue(0).toString().split(",");

        // Only ingest attributes that have a latitude and longitude
        if (attributes.length == 2 && attributes[LATITUDE_COL_IDX] != null && attributes[LONGITUDE_COL_IDX] != null) {

            featureBuilder.reset();
            final SimpleFeature simpleFeature = featureBuilder.buildFeature(String.valueOf(UUID.randomUUID().getMostSignificantBits()));
            simpleFeature.setDefaultGeometry(getGeometry(attributes));

            try {
                final SimpleFeature next = featureWriter.next();
                for (int i = 0; i < simpleFeature.getAttributeCount(); i++) {
                    next.setAttribute(i, simpleFeature.getAttribute(i));
                }
                ((FeatureIdImpl)next.getIdentifier()).setID(simpleFeature.getID());
                featureWriter.write();
            } catch (IOException e) {
                log.error("Exception writing feature", e);
            }
        }
    }

    private Geometry getGeometry(final String[] attributes) {
        try {
            final Double lat = (double)Integer.parseInt(attributes[LATITUDE_COL_IDX]) / 1e7;
            final Double lon = (double)Integer.parseInt(attributes[LONGITUDE_COL_IDX]) / 1e7;
            return geometryFactory.createPoint(new Coordinate(lon, lat));
        } catch (NumberFormatException e) {
            log.error("Number format exception", e);
        }
        return null;
    }
}
