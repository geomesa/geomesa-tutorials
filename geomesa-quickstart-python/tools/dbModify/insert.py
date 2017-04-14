#!/usr/bin/env python
#coding:utf-8
"""
insert.py

Description:
   These functions prepare data and insert them into Accumulo tables (GeoMesa SimpleFeatures).

Created by: Jordan Muss

Creation Date: 4-12-2017
Version:       1.0

Dependencies: 
         Public:     jnius, json
         Private:   ECQL, SetupJnius

Interfaces:
            queryFeaturesToDict         something
                                                     more

Updates:

To Do:
"""
from __future__ import print_function

def insert(JNI, ecql, simpleFeatureTypeName, dataStore, filter_string, field_dict, print_num=10):
    ''' Insert data into an Accumulo simpleFeatureType (table): '''
    jLong = JNI.autoclass('java.lang.Long')
    WKTUtils = JNI.autoclass("org.locationtech.geomesa.utils.interop.WKTUtils")
    dfc = JNI.autoclass("org.geotools.feature.DefaultFeatureCollection")
    SimpleFeatureStore = JNI.autoclass("org.geotools.data.simple.SimpleFeatureStore")
    
    # ToDo: This is test stuff, delete this block when finished

    sft = simpleFeatureType(JNI)
    sft.createSimpleFeatureType(tableName, attribute_list)
    sft.setDateTimeIndex("When")
    
    print("Creating feature-type (schema): {}".format(tableName))
    dataStore.createSchema(sft.simpleFeature)
    
    num_features = 1000
    print("Creating new {} new features".format(num_features))
    people_names = ["Addams", "Bierce", "Clemens"]
    num_names = len(people_names)
    
    SECONDS_PER_YEAR = 365 * 24 * 60 * 60
    random.seed(5771)
    MIN_DATE = datetime(2014, 1, 1, 0, 0, 0)
    MIN_X = -78.485146
    MIN_Y = 38.030758
    DX = 2.0
    DY = 2.0
    fc = dfc()
    
    for i in range(num_features):
        ID = "Observation.{}".format(i)
        fb = sft.build([], ID)
        fb.setAttribute("Who", people_names[i % num_names])
        fb.setAttribute("What", jLong(i))
        x = MIN_X + random.random() * DX
        y = MIN_Y + random.random() * DY
        geometry = WKTUtils.read("POINT({} {})".format(x, y))
        fb.setAttribute("Where", geometry)
        sample_dateTime = MIN_DATE + timedelta(seconds=int(round(random.random() * SECONDS_PER_YEAR)))
        jDateTime = pyDateTimeToJava(JNI, sample_dateTime, "UTC")
        fb.setAttribute("When", jDateTime.toDate())
        fc.add(fb)
        del fb
    
    print("Inserting new features")
    featureStore = JNI.cast("org.geotools.data.simple.SimpleFeatureStore", dataStore.getFeatureSource(tableName))
    featureStore.addFeatures(fc)

    return 
