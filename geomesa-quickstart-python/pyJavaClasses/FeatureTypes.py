#!/usr/bin/env python
#coding:utf-8
"""
FeatureTypes.py

Description:
   This function uses jnius to implement various features from the SimpleFeatureType(s)
   java classes.

Created by: Jordan Muss

Creation Date: 4-4-2017
Version:       1.0

Dependencies: 
         Public:     jnius
         Private:   setupJnius

Interfaces:
        createSimpleFeatureType              Class to create a 'bare' SimpleFeatureType from a list of attributes,
                                                              and to perform appropriate tasks, e.g. set feature indices

Updates:

To Do:
"""
from .customJavaClasses import getJavaTrueFalse 

class SimpleFeatureType():
    def __init__(self, JNI):
        """ Create the necessary java class interfaces: """
        self.SimpleFeatureTypes = JNI.autoclass("org.locationtech.geomesa.utils.interop.SimpleFeatureTypes")
        self.SimpleFeatureBuilder = JNI.autoclass("org.geotools.feature.simple.SimpleFeatureBuilder")
        self.Hints = JNI.autoclass("org.geotools.factory.Hints")
        self.simpleFeature = None
        self.true = getJavaTrueFalse(JNI, True)
    
    def createSimpleFeatureType(self, simpleFeatureTypeName, attribute_list):
        """ This will create a 'bare' SimpleFeatureType from a list of attributes: """
        attributes = ','.join(attribute_list)
        self.simpleFeature = self.SimpleFeatureTypes.createType(simpleFeatureTypeName, attributes)
    
    def setDateTimeIndex(self, field_name):
        """ Use the user-data (hints) to specify which date-time field is meant to be indexed: """
        if self.simpleFeature is None:
            print("You must create a simple feature type before you can set an index")
            index_set = False
        else:
            self.simpleFeature.getUserData().put(self.SimpleFeatureTypes.DEFAULT_DATE_KEY, field_name)
            index_set = True
        return index_set
    
    def build(self, feature_shell, id):
        """ Create a new (unique) identifier and empty feature shell: """
        feature_builder = self.SimpleFeatureBuilder.build(self.simpleFeature, feature_shell, id)
        """ Explicitly tell GeoTools to use the provided ID: """
        feature_builder.getUserData().put(self.Hints.USE_PROVIDED_FID, self.true)
        return feature_builder
