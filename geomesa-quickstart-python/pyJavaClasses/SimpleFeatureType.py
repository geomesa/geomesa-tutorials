#!/usr/bin/env python
#coding:utf-8
"""
SimpleFeatureType.py

Description:
   This function uses jnius to implement various features from the SimpleFeatureType(s)
   java classes.

Created by: Jordan Muss

Creation Date: 4-4-2017
Version:       1.0

Dependencies: 
         Public:     jnius
         Private:   SetupJnius

Interfaces:
        createSimpleFeatureType              Class to create a 'bare' SimpleFeatureType from a list of attributes,
                                                              and to perform appropriate tasks, e.g. set feature indices

Updates:

To Do:
"""
class SimpleFeatureType():
    def __init__(self, JNI):
        """ Create the necessary java class interfaces: """
        self.SimpleFeatureTypes = JNI.autoclass("org.locationtech.geomesa.utils.interop.SimpleFeatureTypes")

    def createSimpleFeatureType(self, simpleFeatureTypeName,  attribute_list):
        """ This will create a 'bare' simpleFeatureType from a list of attributes: """
        attributes = ','.join(attribute_list)
        simpleFeatureType = self.SimpleFeatureTypes.createType(simpleFeatureTypeName, attributes)
        return simpleFeatureType

    def setDateTimeIndex(self, simpleFeature, field_name):
        """ use the user-data (hints) to specify which date-time field is meant to be indexed: """
        simpleFeature.getUserData().put(self.SimpleFeatureTypes.DEFAULT_DATE_KEY, field_name)
