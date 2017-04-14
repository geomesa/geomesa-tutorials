#!/usr/bin/env python
#coding:utf-8
"""
filter.py

Description:
   These functions are helpers to build ECQL queries/filters to access data GeoMesa data.

Created by: Jordan Muss

Creation Date: 3-31-2017
Version:       1.0

Dependencies: 
         Public:     jnius
         Private:   SetupJnius

Interfaces:
            ECQLQuery                Helper cass to allow python access to java Query & CQL functions. Has createFilter,
                                              createQuery, & getFeatures methods
            createBBoxFilter        Function to create a bounding-box (BBOX) filter string of a rectangular query area
            createWithinFilter      Function to create a container (WITHIN) filter of a query area described by a polygon
                                              or multipolygon WKT
            createDuringFilter      Function to create a temporal query string for a fixed range of date/times (DURING)
            createAttributeFilter   Function to create a query that accepts everything else (other predicates that operate
                                               on other attribute types); this uses the GeoTools Filter constant "INCLUDE"

Updates:
            4-6-2017            createWithinFilter          Function to create a container (WITHIN) filter of a query area
                                                                            described by a polygon or multipolygon WKT
To Do:
"""
class ECQLQuery:
    """ This is a helper class to allow python access to java Query & CQL functions."""
    def __init__(self, JNI):
        ''' This is sets up links to the java types & java Query and CQL functions.'''
        self.jString = JNI.autoclass('java.lang.String')
        self.Query = JNI.autoclass('org.geotools.data.Query')
        self.CQL = JNI.autoclass('org.geotools.filter.text.cql2.CQL')
    
    def createFilter(self, filter_string):
        ''' Create the ECQL filter from the prepared (python) string: '''
        return self.CQL.toFilter(self.jString(filter_string))
    
    def createQuery(self, simpleFeatureTypeName, dataStore, filter_string):
        ''' Return an ECQL filter query for iterating & additional processing: '''
        jSFT_name = self.jString(simpleFeatureTypeName)
        cqlFilter = self.createFilter(filter_string)
        return self.Query(jSFT_name, cqlFilter)
    
    def getFeatures(self, simpleFeatureTypeName, dataStore, filter_string):
        ''' Build & execute an ECQL query from a filter string for the DataStore.
            Return an ECQL filter query for iterating & additional processing: '''
        jSFT_name = self.jString(simpleFeatureTypeName)
        query = self.createQuery(simpleFeatureTypeName, dataStore, filter_string)
        #query = ecql.queryFeatures(simpleFeatureTypeName, dataStore, filter_string)
        ''' Submit the query, which will return an iterator over matching features: '''
        featureSource = dataStore.getFeatureSource(jSFT_name)
        #featureItr = featureSource.getFeatures(query).features()
        return featureSource.getFeatures(query)

def createBBoxFilter(geomField, x0, y0, x1, y1):
    ''' Create a bounding-box (BBOX) filter of a rectangular query area: '''
    cqlGeometry = "BBOX({}, {}, {}, {}, {})".format(geomField, x0, y0, x1, y1)
    return cqlGeometry

def createWithinFilter(geomField, poly_descriptor):
    ''' Create a container (WITHIN) filter of a query area described by a polygon or
        multipolygon WKT: '''
    cqlGeometry = "WITHIN({}, {})".format(geomField, poly_descriptor)
    return cqlGeometry

def createDuringFilter(dateField, t0, t1):
    ''' Create a temporal query string for a fixed range of date/times (DURING): '''
    cqlDates = "({} DURING {}/{})".format(dateField, t0, t1)
    return cqlDates

def createAttributeFilter(attributesQuery):
    ''' The GeoTools Filter constant "INCLUDE" is a default meant to accept
        everything else (other predicates that operate on other attribute types): '''
    cqlAttributes = "INCLUDE" if attributesQuery is None else attributesQuery
    return cqlAttributes

