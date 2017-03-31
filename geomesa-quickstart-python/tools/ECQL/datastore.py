#!/usr/bin/env python
#coding:utf-8
"""
datastore.py

Description:
   This function uses jnius and a parameter map to open a GeoMesa datastore. It
   returns the a DataStore java object, which is used to access the GeoMesa data.

Created by: Jordan Muss

Creation Date: 3-31-2017
Version:       1.0

Dependencies: 
         Public:     jnius
         Private:   setupJnius

Updates:

To Do:
"""
def getDataStore(JNI, dbConf):
    ''' This function reflects the Java DataStoreFinder class to get a datastore from
        a mapping of database parameters. '''
    class DataStoreFinder(JNI.JavaClass):
        __javaclass__ = "org/geotools/data/DataStoreFinder"
        __metaclass__ = JNI.MetaJavaClass
        getDataStore = JNI.JavaMethod('(Ljava/util/Map;)Lorg/geotools/data/DataStore;', static=True)
    
    return DataStoreFinder.getDataStore(dbConf)

def createAccumuloDBConf(JNI, conf_dict):
    '''  Create a DataStore config map (java hashmap using jnius) from a dict of parameters. '''
    jMap = JNI.autoclass('java.util.HashMap')
    dsConf = jMap()
    for key, value in conf_dict.items():
        dsConf.put(key, value)
    return dsConf
