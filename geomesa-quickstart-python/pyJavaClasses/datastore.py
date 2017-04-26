#!/usr/bin/env python
#coding:utf-8
"""
datastore.py

Description:
   This function uses jnius and a parameter map to open a GeoMesa datastore. It
   returns the a DataStore java object, which is used to access the GeoMesa data.
   It also includes functions to get information about the contents of a DataStore.

Created by: Jordan Muss

Creation Date: 3-31-2017
Version:       1.0

Dependencies: 
         Public:     jnius
         Private:   SetupJnius

Updates:
        4-6-2017          getTableFields          Added functionality to get data type for fields

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

def getTableNames(dataStore):
    ''' Return a list of Feature (type) names: '''
    return dataStore.getTypeNames()

def getTableFields(dataStore, feature):
    ''' Return a dict with keys=feature field names, and values=basic feature descriptions: '''
    schema = dataStore.getSchema(feature)
    field_dict = {}
    for field in schema.descriptors.toArray():
        name = field.localName
        descriptor = field.toString()
        data_type = descriptor.split("{}:".format(name))[1].split(">")[0]
        field_dict[name] = {'type':data_type, 
                                         'default':field.defaultValue,
                                         'isNillable':field.isNillable(), 
                                         'descriptor':descriptor}
    return field_dict

def deleteTable(dataStore, tablename):
    ''' Drop a table (feature) from an GeoMesa Accumulo database (table): '''
    print("Warning:dropping {} from {}; this action cannot be reversed; all data will be lost!")
    dataStore.removeSchema(tablename)
