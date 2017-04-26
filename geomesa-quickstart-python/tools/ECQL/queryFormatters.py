#!/usr/bin/env python
#coding:utf-8
"""
queryFormatters.py

Description:
   These functions executes an ECQL query and either loads the results into a python
   dictionary or list, or writes it to a JSON file. Field names and java data type are provided
   in field_dict, where the dict key is the field name and the java data type is stored in
   "type". The type_dict provides functions to convert from java data types to python data
   types.

Created by: Jordan Muss

Creation Date: 4-11-2017
Version:       1.0

Dependencies: 
         Public:     jnius, json
         Private:   ECQL, SetupJnius

Interfaces:
            queryFeaturesToDict         Execute a query, convert the java objects to python types, and load
                                                     load the results into a python dict of dicts. The master dict is keyed
                                                     by row number & each sub_dict is the entire row of data, keyed by
                                                     field name. Warning, this seems to have a potential java memory
                                                     leak, because some objects are not properly destroyed.

            queryFeaturesToList          Execute a query, but append each record (a java object) to a python
                                                     list, which is the returned object. Warning, this has a potential java
                                                     memory leak, because the objects are not properly destroyed.

            queryFeaturesToJSON       Execute a query, convert each row (a java object) to dict of python
                                                     types keyed by field name. Each row dict is then written as a json
                                                     object to a text file/ Warning, this seems to have a potential java
                                                     memory leak, because some objects are not properly destroyed.

Updates:

To Do:
"""
from __future__ import print_function
import json

def queryFeaturesToDict(ecql, simpleFeatureTypeName, dataStore, filter_string, field_dict, print_num=10):
    ''' Return the results of a ECQL filter query as a dict for additional processing: '''
    type_dict = {"date":lambda v : v.toString(), 
                         "double":lambda v : v, 
                         "float":lambda v : v, 
                         "integer":lambda v : v, 
                         "long":lambda v : v, 
                         "point":lambda v : {"X":v.x, "Y":v.y, "geom":v}, 
                         "string":lambda v : v
    }
    ''' Get field names and types for processing and type conversion: '''
    fields = {key:val["type"].lower() for key, val in field_dict.items()}
    ''' Submit the query, which will return a features object: '''
    features = ecql.getFeatures(simpleFeatureTypeName, dataStore, filter_string)
    ''' Get an iterator of the matching features: '''
    featureIter = features.features()
    ''' Loop through all results and put them into a dictionary for secondary processing: '''
    n = 0
    results = {}
    while featureIter.hasNext():
        feature = featureIter.next()
        n += 1
        record = {}
        for key, fType in fields.items():
            record[key] = type_dict[fType](feature.getProperty(key).getValue())
        results[n] = record
    featureIter.close()
    if print_num is not None and print_num > 0 and n <= print_num:
        print("N|{}".format("|".join(record.keys())))
        for i in range(print_num):
            p = "|".join(["{}".format(v) for v in record.values()])
            print("{}|{}".format(i, p))
    return results

def queryFeaturesToList(ecql, simpleFeatureTypeName, dataStore, filter_string):
    ''' Return the results of a ECQL filter query as a list of GeoMesa java objects for additional processing: '''
    ''' Submit the query, which will return a features object: '''
    features = ecql.getFeatures(simpleFeatureTypeName, dataStore, filter_string)
    ''' Get an array (list) of the matching features as GeoMesa java objects: '''
    results = features.toArray()
    return results

def queryFeaturesToJSON(ecql, simpleFeatureTypeName, dataStore, filter_string, field_dict, out_file):
    ''' Return the results of a ECQL filter query as a dict for additional processing: '''
    type_dict = {"date":lambda v : v.toString(), 
                         "double":lambda v : v, 
                         "float":lambda v : v, 
                         "integer":lambda v : v, 
                         "long":lambda v : v, 
                         "point":lambda v : {"X":v.x, "Y":v.y}, 
                         "string":lambda v : v
    }
    ''' Get field names and types for processing and type conversion: '''
    fields = {key:val["type"].lower() for key, val in field_dict.items()}
    ''' Submit the query, which will return a features object: '''
    features = ecql.getFeatures(simpleFeatureTypeName, dataStore, filter_string)
    ''' Get an iterator of the matching features: '''
    featureIter = features.features()
    ''' Loop through all results and put them into a dictionary for secondary processing: '''
    n = 0
    with open(out_file, "w") as json_file:
        while featureIter.hasNext():
            n += 1
            feature = featureIter.next()
            record = {}
            for key, fType in fields.items():
                record[key] = type_dict[fType](feature.getProperty(key).getValue())
            print(json.dumps(record), file=json_file)
            del record
    featureIter.close()
    return n
