#!/usr/bin/env python
#coding:utf-8
"""
queryToDict.py

Description:
   This function executes an (E)CQL query and loads the results into a python dictionary. Field
   names and java data type are provided in field_dict, where the dict key is the field name
   and the java data type is stored in "type". The type_dict provides functions to convert
   from java data types to python data types.

Created by: Jordan Muss

Creation Date: 4-6-2017
Version:       1.0

Dependencies: 
         Public:     jnius
         Private:   ECQL, setupJnius

Interfaces:

Updates:

To Do:
"""
def queryFeaturesToDict(ecql, simpleFeatureTypeName, dataStore, filter_string, field_dict, print_num=10):
    ''' Return the results of a (E)CQL filter query as a dict for additional processing: '''
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
        if print_num is not None and print_num > 0 and n <= print_num:
            if n == 1:
                print("N|{}".format("|".join(record.keys())))
            p = "|".join(["{}".format(v) for v in record.values()])
            print("{}|{}".format(n, p))
    featureIter.close()
    return(results)
