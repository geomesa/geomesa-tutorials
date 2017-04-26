#!/usr/bin/env python
#coding:utf-8
"""
/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
geomesa_quickstart_jnius.py

Note: this has been tested with python 2.7.12 & 3.5

Description:
   This script is a python adaptation of the GeoMesa Accumulo quickstart. It is a query only
   demo, in contrast to the java quickstart that creates the table & loads data into it. This
   script uses two second party packages (jnius & bokeh) that need to be loaded to run. 'jnius'
   is a python-java bridge that allows python programs to run java code.
   
   On unix-based systems these packages can be installed using pip:
            1) sudo -H pip install pyjnius
            2) sudo -H pip install bokeh
   
   There are two command line options:

Created by: Jordan Muss
                   CCRi

Creation Date: 3-31-2017
Version:       1.0

Dependencies: 
         Public:     argparse, bokeh, datetime, jnius, os, re, string, sys, __future__ (print_function)
         Private:   utils.geomesa_jnius_setup, utils.quickstart_command_line_parser (calls utils.geomesa_command_line_parser)
                        tools.ECQL.datastore, tools.ECQL.filter

How to use:
Updates:

To Do: Get queryFeaturesToDict from tools/ECQL/queryFormatters
"""
from __future__ import print_function
from datetime import datetime
'''----------------------------------------------------------------------------------------------------------------------'''
''' Import Java & GeoMesa classes: '''
'''----------------------------------------------------------------------------------------------------------------------'''
from utils.geomesa_jnius_setup import *
from utils.quickstart_command_line_parser import getArgs
from pyJavaClasses.datastore import getDataStore, createAccumuloDBConf
from tools.ECQL import filter
'''----------------------------------------------------------------------------------------------------------------------'''
''' Setup quickstart data access & display functions: '''
'''----------------------------------------------------------------------------------------------------------------------'''
def queryFeaturesToDict(ecql, simpleFeatureTypeName, dataStore, filter_string):
    ''' Return the results of a ECQL filter query as a dict for additional processing: '''
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
        results[n] = {"who":feature.getProperty("Who").getValue(),
                              "what":feature.getProperty("What").getValue(),
                              "when":datetime.strptime(feature.getProperty("When").getValue().toString(), "%a %b %d %H:%M:%S %Z %Y"),
                              "geometry":feature.getProperty("Where").getValue(),
                              "where":feature.getProperty("Where").getValue().toString(),
                              "x":feature.getProperty("Where").getValue().x,
                              "y":feature.getProperty("Where").getValue().y,
                              "why":feature.getProperty("Why").getValue().__str__() }
    featureIter.close()
    return(results)

def printQuickStart(quickStart_dict):
    ''' Print the quickstart feature dict: '''
    for k in sorted(quickStart_dict.keys()):
        print("{}.\t{}|{}|{}|{}|{}".format(k, quickStart_dict[k]["who"], quickStart_dict[k]["what"],
                  quickStart_dict[k]["when"].strftime("%a %b %d %H:%M:%S %Z %Y"), 
                  quickStart_dict[k]["where"], quickStart_dict[k]["why"]))
'''----------------------------------------------------------------------------------------------------------------------'''
''' End GeoMesa query functions: '''
'''----------------------------------------------------------------------------------------------------------------------'''

if __name__ == "__main__":
    '''----------------------------------------------------------------------------------------------------------------------'''
    ''' Get the runtime options: '''
    '''----------------------------------------------------------------------------------------------------------------------'''
    args = getArgs()
    '''----------------------------------------------------------------------------------------------------------------------'''
    ''' Setup jnius for GeoMesa java calls: '''
    '''----------------------------------------------------------------------------------------------------------------------'''
    classpath = args.classpath
    jni = SetupJnius(classpath=classpath)
    '''----------------------------------------------------------------------------------------------------------------------'''
    ''' Setup data for GeoMesa query: '''
    '''----------------------------------------------------------------------------------------------------------------------'''
    simpleFeatureTypeName = "AccumuloQuickStart"
    dsconf_dict = {'instanceId':args.instanceId, 
                     'zookeepers':args.zookeepers, 
                     'user':args.user,
                     'password':args.password,
                     'tableName':args.tableName }

    dsconf = createAccumuloDBConf(jni, dsconf_dict)
    
    dataStore = getDataStore(jni, dsconf) # this may not work in python 3.5
    ECQL = filter.ECQLQuery(jni)
    
    if not args.no_print:
        bbox_filter = filter.createBBoxFilter("Where", -77.5, -37.5, -76.5, -36.5)
        when_filter = filter.createDuringFilter("When", "2014-07-01T00:00:00.000Z", "2014-09-30T23:59:59.999Z")
        who_filter = filter.createAttributeFilter("(Who = 'Bierce')")
        combined_filter = "{} AND {} AND {}".format(bbox_filter, when_filter, who_filter)        
        quickstart = queryFeaturesToDict(ECQL, simpleFeatureTypeName, dataStore, combined_filter)
        printQuickStart(quickstart)
    
    if args.plot:
        from utils.geomesa_plotting import plotGeoPoints
        all_pts_bbox_filter = filter.createBBoxFilter("Where", -78.0, -39.0, -76.0, -37.0)
        all_points = queryFeaturesToDict(ECQL, simpleFeatureTypeName, dataStore, all_pts_bbox_filter)
        plotGeoPoints(all_points, "Quickstart demo (jnius)", save_dir=args.out_dir)
