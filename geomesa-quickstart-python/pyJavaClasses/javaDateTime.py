#!/usr/bin/env python
#coding:utf-8
"""
javaDateTime.py

Description:
   This function takes a python datetime object with optional timezone information, translates
   it into a java datetime object (using jnius) and returns the java datetime object.

Created by: Jordan Muss

Creation Date: 4-6-2017
Version:       1.0

Dependencies: 
         Public:     datetime, jnius
         Private:   SetupJnius

Updates:

To Do:
"""
from datetime import datetime

def pyDateTimeToJava(JNI, dateTime, tz=None):
    if not isinstance(dateTime, datetime):
        print("Error: dateTime ({}) must be a python datetime object, not {}".format(dateTime, type(dateTime)))
        return None
    jDate = JNI.autoclass('org.joda.time.DateTime')
    jDTZ = JNI.autoclass("org.joda.time.DateTimeZone")
    
    jTZ = None
    if tz is not None:
        try:
            jTZ = jDTZ.forID(tz.upper())
        except:
            print("Warning: error setting the time zone info for {}; timezone will be ignored".format(tz))
    if jTZ is not None:
        dtTm = jDate(dateTime.year, dateTime.month, dateTime.day, dateTime.hour, dateTime.minute, dateTime.second, jTZ)
    else:
        dtTm = jDate(dateTime.year, dateTime.month, dateTime.day, dateTime.hour, dateTime.minute, dateTime.second)
    return dtTm
