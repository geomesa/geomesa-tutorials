#!/usr/bin/env python
#coding:utf-8
"""
customJavaClasses.py

Description:
   These functions & classes create custom versions of java classes for python using
   jnius.

Created by: Jordan Muss

Creation Date: 4-5-2017
Version:       1.0

Dependencies: 
         Public:     jnius, sys
         Private:   SetupJnius

Interfaces:
        createSimpleFeatureType              Function with a class to create a 'bare' SimpleFeatureType from a list
                                                              of attributes, and to perform appropriate tasks, e.g. set feature indices

Updates:

To Do:
"""
import sys

def getJavaTrueFalse(JNI, pyBool):
    ''' This function reflects the Java Boolean class and returns a True/False
        request as a java object. '''
    BoolDict = {"true":True,
                        "yes":True,
                        "false":False,
                        "no":False
    }
    class Boolean(JNI.JavaClass):
        __javaclass__ = 'java/lang/Object'
        __metaclass__ = JNI.MetaJavaClass
        TRUE = JNI.JavaObject('()Zjava/lang/Object;', static=True)
        FALSE = JNI.JavaObject('()Zjava/lang/Object;', static=True)
    
    if isinstance(pyBool, str):
        boolVal = BoolDict.get(pyBool.lower(), None)
        if not isinstance(boolVal, bool):
            print("Error:bad value passed to 'getJavaTrueFalse'; {} must be in str(true, false, yes, no).".format(pyBool))
            sys.exit(-1)
    elif isinstance(pyBool, bool):
        boolVal = pyBool
    else:
        print("Error:bad value passed to 'getJavaTrueFalse'; {} must be type bool or in str(true, false, yes, no).".format(pyBool))
        sys.exit(-1)
    jBool = Boolean()
    if boolVal:
        jBoolVal = jBool.TRUE
    else:
        jBoolVal = jBool.FALSE
    return jBoolVal
