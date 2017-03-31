#!/usr/bin/env python
#coding:utf-8
"""
geomesa_jnius_setup.py

Note: this has been tested with python 2.7.12 & 3.5

Description:
   This script performs the necessary steps to setup jnius to provide GeoMesa functionality
   with accumulo. PyJnius is a third party package that requires cython and acts as a Java
   Natie Interface, allowing python programs to run java code.
   
   On unix-based systems these packages can be installed using pip:
            1) sudo -H pip install cython
            2) sudo -H pip install pyjnius

Created by: Jordan Muss
                   CCRi

Creation Date: 3-30-2017
Version:       1.0

Dependencies: 
         Public:     jnius, os, sys, __future__ (print_function)
         Private:   

Interfaces:
        setupJnius              This class setsup jnius to work with the necessary GeoMesa libraries. It tests
                                      that the necessary packages are accessible, or fails with a sys.exit(-1). It also
                                      wraps some of the jnius classes with helper functions specific to GeoMesa cases.

        DataStoreFinder     This class is a jnius Java reflection class that allows PyGeomesa to connect to
                                       GeoMesa Accumulo databases.
Updates:

To Do:
"""
'''----------------------------------------------------------------------------------------------------------------------'''
''' Import Java & Geomesa classes: '''
'''----------------------------------------------------------------------------------------------------------------------'''
#from __future__ import print_function
import os, sys

class setupJnius:
    def __init__(self, classpath=None):
        ''' This is a kluge to allow autoclass & find_javaclass to be 'private' functions
            of this class. In reality, they are global to the geomesa_jnius_setup namespace'''
        global _autoclass, _find_javaclass, _JavaClass, _JavaMethod, _MetaJavaClass
        if classpath is not None:
            java_classpath = os.path.expanduser(classpath)
            os.environ['CLASSPATH'] = java_classpath
        try:
            from jnius import autoclass as _autoclass
            from jnius import find_javaclass as _find_javaclass
            from jnius import JavaClass as _JavaClass
            from jnius import JavaMethod as _JavaMethod
            from jnius import MetaJavaClass as _MetaJavaClass
        except:
            print("Error: the jnius package has not been installed. Please install it with:")
            print("\t\tsudo -H pip install jnius\n\n")
            sys.exit(-1)
        if self.testClasspath():
            self.classpath_status = True
            print("Your classpath is valid.")
        else:
            print("Error:there are errors in your classpath; refer to the logged tests above.")
            sys.exit(-1)
    def autoclass(self, JavaClass):
        return _autoclass(JavaClass)
    def find_javaclass(self, JavaClass, msg=None):
        if msg is None : msg = JavaClass
        try:
            _find_javaclass(JavaClass)
            print("{} class path is OK".format(msg))
            result = True
        except:
            print("Error: {} class path is NOT OK".format(msg))
            result = False
        return result
    
    @property
    def JavaClass(self):
        return _JavaClass
    def JavaMethod(self, jMethod, static=False):
        return _JavaMethod(jMethod, static=static)
    
    @property
    def MetaJavaClass(self):
        return _MetaJavaClass
    
    def testClasspath(self):
        #GeoMesa test:
        GeoMesa = self.find_javaclass("org.geotools.data.DataStoreFinder", msg="GeoMesa")
        #Hadoop test:
        Hadoop = self.find_javaclass("org.apache.hadoop.io.Writable", msg="Hadoop")
        #zookeeper test:
        zookeeper = self.find_javaclass("org.apache.zookeeper.KeeperException", msg="zookeeper")
        #accumulo test:
        accumulo = self.find_javaclass("org.apache.accumulo.core.util.Version", msg="accumulo")
        return GeoMesa and Hadoop and zookeeper and accumulo

'''
class DataStoreFinder(j.JavaClass):
    __javaclass__ = "org/geotools/data/DataStoreFinder"
    __metaclass__ = j.MetaJavaClass
    getDataStore = j.JavaMethod('(Ljava/util/Map;)Lorg/geotools/data/DataStore;', static=True)
'''
