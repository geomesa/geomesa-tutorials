#!/usr/bin/env python
#coding:utf-8
"""
geomesa_command_line_parser.py

Description:
   This is a basic class to parse command line arguments for PyMesa. It will be
   extended by other calls as needed.

Created by: Jordan Muss

Creation Date: 3-30-2017
Version:       1.0

Dependencies: 
         Public:     argparse
         Private:    

Updates:

To Do:
"""
import argparse

class GeoMesaParser(argparse.ArgumentParser):
    def __init__(self, argument_default=argparse.SUPPRESS):
        """
        :GeoMesaParser.
        """
        super(GeoMesaParser,self).__init__(add_help=True, argument_default=argument_default)
        self.add_argument("--instanceId", "--instId", "--inst", "-i", default="local", 
                       help="the name of your Accumulo instance (default=local)")
        self.add_argument("--zookeepers", "--zoo", "-z", default="localhost", 
                       help="your Zookeeper nodes, separated by commas (default=localhost)")
        self.add_argument("--user", "-u", default="root", 
                       help="the name of an Accumulo user that has permissions to create, read and write tables (default=root)")
        self.add_argument("--password", "--pwd", "--pw", default="secret", 
                       help="the password for the previously-mentioned Accumulo user (default=secret)")
        self.add_argument("--tableName", "--tableNm", "--table", "--tbl", "-t",  default="quickstart", 
                       help="the name of the on which to operate. This table will be created if it does not exist (default=quickstart)")
        self.add_argument("--classpath", "--cp", type=str, default=None, 
                       help="set the GeoMesa-Accumulo java classpath (default=None; use the classpath set in the os environment")
      
        self.description = "Run PyGeoMesa command line tools"
