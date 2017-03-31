#!/usr/bin/env python
#coding:utf-8
"""
quickstart_command_line_parser.py

Description:
   This is extends the basic PyMesa command line parser to run the quickstart demo.

Created by: Jordan Muss

Creation Date: 3-30-2017
Version:       1.0

Dependencies: 
         Public:     argparse
         Private:   geomesa_command_line_parser

Updates:

To Do:

import os
def pathExpander(path):
    if ('~' in path):
        return os.path.expanduser(path)
    else:
        return os.path.abspath(path)
out_dir = os.path.expanduser('~/Documents/Code/geomesa/')

"""
'''----------------------------------------------------------------------------------------------------------------------'''
''' Get the runtime options: '''
'''----------------------------------------------------------------------------------------------------------------------'''
from .geomesa_command_line_parser import GeoMesaParser

def getArgs():
    argParser = GeoMesaParser()
    argParser.description = "{} with the quickstart demo using python & jnius.".format(argParser.description)
    argParser.add_argument("--no_print", "--no_quick", default=False, action="store_true", 
                           help="don't run the basic quickstart canned query, which returns nine rows of data (the default is to run it).")
    argParser.add_argument("--plot", "--graph", "-g", default=False, action="store_true", 
                           help="get all values from the 'quickstart' table and plot the points in a browser window.")
    argParser.add_argument("--out_dir", "--out", "-o", default=None, 
                           help="directory in which the plotting html will be saved.")
    
    return argParser.parse_args()
