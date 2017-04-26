#!/usr/bin/env python
#coding:utf-8
"""
geomesa_plotting.py

Note: this has been tested with python 2.7.12 & 3.5

Description:
   This script uses bokeh to perform plotting operations on GeoMesa data collected using
   the PyMesa libraries. It relies heavily on the bokeh plotting library, which can be loaded
   using pip:
            1) sudo -H pip install bokeh

Created by: Jordan Muss
                   CCRi

Creation Date: 3-30-2017
Version:       1.0

Dependencies: 
         Public:     bokeh, re, string, os, sys, __future__ (print_function)
         Private:   
How to use:
Updates:

To Do:
"""
import os, re, string, sys
'''----------------------------------------------------------------------------------------------------------------------'''
''' Load the plotting libraries: '''
'''----------------------------------------------------------------------------------------------------------------------'''
try:
    from bokeh.plotting import ColumnDataSource, figure, output_file, reset_output, show
    from bokeh.models import (HoverTool, BoxZoomTool, CrosshairTool, PanTool, ResetTool,
                                                 SaveTool, WheelZoomTool, ZoomInTool, ZoomOutTool, Range1d) 
except:
    print("Error: the bokeh package has not been installed. Please install it with:")
    print("\t\tsudo -H pip install bokeh\n\n")
    sys.exit(-1)
'''--------------------------------------------------------------------'''
''' Begin plotting functions:'''
'''--------------------------------------------------------------------'''
def plotGeoPoints(data_dict, data_title, offset=0.25, pt_size=7, save_dir=None):
    if save_dir is not None:
        path = os.path.expanduser(save_dir)
        if os.path.exists(path) and os.path.isdir(path):
            os.chdir(path)
        else:
            print("Warning: either {} does not exist or is not a valid directory; writing html file to {}".format(path, os.getcwd()))
    noPunc = re.compile("[%s\\…]" % re.escape(string.punctuation))
    out_file_name = "{}.html".format(noPunc.sub("", data_title).replace(" ", "_"))
    ''' Plot the points collected from GeoMesa'''
    fid, x, y, what, who, when, why = zip(*[(key, row['x'], row['y'], row['what'], row['who'], row['when'], row['why']) for key, row in data_dict.items()])
    ''' Put the time fields into mm/dd/yyyy h:m am/pm format: '''
    when = [tm.strftime("%M/%d/%Y %I:%M %p") for tm in when]
    data_source = ColumnDataSource(dict(fid=fid, x=x, y=y, what=what, who=who, when=when, why=why))
    hover = HoverTool(tooltips=[ ("fid", "@fid"),
                                                    ("(x,y)", "($x, $y)"),
                                                    ("Who", "@who"),
                                                    ("What", "@what"),
                                                    ("When", "@when"),
                                                    ("Why", "@why"), ] )
    tools = [PanTool(), BoxZoomTool(), CrosshairTool(), hover, WheelZoomTool(), ZoomInTool(), ZoomOutTool(), ResetTool(), SaveTool()]
    ''' Set the values to show with hover: '''
    output_file(out_file_name, title="Results of {}".format(data_title))
    geo_plt = figure(title="GeoMesa--{}".format(data_title), tools=tools, plot_width=600, plot_height=600)
    geo_plt.title.align = 'center'
    geo_plt.toolbar.logo = None
    geo_plt.toolbar_sticky = False
    geo_plt.xaxis.axis_label = "Lat"
    geo_plt.yaxis.axis_label = "Long"
    geo_plt.y_range = Range1d(start=min(y)-offset, end=max(y)+offset)
    geo_plt.x_range = Range1d(start=min(x)-offset, end=max(x)+offset)
    geo_plt.square("x", "y", fill_color="red", line_color=None, source=data_source, size=pt_size)
    show(geo_plt)
    reset_output()
