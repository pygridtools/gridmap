"""configuration file for pythongrid"""
import os

CFG = {}

#default python path
CFG['PYTHONPATH'] = os.environ['PYTHONPATH']
CFG['PYGRID']     = "/fml/ag-raetsch/home/cwidmer/svn/tools/python/pythongrid/pythongrid.py"
CFG['TEMPDIR']    = "/fml/ag-raetsch/home/cwidmer/tmp/"
CFG['node_blacklist']    = []

