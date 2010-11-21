"""configuration file for pythongrid"""
import os

CFG = {}

#default python path
CFG['PYTHONPATH'] = os.environ['PYTHONPATH']
CFG['PYGRID']     = "~/work/ibg/lib/pycluster/pythongrid/pythongrid.py"
CFG['TEMPDIR']    = "~/clusterout"
