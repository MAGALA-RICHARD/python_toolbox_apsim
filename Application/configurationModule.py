# import modules and local scripts
from os.path import dirname
import os
import sys
py_path  = dirname(__file__)
sys.path.append(py_path)
pypath_scripts = os.path.join(py_path, 'Scripts')
sys.path.append(pypath_scripts)
import glob
import Utilities
import arcpy
from multiprocessing import Pool, freeze_support
import multiprocessing as mp
from Model import runAPSIM2
import numpy as np
import time
from soilmanager import Replace_Soilprofile2
from Weather_download import daymet_bylocation
from weather_manager import Weather2
import copy
import traceback
import json
import math
import winsound
import platform
import sys
import shutil
import logging
import subprocess
from datetime import datetime
from os.path import join as opj
import time
import re


# set up logging message 
logfile = os.path.join(os.getcwd(), 'logged_message.log')
if os.path.exists(logfile):
  os.remove(logfile)
# get the directory name of the path
basedir = os.path.dirname(__file__)
Logs  = "Logs"
log_messages = opj(basedir, Logs)
if not os.path.exists(log_messages):
  os.mkdir(log_messages)
datime_now = datetime.now()
timestamp = datime_now.strftime('%a-%m-%y')
logfile_name = 'log_messages' + str(timestamp)+ ".log"
log_paths = opj(log_messages,logfile_name)
#f"log_messages{datetime.now().strftime('%m_%d')}.log"
logging.basicConfig(filename=log_paths, level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)
# collect user info from the toolbox by reading the dropped json object

    
def collectsoilinfo(in_raster, horizontable):
        soils= Utilities.SoilRasterManagement(in_raster, horizontable)
        soilinfo = soils.Organise_soils()
        ar = soilinfo.feature_array
        return ar
      
      
def worker(index, basefile, start, end, array):
  try:
        basefile = basefile
        long_lat = array[index][1]
        try:
          path2apsimx = Replace_Soilprofile2(basefile, 'domtcp', long_lat, filename = array[index][2], gridcode = str(array[index][0]), Objectid = str(array[index][2]), crop = None)
          # download weather data from daymet returns path2file
        except ValueError as e:
          logger.exception(f"{repr(e)} at long lat: {long_lat}")
          raise
        
        weatherpath = daymet_bylocation(long_lat, start, end)
        wp = Weather2(path2apsimx, weatherpath, start, end)
        met_files = wp.ReplaceWeatherData()
        print(met_files)
        return met_files
  except Exception as e:
        logger.exception(repr(e))
        raise
       

def DownloadMultipleweatherfiles(function, variables):
        row_count  = len(variables)
        wvar = []
        for i in variables:
            var = wvar.append(function(i))
            print('downloading soils and weather for #', str(i))
        #mpweather= list(map(function, variables))
        listmp = []
        for i in wvar:
                if i != None:
                  listmp.append(i)
        return listmp
      
df =None
def Collect_for_maize_ryecover(apsimx): 
  try:
        df =None 
        df = {}
        dat = None
        dat = runAPSIM2(apsimx)
        df['OBJECTID'] = dat["MaizeR"].OBJECTID.values[0]
        df["CompName"] = dat["MaizeR"].soiltype.values[1].split(":")[0]
        df["Soiltype"] = dat["MaizeR"].soiltype.values[1]
        df["MUKEY"] = dat["MaizeR"].soiltype.values[1].split(":")[1]
        df['meanMaizeYield']= dat["MaizeR"].Yield.mean()
        df['meanMaizeAGB'] =  dat["MaizeR"].AGB.mean()
        df['longitude'] = dat["MaizeR"].longitude.values[1]
        df["Latitude"] = dat["MaizeR"].latitude[0]
        df["ChangeINCarbon"] = None
        df["ChangeINCarbon"]    = dat['Carbon1'].carbon[0]
        df["RyeBiomass"] = dat["WheatR"].AGB.mean()
        df["CO2"] = dat['Annual'].Top_respiration.mean()
        df["meanSOC1"] = dat['Annual'].SOC1.mean()
        df["meanSOC2"] = dat['Annual'].SOC2.mean()
        df['meanN20'] = dat["MaizeR"].TopN2O.mean()
        df['leached_nitrogen']  = dat['Annual'].CumulativeAnnualLeaching.mean()
        df['MineralN']  = dat['Annual'].MineralN.mean()
        return df
  except Exception as e:
    logger.exception(repr(e))
    logger.exception(f"value of columns {dat.columns.names}")
    
   
def Collect_for_maize_soybean_ryecover(apsimx): 
  try:
        df =None
        df = {}
        report = "MaizeR"
        dat = runAPSIM2(apsimx)
        df['OBJECTID'] = dat["MaizeR"].OBJECTID.values[0]
        df["CompName"] = dat["MaizeR"].soiltype.values[1].split(":")[0]
        df["Soiltype"] = dat["MaizeR"].soiltype.values[1]
        df["MUKEY"] = dat["MaizeR"].soiltype.values[1].split(":")[1]
        df['meanMaizeYield']= dat["MaizeR"].Yield.mean()
        df['meanMaizeAGB'] =  dat["MaizeR"].AGB.mean()
        df['longitude'] = dat["MaizeR"].longitude.values[1]
        df["Latitude"] = dat["MaizeR"].latitude[0]
        df["ChangeINCarbon"] = None
        df["ChangeINCarbon"]    =dat['Carbon'].changeincarbon[0]
        df["RyeBiomass"] = dat["WheatR"].AGB.mean()
        df["CO2"] = dat['Annual'].Top_respiration.mean()
        df["meanSOC1"] = dat['Annual'].SOC1.mean()
        df["meanSOC2"] = dat['Annual'].SOC2.mean()
        df['meanN20'] = dat["MaizeR"].TopN2O.mean()
        df['leached_nitrogen']  = dat['Annual'].CumulativeAnnualLeaching.mean()
        df['MineralN']  = dat['Annual'].MineralN.mean()
        df['meanSoybeanYield']= dat["SoybeanR"].Yield.mean()
        return df
  except Exception as e:
       logger.exception(repr(e))
       logger.exception(f"value of columns {dat.columns.names}")
       
CollectforMaize_only = None      
def CollectforMaize_only(apsimx_file):
  try:  
        df =None
        df = {}
        dat = None
        dat = runAPSIM2(apsimx_file)
        df['OBJECTID'] = dat["MaizeR"].OBJECTID.values[0]
        df["CompName"] = dat["MaizeR"].soiltype.values[1].split(":")[0]
        df["Soiltype"] = dat["MaizeR"].soiltype.values[1]
        df["MUKEY"] = dat["MaizeR"].soiltype.values[1].split(":")[1]
        df['meanMaizeYield']= dat["MaizeR"].Yield.mean()
        df['meanMaizeAGB'] =  dat["MaizeR"].AGB.mean()
        df['longitude'] = dat["MaizeR"].longitude.values[1]
        df["Latitude"] = dat["MaizeR"].latitude[0]
        df["CO2"] = dat['Annual'].Top_respiration.mean()
        df["meanSOC1"] = dat['Annual'].SOC1.mean()
        df["meanSOC2"] = dat['Annual'].SOC2.mean()
        df['meanN20'] = dat["MaizeR"].TopN2O.mean()
        df["ChangeINCarbon"]    =dat['Carbon'].changeincarbon[0]
        df['leached_nitrogen']  = dat['Annual'].CumulativeAnnualLeaching.mean()
        df['MineralN']  = dat['Annual'].MineralN.mean()
        return df
  except Exception as e:
     logger.exception(repr(e))
     logger.exception(f"value of columns {dat.columns.names}")
     
df =None     
def CollectforMaize_soybean_no_rye(apsimx_file):
  try:
        df = {}
        report = "MaizeR"
        dat = runAPSIM2(apsimx_file)
        df['OBJECTID'] = dat["MaizeR"].OBJECTID.values[0]
        df["CompName"] = dat["MaizeR"].soiltype.values[1].split(":")[0]
        df["Soiltype"] = dat["MaizeR"].soiltype.values[1]
        df["MUKEY"] = dat["MaizeR"].soiltype.values[1].split(":")[1]
        df['meanMaizeYield']= dat["MaizeR"].Yield.mean()
        df['meanMaizeAGB'] =  dat["MaizeR"].AGB.mean()
        df['longitude'] = dat["MaizeR"].longitude.values[1]
        df["Latitude"] = dat["MaizeR"].latitude[0]
        df["MUKEY"] = dat["MaizeR"].soiltype.values[1].split(":")[1]
        df["CO2"] = dat['Annual'].Top_respiration.mean()
        df["meanSOC1"] = dat['Annual'].SOC1.mean()
        df["meanSOC2"] = dat['Annual'].SOC2.mean()
        df['meanN20'] = dat["MaizeR"].TopN2O.mean()
        df["ChangeINCarbon"]    =dat['Carbon'].changeincarbon[0]
        df['leached_nitrogen']  = dat['Annual'].CumulativeAnnualLeaching.mean()
        df['MineralN']  = dat['Annual'].MineralN.mean()
        df['meanSoybeanYield']= dat["SoybeanR"].Yield.mean()
        return df
  except Exception as e:
     logger.exception(repr(e))
     raise e
     
     
def runapsimx(apsimx_file, crops): 
  
  result = None
  if crops == "Maize, Wheat, Soybean":
    result = Collect_for_maize_soybean_ryecover(apsimx_file)
  elif crops == "Maize, Soybean":
    result = None
    result = CollectforMaize_soybean_no_rye(apsimx_file)
  elif crops == 'Maize, Wheat':
    result = None
    result = Collect_for_maize_ryecover(apsimx_file)
  elif crops == "Maize":
    result = None
    result = CollectforMaize_only(apsimx_file)
  else:
    raise AttributeError(f"Crop provided {crops} is not supported")
  return result
  # dat = Utilities.makedf(result)

def run_multiple_scenarios(apsimx_file, dict_data):
  crops = None
  for crops in dict_data:
      runapsimx(apsimx_file, crops)

def convert_results_tofeatureclass(dat,  srf = 4326):
  '''
  paramters:
  ---------------------
  dat: is a panda data frame
  srf = spatial reference name or code
  ------------------------
  '''
  geodata = 'Gis_result_geodatabase.gdb'
  arcpy.env.workspace = os.path.join(os.getcwd(), geodata)
  if not arcpy.Exists(geodata):
       arcpy.CreateFileGDB_management(os.getcwd(), geodata)
  point_feature_class = 'results_point_feature_class'
  data = dat #dat.groupby(['CompName']).mean()
  if arcpy.Exists(point_feature_class):
    arcpy.management.Delete(point_feature_class)
  fd  = data.convert_dtypes()
  structuredNumpy_records = data.to_records(index= False)
  fc_name = os.path.join(geodata, point_feature_class)
  srf = arcpy.SpatialReference(srf)
  # change some unassigned data type
  dt = structuredNumpy_records.dtype.descr
  for idd, elem  in zip(range(len(dt)), dt):
    if 'Soiltype' in elem:
      dt[idd] = ('Soiltype', '<U25')
    elif 'CompName' in elem:
      dt[idd] = ('CompName', '<U25')
    else:
      pass
  
  featuretobe = structuredNumpy_records.astype(dt)
  arcpy.da.NumPyArrayToFeatureClass(featuretobe, fc_name, ["longitude","Latitude"], srf)
  
  return  fc_name


def create_results_raster_layer(dat, inraster, cellSize = 0.02, field ='meanMaizeAGB',  assignmentType = 'MOST_FREQUENT'):
  '''
  creates raster layer from point simulated data
  
  paramters
  ------------------
  inraster: same as the soil raster or similar to the simulated extent
  dat: pandas data frame
  field: field name to be used in create contnous raster data
  cellsise: size to calcualte the statics e.g mean
  -------------------------------
  returns a string path
  '''
  srf = 4326
  arcpy.env.snapRaster = inraster
  envcod  = arcpy.da.Describe(inraster)['spatialReference'].name
  arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(envcod)
  '''
  paramters:
  ---------------------
  dat: is a panda data frame
  srf = spatial reference name or code
  ------------------------
  '''
  geodata = 'Gis_result_geodatabase.gdb'
  arcpy.env.workspace = os.path.join(os.getcwd(), geodata)
  if not arcpy.Exists(geodata):
       arcpy.CreateFileGDB_management(os.getcwd(), geodata)
  point_feature_class = 'results_point_feature_class'
  ptfc = os.path.join(os.path.join(os.getcwd(), geodata), point_feature_class)
  data = dat #dat.groupby(['CompName']).mean()
  if arcpy.Exists(ptfc):
    arcpy.management.Delete(ptfc)
  fd  = data.convert_dtypes()
  structuredNumpy_records = data.to_records(index= False)
  fc_name = os.path.join(geodata, point_feature_class)
  srf = arcpy.SpatialReference(srf)
  # change some unassigned data type
  dt = structuredNumpy_records.dtype.descr
  for idd, elem  in zip(range(len(dt)), dt):
    if 'Soiltype' in elem:
      dt[idd] = ('Soiltype', '<U25')
    elif 'CompName' in elem:
      dt[idd] = ('CompName', '<U25')
    else:
      pass
  featuretobe = structuredNumpy_records.astype(dt)
  arcpy.da.NumPyArrayToFeatureClass(featuretobe, fc_name, ["longitude","Latitude"], envcod)
  inFeatures= fc_name 
  outRaster = field + "ras"
  if arcpy.Exists(outRaster):
    arcpy.management.Delete(outRaster)
  valField  = field
  priorityField = ''
  arcpy.PointToRaster_conversion(inFeatures, valField, outRaster, 
                               assignmentType, priorityField, cellSize)
                               
  return  outRaster
