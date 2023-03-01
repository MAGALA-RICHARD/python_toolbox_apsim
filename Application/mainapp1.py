# import local modules
from os.path import dirname
root_dir = dirname(__file__)
from os.path import join as opj
all_scripts  = opj(root_dir, 'Scripts')
import sys
sys.path.append(root_dir)
sys.path.append(all_scripts)
import glob
import os
import Utilities
from cropmanager import  InsertCroppingSystems
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
import configurationModule
import logging
import Utilities
import copy
# this module is for debuging
import pandas as pd
logger = logging.getLogger(__name__)
#Set up imports of the modules
jsonfolder = os.path.join(root_dir, "APSIMSimulationFiles") 
sim_info = os.path.join(jsonfolder, "simulation_info.json")
simf = sim_info 
with open(simf, "r+") as sim:
   info = json.load(sim)
# extract the information
report_name, start, end, crops =info['Results_name'], info['start'], info['end'], info["crops"]
basefile, hosoilhorizontable = info["Apsmx_basefile"], info['soilhorizontable']

if os.path.isfile(basefile):
  print("congs! APSIMX file exists")
else:
  print("apsimx file does not exists")
  
os.chdir(info['workspace'])


in_raster = arcpy.Raster(info["soil_raster"])

soilinfo = configurationModule.collectsoilinfo(in_raster, horizontable = hosoilhorizontable)
array1 = soilinfo[:20]
arr = soilinfo[['OBJECTID','Shape', 'feature_to_point_Id',  'feature_to_point_gridcode']]
array = array1[['feature_to_point_gridcode', "Shape", 'OBJECTID']]
iterable_values = list(np.arange(len(array)))

track_failures = []

def MainrunforMP(index):
    #this index is gonna come from the list a
    try:
        pr = configurationModule.worker(index, basefile, start, end, array)
        return configurationModule.runapsimx(pr, crops)
    except Exception as e:
        logger.exception(f'{repr(e)}')
  

def delete_simulation_files(path):
  weather_files_path = opj(path, 'weatherdata')
  weather_files = glob.glob1(weather_files_path, 'weather_Daymet*.met')
  patterns = ['*lat*lon*.csv', 'np*.txt', 'edit*.apsimx', 'edit*.db-shm', 'edit*.db-wal', 'edit*.db']
  localfiles = [glob.glob1(path, pat) for pat in patterns]
  files_to_delete = []
  for file_group in localfiles:
    absolute_paths = [opj(path, file) for file in file_group]
    files_to_delete.extend(absolute_paths)
  for i in files_to_delete:
    try:
      os.remove(i)
    except PermissionError as e:
      print(repr(e))
def delete_weather_files(path):
  weather_files_path = opj(path, 'weatherdata')
  weather_files = glob.glob1(weather_files_path, 'weath*.met')
  files_2_delete = [opj(weather_files_path, fi) for fi in weather_files]
  for i in files_2_delete:
    try:
      os.remove(i)
    except PermissionError as e:
      print(repr(e))      

def save_simulation_results(result_list):
  data_df = Utilities.makedf(result_list)
  print(data_df.head())
  print(data_df.shape)
  base_sim_path  = os.path.join(os.getcwd(), "SimulationResults")
  if not os.path.exists(base_sim_path):
          os.mkdir(base_sim_path)
  results_csv = os.path.join(base_sim_path, report_name)
  if os.path.isfile(results_csv):
      os.remove(results_csv)
  results_csv = os.path.join(base_sim_path, report_name)
  data_df.to_csv(results_csv)
      
no_of_cores = info["cores"]
core = int(no_of_cores.split("%")[0])/100
cores = math.floor(mp.cpu_count() *core)
# pp =[MainrunforMP(i) for i in range(4)]
# print(pp)
# sys.exit()
if __name__ == "__main__":
    
    mp.set_executable(os.path.join(sys.exec_prefix, 'python.exe'))
    # run and replace weather files
    st = time.perf_counter()
    print("downloading weather files")
    en = time.perf_counter()
    results = None
    with Pool(processes=cores) as poolmp:
      results = poolmp.map(MainrunforMP,  iterable_values)#chunksize= n_tasks_per_chunk
      # print(Utilities.makedf(results))
    save_simulation_results(results)
    en = time.perf_counter()
    
    print(f"simulation took: {st-en}")
    delete_simulation_files(os.getcwd())
    delete_weather_files(os.getcwd())
    if  "Windows"in platform.platform():
        winsound.Beep(2000, 2000)
   
