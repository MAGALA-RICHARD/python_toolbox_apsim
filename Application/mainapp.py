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
arcpy.env.overwriteOutput = True
#Set up imports of the modules
jsonfolder = os.path.join(root_dir, "APSIMSimulationFiles") 
sim_info = os.path.join(jsonfolder, "simulation_info.json")
simf = sim_info 
with open(simf, "r+") as sim:
   info = json.load(sim)
# extract the information
start, end, crops, rn = info['start'], info['end'], info["crops"], info["rname"]
ws, basefile, watershedfcl =  info["Apsmx_basefile"], info['fc'], info['workspace']
resolution   =  info["cell_res"]
print(basefile)
if os.path.isfile(basefile):
  print("congs! APSIMX file exists")
else:
  print("apsimx file does not exists")
  
os.chdir(info['workspace'])
print("creatign a feature class fishnets")
array, path2points, featurelayer = configurationModule.create_fishnet(watershedfcl, height = resolution)
#print(array)
array = array
iterable_values = list(np.arange(len(array)))

track_failures = []

def MainrunforMP(index):
    #this index is gonna come from the list a
    pr = None
    try:
        pr = configurationModule.worker(index, basefile, start, end, array)
        report = None
        report = configurationModule.runapsimx(pr, crops)
        os.remove(pr.split(".apsimx")[0] + ".db")
        os.remove(pr)
        return report
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
  data_df = None
  data_df = Utilities.makedf(result_list)
  base_sim_path  = os.path.join(os.getcwd(), "SimulationResults")
  if not os.path.exists(base_sim_path):
          os.mkdir(base_sim_path)
  results_csv = os.path.join(base_sim_path, rn)
  if os.path.isfile(results_csv):
      os.remove(results_csv)
  results_csv = os.path.join(base_sim_path, rn)
  data_df.to_csv(results_csv)
      
no_of_cores = info["cores"]
core = int(no_of_cores.split("%")[0])/100
cores = math.floor(mp.cpu_count() *core)
# pp =[MainrunforMP(i) for i in range(4)]
# print(pp)
# sys.exit()
def map_results(path, watershedfc, inFeatures,  infield, jf):
      gis_results = os.path.join(path,"GIS_files")
      if not os.path.exists(gis_results):
          os.mkdir(gis_results)
        
      '''
      inFeatures: feature tto join the csvfiles
      jf: joinfield
      infield: infield of the Infeature
      '''
      watershedcode = str(watershedfc[-12:]) + ".csv"
      csv_names_list =[]
      for file in os.listdir(path):
          if file.endswith(watershedcode):
              csv_names_list.append(os.path.join(path, file))
      arcpy.AddMessage(csv_names_list)
      count_rows = len(csv_names_list) 
      i = 0
      arcpy.SetProgressor('step', 'Processing ...', 0, count_rows, i)
      arcpy.env.workspace = gis_results
      arcpy.env.overwriteOutput = True
      os.chdir(gis_results)
      for csv in csv_names_list:
          arcpy.SetProgressorLabel(f'mapping :  ' + str(csv))
          arcpy.SetProgressorPosition()
          arcpy.MakeFeatureLayer_management(in_features=inFeatures, out_layer='fclyr')
          arcpy.MakeTableView_management(in_table=csv, out_view='csvview')
          arcpy.AddJoin_management(in_layer_or_view='fclyr', in_field=infield, join_table='csvview', join_field=jf)
          arcpy.CopyFeatures_management(in_features='fclyr',out_feature_class=os.path.splitext(os.path.basename(csv))[0])
          arcpy.SetProgressorLabel("Updating  {0}...".format(csv))
arcpy.env.workspace = ws      
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
    
    map_results(ws,  watershedfcl, featurelayer, "FID", "OBJECTID")
    print(f"simulation took: {st-en}")
    delete_simulation_files(os.getcwd())
    delete_weather_files(os.getcwd())
    if  "Windows"in platform.platform():
        winsound.Beep(2000, 2000)
   
