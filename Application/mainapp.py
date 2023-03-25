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
import createfishnets
import tempfile
import shutil

dirpath = tempfile.mkdtemp()
# ... do stuff with dirpath

# this module is for debuging
import pandas as pd
logger = logging.getLogger(__name__)
arcpy.ResetEnvironments()
arcpy.env.overwriteOutput = True


#Set up imports of the modules
jsonfolder = os.path.join(root_dir, "APSIMSimulationFiles") 
sim_info = os.path.join(jsonfolder, "simulation_info.json")
simf = sim_info 
with open(simf, "r+") as sim:
   info = json.load(sim)
# extract the information
start, end, crops, rn = info['start'], info['end'], info["crops"], info["rname"]
basefile, watershedfcl =  info["Apsmx_basefile"], info['fc']
resolution   =  info["cell_res"]
print(crops)
print(basefile)
if os.path.isfile(basefile):
  print("congs! APSIMX file exists")
else:
  print("apsimx file does not exists")
# extract user sepcifies directory
ws = info['workspace']
os.chdir(info['workspace'])
print("creatign a feature class fishnets")
os.chdir(dirpath)
array, path2points, featurelayer = createfishnets.create_fishnet(watershedfcl, height = resolution)
os.chdir(info['workspace'])


if info['test'] == 'true':
   array = array[:10]
else:
  array = array
iterable_values = list(np.arange(len(array)))

track_failures = []

def MainrunforMP(index):
    #this index is gonna come from the list a
    try:
        pr = None
        pr = configurationModule.worker(index, basefile, start, end, array)
        print(pr)
        report = None
        report = configurationModule.runapsimx(pr, crops)
        #print(report)
        return report
    except Exception as e:
        logger.exception(f'{repr(e)}')
  

def delete_simulation_files(path):
  weather_files_path = opj(path, 'weatherdata')
  weather_files = glob.glob1(weather_files_path, 'weather_Daymet*.met')
  patterns = ['*lat*lon*.csv', 'np*.txt', 'edit*.apsimx', 'edit*.db-shm', 'edit*.db-wal', 'edit*.db', 'coh*.db-wal', 'edit*.bak', 'coh*.apsimx', 'pysoil*.txt', 'run*.txt', ]
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
#pp =[MainrunforMP(i) for i in range(4)]
# print(pp)
# sys.exit()
run_async = info['asyncro'] 
data =None
data = []
def log_result(result):
    # This is called whenever MainrunforMP(i) returns a result.
    # result_list is modified only by the main process, not the pool workers.
    data.append(result)
def asyncrun_function():
    with mp.Pool(processes=cores) as pool:
      for i in iterable_values:
        pool.apply_async(MainrunforMP, args = (i, ), callback = log_result)
      pool.close()
      pool.join()
      save_simulation_results(data)
if __name__ == "__main__":
    
    mp.set_executable(os.path.join(sys.exec_prefix, 'python.exe'))
    # run and replace weather files
    st = time.perf_counter()
    print("running please wait")
    en = time.perf_counter()
    if run_async:
      print('Running in asynchronous mode')
      asyncrun_function()
    else:
      print('Running in synchronous mapping mode')
      results = None
      with Pool(processes=cores) as poolmp:
        results = poolmp.map(MainrunforMP,  iterable_values)#chunksize= n_tasks_per_chunk
        # print(Utilities.makedf(results))
      save_simulation_results(results)
      en = time.perf_counter()
    
    print(f"simulation took: {st-en}")
    delete_simulation_files(os.getcwd())
    delete_weather_files(os.getcwd())
    # clean the root directory
    #delete_simulation_files(root_dir)
    shutil.rmtree(dirpath)
    if  "Windows"in platform.platform():
        winsound.Beep(2000, 2000)
   
