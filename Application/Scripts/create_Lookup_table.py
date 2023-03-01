pym = r'C:\Users\rmagala\OneDrive\Simulation_Application\Scripts'
#pym = r'C:\Users\rmagala\Box\ACPF_MyProject\APSIM scripting data\pyapsimx\python scripts'
import sys
sys.path.append(pym)
import glob
import os
import utils1
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
import runmx
import logging
import Utilities
logger = logging.getLogger(__name__)
# create fishnets for simulation
points, arrayfc = Utilities.create_fishnet()
jsonfolder = os.path.join(os.getcwd(), "APSIMSimulationFiles") 
sim_info = os.path.join(jsonfolder, "simulation_info.json")
simf =sim_info 
with open(simf, "r+") as sim:
   info = json.load(sim)
# extract the information
start =info['start']
end  = info['end']
crops = info["crops"] 
basefile = info["Apsmx_basefile"] 
ht = info['soilhorizontable']
hosoilhorizontable = ht
in_raster = arcpy.Raster(info["soil_raster"])
ws = info['workspace']
