# -*- coding: utf-8 -*-

import arcpy
import traceback
import sys
import time
import copy
import os
from platform import python_version
from arcpy.sa import *
import openpyxl
import winsound
import platform
import arcpy
pym = r'C:\Users\rmagala\OneDrive\Simulation_Application\Papsimx\Scripts'
pym1 = r'C:\Users\rmagala\OneDrive\Simulation_Application\Papsimx'
#pym = r'C:\Users\rmagala\Box\ACPF_MyProject\APSIM scripting data\pyapsimx\python scripts'
import sys
sys.path.append(pym)
sys.path.append(pym1)
import utils2
import traceback
import utils1
import multiprocessing             
import pyweather11
import APSIMrun
import weather2
from pyweather11 import daymet_bylocation
from pysoil4 import Replace_Soilprofile2
import pysoil4
from APSIMrun import runAPSIM2
from weather2 import Weather2
import numpy as np
import arcpy
import time
import glob
from multiprocessing import Pool, freeze_support
import multiprocessing as mp
import utils1
import utilsx
import utilsxy
import glob
import subprocess
import pandas as pd
import shutil
import json
import cropmanager
import re
#from pyproj import transform, CRS, Transformer                    
class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "APSIM Toolbox"
        self.alias = "toolbox"
        # List of tool classes associated with this toolbox
        self.tools = [APSIMCropSimulationTool]


class APSIMCropSimulationTool(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "APSIM Next Generation Simulations"
        self.description = "This tool can be used to set up APSIM and map the simulations results on a watershed scale"
        self.canRunInBackground = False
  
    def getParameterInfo(self):
        """Define parameter definitions"""
        
        param0 = arcpy.Parameter(
            # Input workspace
            displayName="Insert input workspace",
            name="in_workspace",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input")
        param0.defaultEnvironmentName = "workspace"
        param0.value = r'C:\Users\rmagala\Box\ACPF_MyProject\ACPF_DATA'
        param1 = arcpy.Parameter(
            # Input workspace
            displayName="Insert APSIMX simulation base file",
            name="apsimxbasefile",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        param1.filter.list = ["apsimx"]
        param1.value =r'C:\Users\rmagala\Box\corn.apsimx'
        param2 = arcpy.Parameter(
            # Soil raster layer
            displayName = "Insert soil raster Layer downloaded from SSURGO",
            name = 'soil_rasterx',
            datatype = "DERasterDataset",
            parameterType = "Required",
            direction = "Input")
        param2.value = r'C:\Users\rmagala\Box\ACPF_MyProject\ACPF_DATA\sievers_case_study\soilMudCreek_20221110\RMagala_20221110\soils_MudCreek_Sievers.gdb\gSSURGO'
        #param1.filter.list = ["tif"]
        param3 = arcpy.Parameter(
            # Soil raster layer
            displayName = "Insert soil raster table for the soil horizon downloaded from SSURGO",
            name = 'table_raster',
            datatype = "DETable",
            parameterType = "Required",
            direction = "Input")
        param3.value = r'C:\Users\rmagala\Box\ACPF_MyProject\ACPF_DATA\sievers_case_study\soilMudCreek_20221110\RMagala_20221110\soils_MudCreek_Sievers.gdb\SurfHrz070801030303'
        param4 = arcpy.Parameter(
            # Soil raster layer
            displayName = "Determine the cropping system (For crop rotations, crops should be written with coma seperated format following their order of rotation)",
            name = 'soil_crpping',
            datatype = "GPString",
            parameterType = "Optional",
            direction = "Input")
        #param1.filter.list = ["tif"]
        # start year
        param5 = arcpy.Parameter(
            displayName = "Specify The year Starting the Simulation",
            name = 'start_year',
            datatype = "GPLong",
            parameterType = "Required",
            direction = "Input")
        param5.value = 1985
        
        # end year
        param6 = arcpy.Parameter(
            displayName = "Specify The Year Ending the Simulation",
            name = 'end_year',
            datatype = "GPLong",
            parameterType = "Required",
            direction = "Input")
        param6.value = 2021
        # distance alagarithm
        param7 = arcpy.Parameter(
            displayName = "Specify Report Name",
            name = 'report',
            datatype = "GPString",
            parameterType = "Required",
            direction = "Input")
        param7.value = 'P_3051_T'
        # output file
        # read simulation database
        param8 = arcpy.Parameter(
            displayName = "Please Specify whether we should delete the simulation files including the apsimx, databases and met files after the simulation",
            name = 'readdb',
            datatype = "GPBoolean",
            parameterType = "Required",
            direction = "Output")
        param8.value = 'false'
        # mapping field
        param9 = arcpy.Parameter(
            displayName = "Output Features name for visualisation",
            name = 'output_features2',
            datatype = "DERasterDataset",
            parameterType = "Optional",
            direction = "Output")
        param10 = arcpy.Parameter(
            displayName = "Name of the simulation results data frame(csv output)",
            name = 'output_features3',
            datatype = "DERasterDataset",
            parameterType = "Optional",
            direction = "Output")
        param11 = arcpy.Parameter(
            displayName = "Turn on completion notification sound windows only (A sound will be played when the tool has been excuted successfully. The default is no sound)",
            name = 'muteparam',
            datatype = "GPBoolean",
            parameterType = "Required",
            direction = "Input")
        param11.value = 'false'
        param12 = arcpy.Parameter(
            displayName = "Select the percentage of cores on your computer to be used in parallel processing",
            name = 'process-cores',
            datatype = "GPString",
            parameterType = "Required",
            direction = "Input")
        param12.value = "80%"
        parameters = [param0, param1, param2, param3, param4,  param5, param6, param7, param8, param9, param10, param11, param12]
        return parameters

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        arcpy.AddMessage("Using Python Version {0}: please check for any incopatibility issue".format(python_version()))
        try:
            starttime = time.perf_counter()
            arcpy.SetProgressor('step', 'collecting user information...')
            arcpy.AddMessage('setting up the required inputs and environmental geoprocessing')
            # set up all the inputs
            arcpy.SetProgressorLabel("Collecting user inputs")
            arcpy.SetProgressorPosition()
            ws = parameters[0].valueAsText
            basefile = parameters[1].valueAsText
            in_raster = parameters[2].valueAsText
            table = parameters[3].valueAsText
            crops = parameters[4].valueAsText
            start =parameters[5].valueAsText
            end =  parameters[6].valueAsText
            report = parameters[7].valueAsText
            cl = parameters[8].valueAsText
            visualisation  = parameters[9].valueAsText
            results = parameters[10].valueAsText
            sound = parameters[11].valueAsText
            cores = parameters[12].valueAsText
            arcpy.AddMessage("You exported {0} file as a base simulation file".format(basefile))
            startyear = int(start)
            endyear = int(end)
            df = {}
            #cr = re.sub(",", '', crops).split() 
            arcpy.AddMessage(type(crops))
            crop_edited = cropmanager.InsertCroppingSystems(basefile, param = crops)
            df["start"] = startyear
            df["crops"] = crops
            df["end"] = endyear
            df["Apsmx_basefile"] = crop_edited
            os.startfile(crop_edited)
            df['soilhorizontable'] = table
            df['workspace'] = ws
            df["soil_raster"] = in_raster
            df["Cleanup"] = cl
            df["cores"] = cores
            df_dumb = json.dumps(df)
            arcpy.AddMessage(df)
            os.chdir(ws)
            folder = "APSIMSimulationFiles"
            if not os.path.exists(folder):
              os.mkdir(folder)
            jsonfolder = os.path.join(os.getcwd(), folder)
            sim_info = os.path.join(jsonfolder, "simulation_info.json")
            with open(sim_info, "w+") as jsf:
              jsf.write(df_dumb)
            arcpy.AddMessage(sim_info)
            with open(sim_info, "r+") as op:
              pp  = json.load(op)
            # set up the environment
            arcpy.env.scratchWorkspace = 'in_memory'
            arcpy.AddMessage('scratch workspace is in: ' + str(arcpy.env.scratchWorkspace))
            arcpy.env.overwriteOutput = True
            arcpy.env.workspace = ws
            os.chdir(ws)
            arcpy.AddMessage(os.path.exists('scriptrunx.py'))
            arcpy.AddMessage("++++++++++++++++++++==++++++++++++==================")
            # Set the output cordnate system
            arcpy.env.outputCoordinateSystem = arcpy.Describe(in_raster).spatialReference
            # Display to the user the out put corndate system being used
            arcpy.AddMessage("Your Geoprocessing spatial cordnate system has been reset to {0}:".format(arcpy.Describe(in_raster).spatialReference.name))
            #arcpy.SetProgressorLabel('Processing please wait......'
            cleanup = cl
            arcpy.AddMessage("Communicating with the simulator program")
            os.chdir(ws)
            torun = os.path.join(os.getcwd(), "mainapp.py")
            cmd = '"' + os.path.join(sys.exec_prefix, f'python.exe" "{torun}"')
            st = time.perf_counter()
            try:
               completed = subprocess.run(cmd, shell=False, check=True, capture_output=False)
            except subprocess.CalledProcessError as scripterror:
               arcpy.AddMessage(scripterror)
               tb = sys.exc_info()[2]
               tbinfo = traceback.format_tb(tb)[0]
               pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
               arcpy.AddMessage(pymsg + "\n")
            arcpy.AddMessage(os.getcwd())
            csvfile = os.path.join(ws,"SimulationResults")
            fd = pd.read_csv(os.path.join(csvfile,"resultsexternal.csv"))
            arcpy.AddMessage(fd)
            en = time.perf_counter()
            arcpy.AddMessage(f'External script run took: {en-st} seconds')
            if cleanup == 'true':
                    utils1.CLeaUp(ws)
                    arcpy.AddMessage("Done*****") 
                #pain(data, runapsimx)
            # produce sound when the tool ran successfully
            arcpy.AddMessage("Sucess!************** check out the output in the map window")
            if sound =='true' and "Windows"in platform.platform():
                     winsound.Beep(4500, 1500)
            arcpy.AddMessage("Sucess!**************")
        except:
             # traceback error objects
            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]

            # Concatenate information together concerning the error into a message string
            pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
            arcpy.AddMessage(pymsg + "\n")

            if arcpy.GetMessages(2) not in pymsg:
                msgs = "ArcPy ERRORS:\n" + arcpy.GetMessages(2) + "\n"
                arcpy.AddError(msgs)
                arcpy.AddMessage(msgs)
        finally:
            endtime = time.perf_counter()
            arcpy.AddMessage(f'Watershed Simulation took: {endtime-starttime} seconds')
    

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
