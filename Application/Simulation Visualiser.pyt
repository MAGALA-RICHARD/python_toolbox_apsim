# -*- coding: utf-8 -*-
# Import the modules
import arcpy
import traceback
import copy
import os
from platform import python_version
from arcpy.sa import *
import winsound
import platform
from os.path import dirname
root_dir = dirname(__file__)
from os.path import join as opj
all_scripts  = opj(root_dir, 'Scripts')
import sys
sys.path.append(root_dir)
sys.path.append(all_scripts)
import Utilities
import Weather_download
from Model import runAPSIM2
from soilmanager import Replace_Soilprofile2
from Weather_download import daymet_bylocation
from weather_manager import Weather2
from Utilities import extract_param_table
import traceback            
import cropmanager
import numpy as np
import arcpy
import time
import glob
import glob
import subprocess
import pandas as pd
import shutil
import json
import re
import logging
import fmp


#from pyproj import transform, CRS, Transformer                    
class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "APSIM Toolbox"
        self.alias = "toolbox"
        # List of tool classes associated with this toolbox
        self.tools = [Visualizer]


class Visualizer(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "APSIM puthon toolbox simulation mapping"
        self.description = "This tool can be used to map python toolbox results"
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
        param0.value = r'C:\my_sim'
       
        param1 = arcpy.Parameter(
            # Input workspace
            displayName="Insert the simulated excel spread sheet file",
            name="apsimxbasefile",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        param1.filter.list = ["csv"]
        param1.value = r'C:\my_sim\SimulationResults\BearCreek070801050403_maize_rye.csv'
        param2 = arcpy.Parameter(
            # Soil raster layer
            displayName = "Insert soil raster Layer downloaded from SSURGO",
            name = 'soil_rasterx',
            datatype = "DERasterDataset",
            parameterType = "Required",
            direction = "Input")
        param2.value = r'C:\ACPd\Base_files\acpf_huc070801050403\acpf070801050403.gdb\gSSURGO'
        #param1.filter.list = ["tif"]
        param3 = arcpy.Parameter(
            # Soil raster layer
            displayName = "Insert Watersehd featurelayer buffered",
            name = 'table_raster',
            datatype = "GPFeatureLayer",
            parameterType = "Required",
            direction = "Input")
        param3.value = r'C:\ACPd\Base_files\acpf_huc070801050403\acpf070801050403.gdb\buf070801050403'
        param4 = arcpy.Parameter(
            displayName = "Select the variable name for raster creation",
            name = 'featureclass_name',
            datatype ='GPString',
            parameterType = "Optional",
            direction = "Input", 
            multiValue=True)
        param4.filter.type = "ValueList"
        param4.value = 'meanN20'
        param4.filter.list = ['meanN20', 'ChangeINCarbon', 'leached_nitrogen', 'meanMaizeYield', 'RyeBiomass']
        
        param5 = arcpy.Parameter(
            displayName = "Select the cell size",
            name = 'excel_results_name',
            datatype ='GPLong',
            parameterType = "Optional",
            direction = "Input")
        param5.value = 20
        param5.filter.list = [10, 20, 30, 40, 50, 100, 200, 250, 500, 1000, 1500, 2000, 2500]

        param6 = arcpy.Parameter(
            displayName = "Cell statistic Asignment type",
            name = 'cell_asignment_type',
            datatype ='GPString',
            parameterType = "Optional",
            direction = "Input",
            )
        param6.filter.type = "ValueList"
        param6.value = 'MEAN'
        param6.filter.list = ['MOST_FREQUENT', 'MEAN', 'SUM', 'MAXIMUM', 'MINIMUM', 'COUNT', 'RANGE', 'STANDARD_DEVIATION']
        
        param7 = arcpy.Parameter(
            displayName = "Turn on completion notification sound windows only (A sound will be played when the tool has been excuted successfully. The default is no sound)",
            name = 'muteparam',
            datatype = "GPBoolean",
            parameterType = "Required",
            direction = "Input")
        param7.value = 'false'
        param8 = arcpy.Parameter(
            displayName = "Select the percentage of cores on your computer to be used in parallel processing",
            name = 'process-cores',
            datatype = "GPString",
            parameterType = "Required",
            direction = "Input")
        param8.value = "80%"
        param8.filter.type = "ValueList"
        param8.filter.list = ["20%", "40%", "50%", "60%", "80%", '85%', "95%", "100%"]
        param9 = arcpy.Parameter(
            displayName = "Select the cell size",
            name = 'size',
            datatype ='GPLong',
            parameterType = "Optional",
            direction = "Input")
        param9.value = 30
        param9.filter.list = [0.02, 0.03, 10, 20, 30, 40, 50, 100, 200, 500, 1000, 1500, 2000, 2500]
        

        parameters = [param0, param1, param2, param3, param4,  param5, param6, param7, param8, param9]
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
        #lgname  = opj(ws, "logged_messages")
        #logging.basicConfig(filename=lgname, level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
        #logger = logging.getLogger(__name__)
        arcpy.AddMessage("Using Python Version {0}".format(python_version()))
        try:
            starttime = time.perf_counter()
            arcpy.SetProgressor('step', 'collecting user information...')
            arcpy.AddMessage('setting up the required inputs and environmental geoprocessing')
            # set up all the inputs
            arcpy.SetProgressorLabel("Collecting user inputs")
            arcpy.SetProgressorPosition()
            ws = parameters[0].valueAsText
            excel_file = parameters[1].valueAsText
            in_raster = parameters[2].valueAsText
            watershedfc = parameters[3].valueAsText
            mapping_field= parameters[4].valueAsText
            cellSize =int(parameters[5].valueAsText)
            cellstat =  parameters[6].valueAsText
            sound = parameters[7].valueAsText 
            cores= parameters[8].valueAsText 
            csize = parameters[9].valueAsText
            # set up the working environment
            arcpy.env.scratchWorkspace = 'in_memory'
            arcpy.env.overwriteOutput = True
            arcpy.env.workspace = ws
            arcpy.AddMessage('=================================================================================================')          
            os.chdir(ws)
            arcpy.env.parallelProcessingFactor = "85%"
            arcpy.AddMessage(f"working directory is set to: {ws}.")
            mf = mapping_field.split(";")

            arcpy.AddMessage(mf)
            arcpy.AddMessage("==============================================================")
            # Set the output cordnate system
            arcpy.env.outputCoordinateSystem = arcpy.Describe(watershedfc).spatialReference
            # Display to the user the out put corndate system being used
            # produce sound when the tool ran successfully
            
            ##============create======point feature =========class
            data  = pd.read_csv(excel_file)
            dat = data
            height = cellSize
            #create_pointfc(dat, watershedfc, height = 20):
            '''
            paramters:
            ---------------------
            dat: is a panda data frame
            srf = spatial reference name or code
            ------------------------
            '''
            geodata = 'Gis_result_geodatabase.gdb'
            #set the environment
            fname = 'fc' + watershedfc.split("\\")[-1:][0]
            fcname = 'fcpoints'
            arcpy.AddMessage(fname)
            arcpy.env.workspace = os.path.join(os.getcwd(), geodata)
            if not arcpy.Exists(geodata):
                arcpy.CreateFileGDB_management(os.getcwd(), geodata)
            envcod  = arcpy.da.Describe(watershedfc)['spatialReference'].name
            arcpy.AddMessage(envcod)
            arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(envcod)
            arcpy.env.overwriteOutput = True
            point_feature_class = fcname
            data = dat #dat.groupby(['CompName']).mean()
            if arcpy.Exists(point_feature_class):
                arcpy.management.Delete(point_feature_class)
            fd  = data.convert_dtypes()
            structuredNumpy_records = data.to_records(index= False)
            fc_name = os.path.join(geodata, fcname)
            srf = envcod
            # change some unassigned data type
            dt = structuredNumpy_records.dtype.descr
            for idd, elem  in zip(range(len(dt)), dt):
                if 'Soiltype' in elem:
                  dt[idd] = ('Soiltype', '<U25')
                elif 'CompName' in elem:
                   dt[idd] = ('CompName', '<U25')
                else:
                  pass
            spr = arcpy.SpatialReference(4326)  

            featuretobe = structuredNumpy_records.astype(dt)
            arcpy.da.NumPyArrayToFeatureClass(featuretobe, fc_name, ["longitude","Latitude"], spr)
            
            # create a fishnet
            templateExtent = watershedfc
            fc= templateExtent
            fcname = fname+ 'box'
            desc = arcpy.Describe(fc)
            in_feature_path = arcpy.CreateFishnet_management(fcname,str(desc.extent.lowerLeft),str(desc.extent.XMin) + " " + str(desc.extent.YMax + 10),
                f"{height}",f"{height}","0","0",str(desc.extent.upperRight),"NO_LABELS","#","POLYGON")
            #let's clip out the unwanted part according to the processing extent
            in_features = in_feature_path
            clip_features = watershedfc
            clipedname = fname +"clipedfc"
            fishnet_feature_class = os.path.join(geodata, clipedname)

            if arcpy.Exists(fishnet_feature_class):
                arcpy.management.Delete(fishnet_feature_class)
                #arcpy.
            xy_tolerance = ""
            # Execute Clip
            arcpy.Clip_analysis(in_features, clip_features, fishnet_feature_class, xy_tolerance)
            #return fc_name, fishnet_feature_class
            arcpy.AddMessage(arcpy.da.Describe(fishnet_feature_class)['spatialReference'].name)
            arcpy.AddMessage("============")
            arcpy.AddMessage(arcpy.da.Describe(fc_name)['spatialReference'].name)
            arcpy.AddMessage(arcpy.Exists(fc_name))
            arcpy.AddMessage(f'spatial reference for the point feature class is: {spr}')
            #fname, fishnets = create_pointfc(data, watershedfc, cellSize)
            def joinlayers():
                    target_features = fishnet_feature_class
                    join_features = fc_name
                    out_feature_class =  fishnet_feature_class +"join"
                    arcpy.analysis.SpatialJoin(target_features, join_features, out_feature_class, 'JOIN_ONE_TO_MANY')
                    return out_feature_class
            out_feature_class =  joinlayers()
            # map the results in a loop
            count_rows = len(mf)
            i = 0
            # set the progressor to display the progress
            arcpy.SetProgressor('step', 'Processing field output name...', 0, count_rows, 1)
            for fieldname in mf:
                arcpy.SetProgressorLabel('Rasterizing:  ' + fieldname)
                arcpy.SetProgressorPosition()
                value_field = fieldname
                out_rasterdataset =  fieldname
                cell_assignment = 'MAXIMUM_AREA'
                priority_field= 'TARGET_FID'
                cellsize  = csize
                in_features = out_feature_class 
                arcpy.conversion.PolygonToRaster(in_features, value_field, out_rasterdataset, cell_assignment, priority_field, cellsize)
                arcpy.SetProgressorLabel("Updating  {0}...".format(fieldname))
            arcpy.ResetProgressor()
            if sound =='true' and "Windows"in platform.platform():
                     winsound.Beep(4500, 1500)
                     arcpy.AddMessage("Succeeded!===================================")
            else:
                arcpy.AddMessage("Succeeded!===========================================")
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
            # just in case something drammatically happens before the end of the simulations we still need to clear work log files off the computer

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return

