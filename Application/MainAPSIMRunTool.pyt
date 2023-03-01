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
from Utilities import create_fishnet
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
from subprocess import Popen, PIPE

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
        param0.value = r'C:\my_sim'
        
        param1 = arcpy.Parameter(
            # Input workspace
            displayName="Insert APSIMX simulation base file",
            name="apsimxbasefile",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        param1.filter.list = ["apsimx"]
        param1.value =r'C:\BaseAPSIM\CORN.apsimx'
        param2 = arcpy.Parameter(
            # Soil raster layer
            displayName = "Insert feature class (extent of the target simulation area)",
            name = 'soil_feature',
            datatype = "GPFeatureLayer",
            parameterType = "Required",
            direction = "Input")
        param2.value = r'C:\ACPd\Base_files\acpf_huc070801050305\acpf070801050305.gdb\buf070801050305'
        #param1.filter.list = ["tif"]
        param3 = arcpy.Parameter(
            # Soil raster layer
            displayName = "Cell sampling resolution",
            name = 'table_raster',
            datatype = "GPLong",
            parameterType = "Required",
            direction = "Input")
        param3.value = 500

        param4 = arcpy.Parameter(
            # Soil raster layer
            displayName = "Determine the cropping system (For crop rotations, crops should be written with coma seperated format following their order of rotation)",
            name = 'soil_crpping',
            datatype = "GPString",
            parameterType = "Optional",
            direction = "Input",
            multiValue=True)
        param4.value = "Maize"
        param4.filter.type = "ValueList"
        param4.filter.list = ['Maize', 'Maize rye', 'Maize rye Soybean', 'Maize Soybean']

        
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
            displayName = "Do Not show The command line interface",
            name = 'report',
            datatype = "GPBoolean",
            parameterType = "Optional",
            direction = "Input")
        param7.value = 'false'
        # output file
        # read simulation database
        param8 = arcpy.Parameter(
            displayName = "Run the tools with multiple scenarios (Checked)",
            name = 'do_scenarios',
            datatype = "GPBoolean",
            parameterType = "Optional",
            direction = "Output")
               
        # mapping field
        param9 = arcpy.Parameter(
            displayName="Value Table",
            name="valuetable",
            datatype="GPValueTable",
            parameterType="Optional",
            
            )
        param9.columns  = [['GPString','Cropping system scenario'], ['GPDouble', 'Proportion of each scenario']]
        param9.filters[0].type = 'ValueList'
        param9.filters[1].type = 'ValueList'
        param9.filters[1].list = [0.1, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.65, .07, 0.8, 0.85, 0.9, 1]#, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75 0.8, 0.85, 0.9, 1]
        param9.filters[0].list = ['Maize', 'Maize rye', 'Maize rye Soybean', 'Maize Soybean']
        
            
        # param9.filters[1].list = ['ALL', 'MEAN', 'MAXIMUM', 'MINIMUM', 'RANGE', 'STD', 'SUM']
        param10 = arcpy.Parameter(
            displayName = "Maize Tillage depth",
            name = 'tilagedepth',
            datatype = "GPLong",
            parameterType = "Optional",
            direction = "Input")
        param10.filter.type = "ValueList"
        param10.value = 150
        param10.filter.list = [0, 1, 100, 150, 200, 250, 300, 350]
        param11 = arcpy.Parameter(
            displayName = "Select proportion of residues to retain at site",
            name = 'featureclass_name',
            datatype ='GPDouble',
            parameterType = "Optional",
            direction = "Input")
        param11.filter.type = "ValueList"
        param11.value = 1.0
        param11.filter.list = [0, 0.1, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.75,1]

        param12 = arcpy.Parameter(
            displayName = "Turn on completion notification sound windows only (A sound will be played when the tool has been excuted successfully. The default is no sound)",
            name = 'muteparam',
            datatype = "GPBoolean",
            parameterType = "Required",
            direction = "Input")
        param12.value = 'false'
        param13 = arcpy.Parameter(
            displayName = "Select the percentage of cores on your computer to be used in parallel processing",
            name = 'process-cores',
            datatype = "GPString",
            parameterType = "Required",
            direction = "Input")
        param13.value = "80%"
        param13.filter.type = "ValueList"
        param13.filter.list = ['5%', '10%',"20%", "40%", "50%", "60%", "80%", '85%', "95%", "100%"]

        
        
        param14 = arcpy.Parameter(
            displayName = "Amount of Nitrogen to be applied ",
            name = 'Nitrogen',
            datatype ='GPLong',
            parameterType = "Optional",
            direction = "Input")
        param14.value = 200
        

        param15 = arcpy.Parameter(
            displayName = "Date to apply Nitrogen to maize",
            name = 'date',
            datatype ='GPString',
            parameterType = "Optional",
            direction = "Input")
        param15.filter.type = "ValueList"
        param15.value = '30-may'
        
        parameters = [param0, param1, param2, param3, param4,  param5, param6, param7, param8, param9, param10, param11, param12, param13, param14, param15]
        return parameters

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        if parameters[8].value == False:
            parameters[9].enabled = 0
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        
        """The source code of the tool."""
        arcpy.AddMessage("Using Python Version {0}".format(python_version()))
        try:
            starttime = time.perf_counter()
            arcpy.SetProgressor('step', 'collecting user information...')
            arcpy.AddMessage('setting up the required inputs and environmental geoprocessing')
            # set up all the inputs
            arcpy.SetProgressorLabel("Collecting user inputs")
            arcpy.SetProgressorPosition()
            ws = parameters[0].valueAsText
            lgname = opj(ws, "logging_info")
            logging.basicConfig(filename=lgname, level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
            logger = logging.getLogger(__name__)
            basefile = parameters[1].valueAsText
            watershedfc = parameters[2].valueAsText
            resolution = parameters[3].valueAsText
            crops = parameters[4].valueAsText
            start =parameters[5].valueAsText
            end =  parameters[6].valueAsText
            cmi= parameters[7].valueAsText
            scenarios = parameters[8].valueAsText
            paramtable  = parameters[9].valueAsText
            depth = int(parameters[10].valueAsText)
            residue = float(parameters[11].valueAsText)
            sound = parameters[12].valueAsText
            cores = str(parameters[13].valueAsText)
            Nitrogen = int(parameters[14].valueAsText)
            fdate = parameters[15].valueAsText
            startyear = int(start)
            endyear = int(end)
            if cmi == 'true':
               shellvalue =True
            else: 
                shellvalue =False  
                      
            arcpy.AddMessage('=================================================================================================')

            if not os.path.isfile(basefile):
                arcpy.AddMessage("APSIMX file does not exist. kill the process and try again")
            else:
                arcpy.AddMessage("APSIMX file exists; we are continuing with the simulations")
            arcpy.AddMessage("*****************************************************************")
            os.chdir(root_dir)
            
            crops = crops.split(";")
            crop_bag = []
            crop = None
            dt = {}
            for crop in crops:
             if crop == "'Maize rye'":
                dt[1] = "Maize, Wheat"
             elif crop == "'Maize rye Soybean'":
                 dt[2]="Maize, Wheat, Soybean"

             elif crop  == "'Maize Soybean'":
                dt[3] = 'Maize, Soybean'
             else:
                dt[4] = crop
            tillageparameters = [residue, depth]
            key = None
            files = []
            dictionary2bejsoned = None
            
            dictionary2bejsoned = {}
            count_rows = len(crops)
            i = 0
            # set the progressor to display the progress
            arcpy.SetProgressor('step', 'Processing ...', 0, count_rows, i)
            for key in dt:
                arcpy.SetProgressorLabel(f'SIMULATING:  ' + dt[key])
                arcpy.SetProgressorPosition()
                apsimx_fl  = None
                cropp= dt[key]
                apsimx_fl = cropmanager.InsertCroppingSystems(basefile, param = cropp, NAmount = Nitrogen, fertilizerdate = fdate, tillage_param =tillageparameters, name = key)
                files.append(apsimx_fl)
                res_name = dt[key].replace(",", "").replace(" ", "_")
                dictionary2bejsoned["start"] = startyear
                dictionary2bejsoned["crops"] = None
                dictionary2bejsoned["crops"] = dt[key]
                dictionary2bejsoned["end"] = endyear
                dictionary2bejsoned["Apsmx_basefile"] =apsimx_fl
                dictionary2bejsoned['workspace'] = ws
                dictionary2bejsoned["cores"] = cores
                dictionary2bejsoned['rname'] = res_name + str(watershedfc[-12:]) + ".csv"
                dictionary2bejsoned["fc"]  = watershedfc
                dictionary2bejsoned["cell_res"]  = resolution
                df_dumb = json.dumps(dictionary2bejsoned)
                
                # create a summary of user inputs
                arcpy.AddMessage(f'Simulating: {dt[key]}')
                arcpy.AddMessage(f'Start date is : {startyear},  and end date is :{end}')
                arcpy.AddMessage(f'Nitrogen applied is: {Nitrogen} on  {fdate}')
                arcpy.AddMessage(f' Residue retained is: {residue}, incorporated at: {depth}')
                folder = "APSIMSimulationFiles"
                if not os.path.exists(folder):
                    os.mkdir(folder)
                jsonfolder = os.path.join(os.getcwd(), folder)
                sim_info = None
                sim_info = os.path.join(jsonfolder, "simulation_info.json")
                jsf =None
                with open(sim_info, "w+") as jsf:
                    jsf.write(df_dumb)
                with open(sim_info, "r+") as op:
                    pp  = json.load(op)
                del sim_info
                 
                    # set up the environment
                time.sleep(0.3)
                arcpy.env.scratchWorkspace = 'in_memory'
                arcpy.env.overwriteOutput = True
                arcpy.env.workspace = ws
                os.chdir(ws)
                arcpy.AddMessage(f"working directory is set to: {ws}.")
                arcpy.AddMessage("==============================================================")
                    # Set the output cordnate system
        
                arcpy.AddMessage("Communicating with the simulator program. Please wait")
                os.chdir(ws)
                torun = None
                torun = os.path.join(root_dir, "mainapp.py")
                cmd = None
                cmd = '"' + os.path.join(sys.exec_prefix, f'python.exe" "{torun}"')
                dm= os.path.join(root_dir, 'mainapp.py')
                st = time.perf_counter()
                os.chdir(root_dir)
                try:
                    #completed = subprocess.run(cmd, shell=False, check=True, capture_output=False)
                    process = subprocess.Popen(cmd, shell= shellvalue, stdout=PIPE, stderr=PIPE)
                    stdout, stderr = process.communicate()
                    logger.exception(stderr)
                    #arcpy.AddMessage(stderr)
                    #process = subprocess.Popen(['sys.exec_prefix', 'mainapp.py'], stdout=PIPE, stderr=PIPE)
                    
                    os.remove(apsimx_fl)  
                except subprocess.CalledProcessError as scripterror:
                        arcpy.AddMessage(scripterror)
                        tb = sys.exc_info()[2]
                        tbinfo = traceback.format_tb(tb)[0]
                        pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
                        arcpy.AddMessage(pymsg + "\n")
                results_folder = os.path.join(ws,"SimulationResults")
                results = os.path.join(results_folder, dictionary2bejsoned['rname'])
                fd = pd.read_csv(results)
                arcpy.AddMessage(fd.head)
                arcpy.AddMessage(fd.meanN20)
                arcpy.AddMessage(fd.shape)
                arcpy.SetProgressorLabel("Updating  {0}...".format(dt[key]))
            arcpy.ResetProgressor()
            array, path2points, featurelayer = Utilities.create_fishnet(watershedfc, height = resolution)
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
            arcpy.ResetProgressor()
            map_results(results_folder,  watershedfc, featurelayer, "OBJECTID", "OBJECTID")
            arcpy.AddMessage("==============================================================")
            en = time.perf_counter()
            arcpy.AddMessage(f'Simulations  took: {en-st} seconds')

            # produce sound when the tool ran successfully
            if sound =='true' and "Windows"in platform.platform():
                     winsound.Beep(4500, 1500)
            arcpy.AddMessage("Succeeded!======================================================")
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
            logger.exception(repr(pymsg))
            raise
            
        finally:
            endtime = time.perf_counter()
            arcpy.AddMessage(f'Simulations complete: [{endtime-starttime} seconds]')
            # just in case something drammatically happens before the end of the simulations we still need to clear work log files off the computer
            Utilities.delete_simulation_files(ws)
            Utilities.delete_weather_files(ws)

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
