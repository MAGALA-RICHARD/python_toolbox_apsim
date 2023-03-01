import os
import json
import sys
import time
import logging
# example code for organising the parameters for the function below
logger = logging.getLogger(__name__)
def InsertCroppingSystems(path2apsimx, param, NAmount = 220, fertilizerdate = '30-may', tillage_param =[1, 150], name = "edited"):
      try:
        '''
        Replaces APASIMX soil properties
        
        parameters
        ------------
        tillage_param: a list with index zero representing tillage fraction and 1 depth
        path2apsimx: this is the path to apsimx file. it is a complete path to the file
        param: parameter for the simple rotation manager to replace the existing ones. It should be a tuple
        fertilizerdate: date to apply the fertilizers
        NAmount: Amount of fertilizers to be applied
        '''
        fraction, depth = tillage_param[0], tillage_param[1]
        tillage_param1 = [{'Key': 'Crop', 'Value': '[Maize]'}, {'Key': 'Fraction', 'Value': fraction}, {'Key': 'Depth', 'Value': depth}]
        fertilizer = [{'Key': 'Crop', 'Value': '[Maize]'}, {'Key': 'FertiliserType', 'Value': 'UreaN'}, {'Key': 'Fraction', 'Value': '1'}, {'Key': 'Amount', 'Value': NAmount},
                {'Key': 'FertiliserDates', 'Value': fertilizerdate}, {'Key': 'AmountType', 'Value': 'True'}]
        # first create an wempy list
        parameters = []
        dictvalue = {'Key': 'Crops', 'Value': 'Maize'}
        dictvalue["Value"] = param
        parameters.append(dictvalue)
        assert path2apsimx.endswith(".apsimx")
        assert os.path.isfile(path2apsimx)
        with open(path2apsimx, "r+") as apsimx:
              app_ap = json.load(apsimx) 
              # search for the Core simulation node
              # the challenge is that the nodes may not be in the correct oder everytime. so we loop through using enumeration fucntion
              for counter, root in enumerate(app_ap["Children"]):
                 if root['$type'] == 'Models.Core.Simulation, Models':
                   if not counter:
                    print("No core simulation node found")
                   else: 
                      coresimulationNode = counter
                      #print('searching for the main core simulation node')
                      for counter, root in enumerate(app_ap["Children"][coresimulationNode]["Children"]):
                            if root['$type'] == 'Models.Core.Zone, Models':
                              if not counter:
                                print("No field zone found: ", app_ap["Children"][coresimulationNode]["Name"])
                              else: 
                                  fieldzone = counter
                                  # remember zone has many nodes
                                  # now lets look for soils
                                  for counter, root in enumerate(app_ap["Children"][coresimulationNode]["Children"][fieldzone]["Children"]):
                                       if  root["Name"] == "Simple_Rotation":
                                         simple_rotation = counter
                                         
                                         #print(app_ap["Children"][coresimulationNode]["Children"][fieldzone]["Children"][simple_rotation]['Parameters'])
                                         app_ap["Children"][coresimulationNode]["Children"][fieldzone]["Children"][simple_rotation]['Parameters'] = parameters
                                  counter = None
                                  root = None
                                  for counter, root in enumerate(app_ap["Children"][coresimulationNode]["Children"][fieldzone]["Children"]):
                                    for key in root:
                                      if root[key] == "AddN_Maize":
                                         nitrogen_maize = counter
                                         app_ap["Children"][coresimulationNode]["Children"][fieldzone]["Children"][nitrogen_maize]['Parameters'] = fertilizer
                                      elif  root[key] == "TillMaize":
                                         till_maize = counter
                                         #print(app_ap["Children"][coresimulationNode]["Children"][fieldzone]["Children"][counter]["Name"])
                                         app_ap["Children"][coresimulationNode]["Children"][fieldzone]["Children"][till_maize]['Parameters'] = tillage_param1
                                         #print(app_ap["Children"][coresimulationNode]["Children"][fieldzone]["Children"])
                                         apsix = json.dumps(app_ap)
                                         filename = "edited" + str(name) + str(time.perf_counter())[-5:] +  ".apsimx"
                                         nameout  = None
                                         nameout =os.path.join(os.getcwd(), filename)
                                         with open(nameout, "w+")  as openfile:        
                                            openfile.write(apsix)
                                         #print(filename)
                                         return nameout
      except Exception as e:
          logger.exception(repr(e))
          raise e
        

# test the function
# aps = r'C:\Users\rmagala\Box\corn.apsimx'
# p = InsertCroppingSystems(aps, param = "Soybean, Maize")
# os.startfile(p)
