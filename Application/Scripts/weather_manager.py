import os, json
from os.path import join as opj
import logging

logger = logging.getLogger(__name__)

class Weather:
  def __init__(self, path2apsim, apsimxfile, weatherfile, path2weather):
   '''Two file path are required one for apsimx file and one for weather'''
   self.apsimxfile = apsimxfile
   self.pathea2apsim = path2apsim
   self.weatherfile = weatherfile
   self.path2weather  = path2weather
   
  def ReplaceWeatherData(self):
        if not self.apsimxfile.endswith(".apsimx"):
          print("apsimx extension required")
        else:
          pathstring = opj(self.pathea2apsim, self.apsimxfile)
        if not self.weatherfile.endswith(".met"):
           print(".met extension required on weather files")
        else:
           wstring = opj(self.path2weather, self.weatherfile)
           
        if not os.path.isfile(pathstring) and  not os.path.isfile(wstring):
           print("filenames are not valid")
        else:
           with  open(pathstring, "r+") as apsimxx: 
              app_ap = json.load(apsimxx)
      # search for the Core simulation node
      # the challenge is that the nodes may not be in the correct oder everytime. so we loop through using enumeration fucntion
        for counter, root in enumerate(app_ap["Children"]):
            if root['$type'] == 'Models.Core.Simulation, Models':
               if not counter:
                  print("No core simulation node found")
               else: 
                 coresimulationNode = counter
      
      # Now let's get the position of the weather and the clock
        for counter, root in enumerate(app_ap["Children"][coresimulationNode]["Children"]):
            if root['$type'] == 'Models.Climate.Weather, Models':
               if not counter:
                  print("No Weather node found at this root at: ", app_ap["Children"][coresimulationNode]["Name"])
               else: 
                    weather = counter
      #   for counter, root in enumerate(app_ap["Children"][coresimulationNode]["Children"]):
      #      if root['$type'] == 'Models.Clock, Models':
      #         if not counter:
      #           print("No Clock node found at this root at: ", app_ap["Children"][coresimulationNode]["Name"])
      #         else: 
      #           clock = counter
      # let's replace weather
                    app_ap["Children"][coresimulationNode]["Children"][weather]["FileName"] =wstring
                    name = self.weatherfile[:-4]
                    name2 = name[-6:]
                    namefile = 'rp{0}{1}'.format(name2, ".apsimx")
                    # pass back to json
                    json_dump = json.dumps(app_ap)
                    # create a namefile
                    os.chdir(self.pathea2apsim)
                    newapsimstring = opj(self.pathea2apsim, namefile)
                    file2write = open(newapsimstring, "w+")
                    file2write.write(json_dump)
                    file2write.close()
                    print("done****************")
      # 
        return newapsimstring
      
      
class Weather2:
  def __init__(self, completepath2apsim, completepath2weathefile, start, end):
  
   '''
   This class takes in complete paths for apsimx and weather files
   
   parameters.
   -----------------------
   completepath2apsim: complete path to apsimx file should end with .apsimx file. it will be evaluated by the os.path.isfile
     if false the code won't run
   completepath2weathefile: complete path to apsimx file should end with .met file. it will be evaluated by the os.path.isfile
     if false the code won't run
   returns: path to apsimx file
   start: start date of the simulation
   end: end date of the simulation
   '''
   self.completepath2apsim = completepath2apsim
   self.completepath2weathefile = completepath2weathefile
   self.start = str(start) + "-01-01T00:00:00"
   self.end = str(end) + '-12-31T00:00:00'
        
  def ReplaceWeatherData(self):
        
        pathstring = self.completepath2apsim
        wstring = self.completepath2weathefile
        
        assert os.path.isfile(pathstring) and os.path.isfile(wstring)
        assert pathstring.endswith(".apsimx") and wstring.endswith('.met')
        
        appsim_data = None
        with open(pathstring, "r+") as apsimxx:
          appsim_data = json.load(apsimxx)
   
        # search for the Core simulation node
        # the challenge is that the nodes may not be in the correct oder everytime. so we loop through using enumeration fucntion
        for counter, root in enumerate(appsim_data["Children"]):
            if root['$type'] == 'Models.Core.Simulation, Models':
               if not counter:
                  logger.exception("No core simulation node found")
                  raise AttributeError(msg)
               else: 
                 coresimulationNode = counter
      
      # Now let's get the position of the weather and the clock
        for counter, root in enumerate(appsim_data["Children"][coresimulationNode]["Children"]):
            if root['$type'] == "Models.Clock, Models" and root["Name"] == "Clock":
              clock = counter
              appsim_data["Children"][coresimulationNode]["Children"][clock]["Start"] =  self.start
              appsim_data["Children"][coresimulationNode]["Children"][clock]["End"] =  self.end
            elif root['$type'] == 'Models.Climate.Weather, Models':
               if not counter:
                  msg = f"No Weather node found at this root at:, {appsim_data['Children'][{coresimulationNode}]['Name']}"
                  logger.exception(msg)
                  raise AttributeError(msg)
               else: 
                    weather = counter
                    appsim_data["Children"][coresimulationNode]["Children"][weather]["FileName"] =wstring
                    #name = self.completepath2weathefile[:-4]
                    #create a unique code for each fiel based on simulation time
                    
                    listsplit = self.completepath2apsim.split("\\")
                    pos = len(listsplit)-1
                    apsimname1  = self.completepath2apsim.split("\\")[pos]
                    newname =  apsimname1[:-7] + ".apsimx"
                    # pass back to json
                    json_dump = json.dumps(appsim_data)
                    # create a namefile
                    final_path = os.path.join(os.getcwd(), newname)
                    with open(final_path, "w+") as file2write:
                      file2write.write(json_dump)
                
                    
      # 
        return final_path

