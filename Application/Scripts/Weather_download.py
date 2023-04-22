import os
from os.path import join as opj
from datetime import datetime 
import datetime
import urllib
import requests
import arcpy
import json
import pandas as pd
import time
import statistics
import numpy as np

#from US_states_abbreviation import getabreviation
def get_iem_bystation(dates, station, path, mettag): 
      '''
      Dates is a tupple/list of strings with date ranges
      
      an example date string should look like this: dates = ["01-01-2012","12-31-2012"]
      
      if station is given data will be downloaded directly from the station the default is false.
      
      mettag: your prefered tag to save on filee
      '''
    # access the elements in the metdate class above
      wdates = metdate(dates)
      stationx = station[:2]
      state_clim = stationx + "CLIMATE"
      str0 = "http://mesonet.agron.iastate.edu/cgi-bin/request/coop.py?network="
      str1 = str0 + state_clim + "&stations=" + station
      str2 = str1+ "&year1=" + wdates.year_start + "&month1=" + wdates.startmonth + "&day1=" + wdates.startday + "&year2="+ wdates.year_end + "&month2=" + wdates.endmonth + "&day2="+ wdates.endday
      str3 = (str2 + "&vars%5B%5D=apsim&what=view&delim=comma&gis=no")
      url  = str3
      rep = requests.get(url)
      if rep.ok:
           metname = station + mettag + ".met"
           os.chdir(path)
           if not os.path.exists('weatherdata'):
             os.mkdir('weatherdata')
           pt = os.path.join('weatherdata', metname)
   
           with open(pt, 'wb') as metfile1:
          #dont forget to include the file extension name
              metfile1.write(rep.content)
              rep.close()
              metfile1.close()
              print(rep.content)
      else: print("Failed to download the data web request returned code: ", rep)
   

class metdate:
      def __init__(self, dates):
         self.startdate = dates[0]
         self.lastdate = dates[1]
         self.startmonth = dates[0][:2]
         self.endmonth =  dates[1][:2]
         self.year_start  = dates[0].split("-")[2]
         self.year_end = dates[1].split("-")[2]
         self.startday = datetime.datetime.strptime(dates[0], '%m-%d-%Y').strftime('%j')
         self.endday = datetime.datetime.strptime(dates[1], '%m-%d-%Y').strftime('%j')
         
dates = ['01-01-2000', '12-31-2020']
#path = r'C:\Users\rmagala\Box\ACPF_MyProject\pathfolder'
# set the environmnet

def dowload_iem_bylocation1(dates, lonlat,  path, state, mettag, dist = 175000):
        '''
        download weather data from Iowa Environmental messonet by location
        parametes
        -----------
        dates: a tupple or a list of date strings e.g ['01-01-2000', '12-31-2020']

        path is the directory where the met file will be stored

        state: state in which the xy coordnates are located

        lonlat: a tupple or a list of x and y coorndates
        
        mettag: your prefered name tag

        dist = is the specified distance underwhich searching should stop
        '''
        SR = arcpy.SpatialReference(4326)
       
        arcpy.env.outputCoordinateSystem = SR
        #arcpy.env.overwriteOutput = True
        arcpy.env.scratchWorkspace = 'in_memory'
        arcpy.env.workspace = path
        arcpy.env.parallelProcessingFactor = "70%"
        outname  = mettag
        
        outfc = opj(path, outname)
        # create inputfeatue class. point feature class
        n1 = time.strftime("%H:%M:%S").replace(":", "")
        namefc  = 'se_' +n1
        search2 = arcpy.CreateFeatureclass_management("in_memory",  namefc, 'POINT')
        lon = lonlat[0]
        lat = lonlat[1]
        lonlat = { 'lon':lon, 'lat': lat}
        codDF = pd.DataFrame(data = lonlat, index = [1])
        searchFieldx = ['lon',	'lat']
        ff = 'file.csv'
        in_table = opj(path, ff)
        codDF.to_csv(in_table)
        searchFieldx = ['lon',	'lat']
      
        cursor1 = arcpy.da.InsertCursor(search2 , ['SHAPE@XY'])
        with arcpy.da.SearchCursor(in_table, searchFieldx) as targetsearchfield:
          for  row in targetsearchfield:
             lat = row[1]
             lon = row[0]
             point = arcpy.Point(lon, lat)
             rowList = [point]
             cursor1.insertRow(rowList)
        
        # set the distance to use
        distance_input = str(dist) + " Meters"
        in_features = search2
        angle = "NO_ANGLE"
        field_names = [['TEXT', "StationID"]]
        # insert near feature calculation code here
        url  = "http://mesonet.agron.iastate.edu/geojson/network.php?network="
        stateclim  =  url + getabreviation(state) + 'CLIMATE'
        rep = requests.get(stateclim)
        if rep.ok:
                    print("access to the server achieved")
                    rep_content  = rep.content
                    rep_json  = json.loads(rep_content)
                    st = json.dumps(rep_json)
                    # extract features
                    features = rep_json['features']
                    # this data is a list of objects so json normalise works prety well
                    df =pd.json_normalize(features)
                     # select the goemetry columns
                    geomcord = df.loc[:,'geometry.coordinates']
                    mpt = list(geomcord)
                    coods = pd.DataFrame(mpt)
                    dff = coods.rename(columns={0: "lon", 1:"lat"})
                    # join it with the station id
        
                    idf = dff.reset_index(drop=True).join(df.loc[:,'id'])
                    pt  = opj(path, "data.csv")
                    idf.to_csv(pt)
                    in_tablesx =  pt
                    n1 = str(lat)[-4:]+'near'
                    namefc  = 'FC' + n1
                    namefc = arcpy.CreateFeatureclass_management(path, namefc, 'POINT')
                    print(arcpy.GetMessages())
                    va1 = arcpy.ValidateFieldName('StationID')
                    arcpy.AddField_management(namefc, va1,'TEXT')
                    searchFieldx = ['lon', 'lat', "id"]
                    cursor1 = arcpy.da.InsertCursor(namefc, ['SHAPE@XY', va1])
                    with arcpy.da.SearchCursor(in_tablesx, searchFieldx) as ss:
                      for  row in ss:
                         lat = row[1]
                         lon = row[0]
                         point = arcpy.Point(lon, lat)
                         rowList = [point, row[2]]
                         cursor1.insertRow(rowList)
                    del cursor1 
                    len(rowList)
                    
                    distance_input = str(dist) + ' Meters'   
                    xp = arcpy.analysis.Near(in_features,  namefc, distance_input, "LOCATION", angle, "GEODESIC")
                    #print(arcpy.GetMessages())
                    if xp:
                       sf=  ['NEAR_FID', 'NEAR_DIST', 'NEAR_X', 'NEAR_Y']
                       NEAR = []
                       with arcpy.da.SearchCursor(search2, sf) as targetsearchfield:
                        for  row in targetsearchfield:
                            FID = row[0]
                            ND = row[1]
                            NX = row[2]
                            NY  =row[3]
                        rowList = [FID, ND, NX, NY]
                        NEAR.append(rowList)
  
                        
                        print(f'Selecting closest station with FID: {FID}')
                        out =path
                        listbag = []
                        near_field = ['FID', 'StationID']
                        with arcpy.da.SearchCursor(namefc, near_field) as targetsearchfield:
                         for  row in targetsearchfield:
                             if row[0] == rowList[0]:
                                nearest_station = row[1]
                                print("nearest station is:", nearest_station)
                                print(f"Nearest weather station: {nearest_station} is {ND} meters away")
                                wdates = metdate(dates)
                                print("near station loaded")
                                stationx = nearest_station[:2]
                                state_clim1 = stationx + "CLIMATE"
                                str0 = "http://mesonet.agron.iastate.edu/cgi-bin/request/coop.py?network="
                                str1 = str0 + state_clim1 + "&stations=" + nearest_station
                                str2 = str1+ "&year1=" + wdates.year_start + "&month1=" + wdates.startmonth + "&day1=" + wdates.startday + "&year2="+ wdates.year_end + "&month2=" + wdates.endmonth + "&day2="+ wdates.endday
                                str3 = (str2 + "&vars%5B%5D=apsim&what=view&delim=comma&gis=no")
                                url  = str3
                                rep = requests.get(url)
                                print(rep)
                                if rep.ok:
                                    
                                    
                                    print("Preparing met file now..............")
                                    st = str(lon)[-4:]
                                    metname = nearest_station + st + mettag +".met" 
                                    print(metname)
                                    outputweather = "weatherdata"
                                    if not os.path.exists(outputweather):
                                      os.mkdir("weatherdata")
                                    pt = opj("weatherdata", metname)
                                    print(pt)
                                    #print(rep.text)
                                    
                                    with  open(pt, 'wb') as opn:
                                       opn.write(rep.content)
                                       
                                       #print(rep.content)
                                    rep.close()
                                  
                                    arcpy.Delete_management(namefc)
                                    del search2
                                    print('**Done**\n met file written to disk...................')
                                    #return(rep.content)
                                else:
                                    print("Failed to download the data")
                                    
              
states = {
    'AK': 'Alaska',
    'AL': 'Alabama',
    'AR': 'Arkansas',
    'AZ': 'Arizona',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DC': 'District of Columbia',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'IA': 'Iowa',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'MA': 'Massachusetts',
    'MD': 'Maryland',
    'ME': 'Maine',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MO': 'Missouri',
    'MS': 'Mississippi',
    'MT': 'Montana',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'NE': 'Nebraska',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NV': 'Nevada',
    'NY': 'New York',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VA': 'Virginia',
    'VT': 'Vermont',
    'WA': 'Washington',
    'WI': 'Wisconsin',
    'WV': 'West Virginia',
    'WY': 'Wyoming'
}

# flip the keys
new_dict = {}
for k, v in states.items():
    new_dict[v] = k
    
def getabreviation(x):
    ab = new_dict[x]
    return(ab)
#function to define the date ranges   
def daterange(start, end):
  '''
  start: the starting year to download the weather data
  -----------------
  end: the year under which download should stop
  '''
  startdates = '01-01'
  enddates = '12-31'
  end = str(end)+"-"+ enddates
  start = str(start) + "-" + startdates
  drange = pd.date_range(start,end)
  return(drange)

# check if a year is aleap year
def isleapyear(year):
  if (year % 400 == 0) and (year % 100 == 0) or (year % 4 ==0) and (year % 100 != 0):
    return(True)
  else:
    return(False)
# download radiation data for replacement 
def get_nasarad(lonlat, start, end):
    lon = lonlat[0]
    lat = lonlat[1]
    pars=  "ALLSKY_SFC_SW_DWN"
    rm = f'https://power.larc.nasa.gov/api/temporal/daily/point?start={start}0101&end={end}1231&latitude={lat}&longitude={lon}&community=ag&parameters={pars}&format=json&user=richard&header=true&time-standard=lst'
    data = requests.get(rm, timeout = 30)
    dt = json.loads(data.content) 
    df = pd.DataFrame(dt["properties"]['parameter'])
    if len(df) ==len(daterange(start, end)):
      return df     

maxt = range (-60, 61) # units (C)
mint = range(-60, 41) # units (C)
radn = range (0, 41) #– units (MJ/m2/day);
rain = range (0, 101)# – units (mm/day)

def  check_met(met):
  for i in met.maxt:
    if i < -60 or i > 60:
      print(f'{i} maximum is high or low')
  for i in met.mint:
    if i < -60 or i > 40:
      print(f'mintemp {i} too high or low')

  for i in met.radn:
    if i < 0 or i > 40:
      print(f'radiation {i} is too high or low')
  for i in met.rain:
    if i < 0 or i > 100:
      print(f'rain{i} is too high or low')
def check_daymetyears(start, end):
   try:
       assert start in range(1980, 2023)
       assert end in  range(1980, 2023)
   except AssertionError: 
      print("requested year preceeds valid data range! \n end years should not exceed 2022 and start year should not be less than 1980")
      raise
# fucntion to download data from daymet
def daymet_bylocation(lonlat, start, end, cleanup = True):
    '''collect weather from daymet solar radiation is replaced with that of nasapower
    ------------
    parameters
    ---------------
    start: Starting year
   
    end: Ending year
   
    lonlat: A tuple of xy cordnates
   
    Cleanup:  A bolean True or False default is true: deletes the excel file generated during the file write up
   
    ------------
    returns complete path to the new met file but also write the met file to the disk in the working directory
    '''
   #import pdb
   # pdb.set_trace()
    import sys
    datecheck = daterange(start, end)
    check_daymetyears(start, end)
    base_url  = 'https://daymet.ornl.gov/single-pixel/api/data?'
    latstr = 'lat=' + str(lonlat[1])
    lonstr =  '&lon=' + str(lonlat[0])
    varss  = ['dayl', 'prcp', 'srad', 'tmax', 'tmin','vp','swe']
    setyears = [str(year) for year in range(start, end + 1)]
   # join the years as a string
    years_in_range = ",".join(setyears)
    years_str = "&years=" + years_in_range
    varfield = ",".join(varss)
    var_str = "&measuredParams=" + varfield
   # join the string url together 
    url =base_url + latstr + lonstr + var_str + years_str
    conn = requests.get(url, timeout = 60)
    assert conn.ok, f'Not successfully connected to the server, returned object: {conn}'
      #print("connection established to download the following data", url)
      #outFname = conn.headers["Content-Disposition"].split("=")[-1]
    outFname = conn.headers["Content-Disposition"].split("=")[-1]
    text_str = conn.content
    outF = open(outFname, 'wb')
    outF.write(text_str)
    outF.close()
    conn.close()
#       read the downloaded data to a data frame 
    dmett = pd.read_csv(outFname, delimiter= ',', skiprows = 7)
    vp = dmett['vp (Pa)'] *0.01
    # calcuate radn
    radn = dmett['dayl (s)'] * dmett['srad (W/m^2)'] * 1e-06
    # re-arrange data frame
    year = np.array(dmett['year'])
    day  =np.array(dmett['yday'])
    radn  = np.array(radn)
    maxt  = np.array(dmett['tmax (deg c)'])
    mint = np.array(dmett['tmin (deg c)'])
    rain = np.array(dmett['prcp (mm/day)'])
    vp = np.array(vp)
    swe  = np.array(dmett['swe (kg/m^2)'])
    df = pd.DataFrame({'year': year, 'day': day, 'radn': radn, 'maxt': maxt, 'mint':mint,'rain': rain, 'vp': vp, 'swe': swe})
    # bind the frame
    # calculate mean annual applitude in mean monthly temperature (TAV)
    ab= [a for a in setyears]
    #split the data frame
    ab = [x for _, x in df.groupby(df['year'])]
    df_bag = []
      # constants to evaluate the leap years
    leapfactor = 4
    for i in ab:
        if (all(i.year % 400 ==0)) and (all(i.year % 100 ==0)) or (all(i.year % 4 ==0)) and (all(i.year % 100 !=0)):
          x= i[['year','radn','maxt','mint','rain','vp','swe',]].mean()
          year = round(x[0], 0)
          day = round(366, 0)
          new_row = pd.DataFrame({'year':[year], 'day':[day], 'radn':[0], 'maxt':[0], 'mint':[0], 'rain':[0], 'vp':[0], 'swe':[0]})
          df_bag.append(pd.concat([i, new_row], ignore_index=True))
          continue
        else:
          #df = pd.concat(i)
          df_bag.append(i)
          frames = df_bag
    newmet = pd.concat(frames)
    newmet.index = range(0, len(newmet))
    # repalce radn data
    rad = get_nasarad(lonlat, start, end)
    newmet["radn"] =  rad.ALLSKY_SFC_SW_DWN.values
    assert len(newmet) == len(datecheck), f"{len(newmet)} is not equal to {len(datecheck)};date not in desired range"
      
   
      #print("met data is in the range of specified dates no discontinuities")
    rg = len(newmet.day.values) +1
    #newmet  = pd.concat(newmet)
    mean_maxt = newmet['maxt'].mean(skipna=True, numeric_only=None)
    mean_mint = newmet['mint'].mean(skipna=True, numeric_only=None)
    AMP = round(mean_maxt - mean_mint, 2)
    tav = round(statistics.mean((mean_maxt, mean_mint)), 2)
    # write then append
    tile = conn.headers["Content-Disposition"].split("=")[1].split("_")[0]
    fn  = conn.headers["Content-Disposition"].split("=")[1].replace("csv", 'met')
    shortenfn = 'weather_' 'Daymet'+ str(lonlat) + '.met'
    if not os.path.exists('weatherdata'):
       os.makedirs('weatherdata')
    fn = shortenfn
    fname = os.path.join('weatherdata', fn) 
    
    headers = ['year', 'day', 'radn', 'maxt', 'mint', 'rain', 'vp', 'swe']
    header_string = " ".join(headers) + "\n"
    # close and append new lines
    with open(fname, "a") as f2app:
      f2app.writelines([f'!site: {tile}\n', f'latitude = {lonlat[1]} \n', f'longitude = {lonlat[0]}\n', f'tav ={tav}\n', f'amp ={AMP}\n'])
      f2app.writelines([header_string])
      f2app.writelines(['() () (MJ/m2/day) (oC) (oC) (mm) (hPa) (kg/m2)\n'])
      #append the weather data
      data_rows = []
      for index, row in newmet.iterrows():
        current_row = []
        for header in headers:
          current_row.append(str(row[header]))
        current_str = " ".join(current_row) + '\n'
        data_rows.append(current_str)
        
      f2app.writelines(data_rows)
      
   
    if cleanup:
       if os.path.isfile(os.path.join(os.getcwd(), outFname)):
         os.remove(os.path.join(os.getcwd(), outFname))
    return fname #fname
      
# dowload weather data from nasapower
def getnasa(lonlat, start, end):
    lon = lonlat[0]
    lat = lonlat[1]
    param= ["T2M_MAX", "T2M_MIN", "ALLSKY_SFC_SW_DWN", "PRECTOTCORR", "RH2M","WS2M"]
    pars = ",".join(param)
    rm = f'https://power.larc.nasa.gov/api/temporal/daily/point?start={start}0101&end={end}1231&latitude={lat}&longitude={lon}&community=ag&parameters={pars}&format=json&user=richard&header=true&time-standard=lst'
    data = requests.get(rm, timeout = 30)
    dt = json.loads(data.content) 
    df = pd.DataFrame(dt["properties"]['parameter'])
    return df
# download only solar radiation for replacements
# path = r'C:\Users\rmagala\Box\ACPF_MyProject\ACPF DATA\sievers_case_study\APSIM Simulations'
# dowload_iem_bylocation1(dates, [ -90.72704709,40.93103233],  path, 'Iowa', dist = 175000, name ="featurestation")
# # clean up
def cleanup ():
  arcpy.Delete_management('in_memory') 
