import pandas as pd
import arcpy
import os
def convertpoint_to_feature_class(pandas_dataf23, geodatabase, nameof_featureclass ="siimulatedresult_feature_class"):
    """
    pandasdf; pandasdf
    geodatabase: geodatabase path for storing the new feature class
    nameof_featureclass: name for the new feature class
    
    """
    data = pd.read_csv(pandas_dataf23)
    structured_Numpy_records = data.to_records(index= False)
    name = os.path.join(geodatabase, nameof_featureclass)
    sr = arcpy.SpatialReference(4326)
    # change some unassigned data type
    det = structured_Numpy_records.dtype.descr
    import copy
    de = list(copy.deepcopy(dt))
    print(type(det))
    length = len(dt)
    for i in range(length):
      if 'Soiltype' in  de[i]:
         dt1 = i
      elif 'CompName' in de[i]:
        dt2 = i
    det[dt1] = ('Soiltype', '<U25')
    featuretobe = structuredNumpy_records.astype(det)
    arcpy.da.NumPyArrayToFeatureClass(featuretobe, name, ["longitude","Latitude"], sr)
    copyfeature  = "copyfeatureclass"
    #arcpy.management.CopyFeatures(name, copyfeature)
    

geodata = 'geodatatest.gdb'
if not arcpy.Exists(geodata):
     arcpy.CreateFileGDB_management(os.getcwd(), geodata)
point_feature_class = 'point_feature_classs'
pp = r'C:\Users\rmagala\OneDrive\Simulation_Application\Papsimx\SimulationResults\resultsexternal.csv'
datf = pd.read_csv(pp)
fd  = datf.convert_dtypes()
structuredNumpy_records = datf.to_records(index= False)
name = os.path.join(geodata, point_feature_class)
sr = arcpy.SpatialReference(4326)
convertpoint_to_feature_class(datf, geodata)
