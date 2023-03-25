import arcpy
import os
import traceback
import sys
import tempfile
import shutil

dirpath = tempfile.mkdtemp()
# ... do stuff with dirpath
#shutil.rmtree(dirpath)
def create_fishnet(watershedfc, height =200, SR = 4326):
    try:
        arcpy.env.scratchWorkspace = "in_memory"
        arcpy.env.overwriteOutput = True
        
        geodata = 'Gis_result_geodatabase.gdb'
        if not arcpy.Exists(geodata):
           arcpy.CreateFileGDB_management(os.getcwd(), geodata)
        arcpy.env.workspace = os.path.join(os.getcwd(), geodata)
        path = os.path.join(os.getcwd(), geodata)
        # change the env according to inputfc
        envcod  = arcpy.da.Describe(watershedfc)['spatialReference'].name
        arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(envcod)
      # create a fishnet
        templateExtent = watershedfc
        fc= templateExtent
        labelname = 'box_fishnet'
        fcname = os.path.join(geodata, labelname)
        if arcpy.Exists(fcname):
          arcpy.management.Delete(fcname)
        desc = arcpy.Describe(watershedfc)
        in_feature_path = arcpy.CreateFishnet_management(fcname,str(desc.extent.lowerLeft),str(desc.extent.XMin) + " " + str(desc.extent.YMax + 10),
            f"{height}",f"{height}","0","0",str(desc.extent.upperRight),"NO_LABELS","#","POLYGON")
        #let's clip out the unwanted part according to the processing extent
        in_features = in_feature_path
        clip_features = watershedfc
        out_feature_class = "fcc_clipped"
        if arcpy.Exists(out_feature_class):
          arcpy.management.Delete(out_feature_class)
        out_feature_class = None
        out_feature_class =  "fcc_clipped"
        #if arcpy.exists(out_feature_class):
          #arcpy.
        xy_tolerance = ""
        # Execute Clip
        arcpy.Clip_analysis(in_features, clip_features, out_feature_class, xy_tolerance)
        # convert each fishnet to point
        name = os.path.join(os.getcwd(), "fishnets") 
        if arcpy.Exists(name):
          arcpy.management.Delete(name)#+ str(watershedfc[-9:])
        fishnets_points = arcpy.management.FeatureToPoint(out_feature_class, name, 'INSIDE')
        sr = arcpy.SpatialReference(SR)
        listfield = []
        lf = arcpy.ListFields(fishnets_points)
        for i in lf:
            listfield.append(i.name)
        feature_array  = arcpy.da.FeatureClassToNumPyArray(name,  listfield, spatial_reference = sr)
        arcpy.management.Delete(name)
        return  feature_array, fishnets_points, out_feature_class
    except  Exception as e:
           tb = sys.exc_info()[2]
           tbinfo = traceback.format_tb(tb)[0]
      
           # Concatenate information together concerning the error into a message string
           pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
           print(pymsg + "\n")
      
           if arcpy.GetMessages(2) not in pymsg:
              msgs = "ArcPy ERRORS:\n" + arcpy.GetMessages(2) + "\n"
              arcpy.AddError(msgs)
              print(msgs)
      
# test the code
#path = r'C:\Users\rmagala\Box\report2'
#ap = r'C:\ACPd\Base_files\acpf_huc070801050305\acpf070801050305.gdb\buf070801050305'

#a, b, c  = create_fishnet(path, ap)
