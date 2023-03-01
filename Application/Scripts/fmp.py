import arcpy
def fieldmapping(in_file, outfile, field_name, ws):
    arcpy.env.workspace = ws
    in_file = in_file
    out_file = outfile
    
    # Create the necessary FieldMap and FieldMappings objects
    fm = arcpy.FieldMap()
    fm1 = arcpy.FieldMap()
    fms = arcpy.FieldMappings()
    
    # and adds them to the FieldMap Object
    #for field in arcpy.ListFields(in_file, 'mean*'):
    fm.addInputField(in_file, field_name)
    
    # Set the merge rule to find the mean value of all fields in the
    # FieldMap object
    fm.mergeRule = 'Mean'
    
    # Set properties of the output name.
    f_name = fm.outputField
    f_name.name = 'averagen20'
    f_name.aliasName = 'Avgaveragen20'
    fm.outputField = f_name
    
    # Add the intersection field to the second FieldMap object
    fm1.addInputField(in_file, "Intersection")
    
    # Add both FieldMaps to the FieldMappings Object
    fms.addFieldMap(fm)
    fms.addFieldMap(fm1)
    return fms
    
    # Create the output feature class, using the FieldMappings object
#arcpy.FeatureClassToFeatureClass_conversion(in_file, arcpy.env.workspace, out_file, field_mapping=fms)
