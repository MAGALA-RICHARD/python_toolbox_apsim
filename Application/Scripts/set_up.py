import subprocess
import sys
import importlib
import os
import pkgutil
def check_if_package_Loaded(package):
  load_package  = pkgutil.find_loader(package)
  if load_package != None:
    value = True
    return value
# check if the package is loaded, then install it
for pkg in ['xmltodict', 'urllib', 'scipy', 'pandas', 'numpy', 'requests', 'winsound', 'platform']:
  if check_if_package_Loaded(pkg) !=True:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', pkg])
    print(f'{pkg} was installed successfully')
    # import it again
    globals()[pkg] = importlib.import_module(pkg)
    # print the success
    print(f'{pkg} originally not install has been imported successfully')
  
 # old code ignore 
# try:
#   globals()[pkg] = importlib.import_module(pkg)
#   print(f"{pkg} imported successfully")
# except ImportError:
#   #if package not found we install it
#   subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'urllib'])
#   print(f'{pkg} was installed successfully')
#   # import it again
#   globals()[pkg] = importlib.import_module(pkg)
#   # print the success
#   print(f'{pkg} originally not install has been imported successfully')
#   

#End************************************************************************


# try:
#  import xmltodict
# except ImportError:
#  from pip._internal import main as pip
#  pip(['install', '--user', 'xmltodict'])
#  import xmltodict
#  
# import pkg_resources
# installed_packages = pkg_resources.working_set
# installed_packages_list = sorted(["%s==%s" % (i.key, i.version)\
#    for i in installed_packages])
# print(installed_packages_list)





  

