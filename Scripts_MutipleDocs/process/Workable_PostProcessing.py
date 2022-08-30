#!/usr/bin/env python
# coding: utf-8
# ------------------------------------------------------------------------------#
# Script: Calling Post Processing Script from VM79
# Initial Author: Omkar Sonar
# Date: 24/01/2022

    
# Revisions:
# ----------------------------------------------------------------
# Date    |    Author       | Description
# ----------------------------------------------------------------
"""

"""

# ------------------------------------------------------------------------------#


# IMPORT PACKAGES

import os              # for handling folder creation, folder traversing
import sys
import numpy
import shutil          # for file copy
from datetime import datetime
import subprocess
import pymongo # need to import module Mongo DB
from pymongo import MongoClient


#---- MongoDB COnnection -------------------------------------------------------
import configparser

read_config = configparser.ConfigParser()
read_config.read('../../Config/central_config.ini')
print('../../Config/central_config.ini')

use_mongo_db = read_config.get("mongodb","use_mongo_db")
hostname = read_config.get("mongodb","hostname")
port = read_config.get("mongodb","port")
dbstring = read_config.get("mongodb","dbstring")

hoststring = "mongodb://"+hostname+":"+str(port)
print(hoststring)
print(use_mongo_db)

if use_mongo_db=='1':
    #client = pymongo.MongoClient("mongodb://localhost:27017") # Make connection
    client = pymongo.MongoClient(hoststring)
    db = client["MLOPSDATABANK"]
    mycol = db["MLOPSDATABANK_DIARY"]

#hostname =    localhost,port     =    27017,dbstring = 'MLOPSDATABANK'
#-------------------------------------------------------------------------------


postprocessing_tool_path = sys.argv[1] #"E:/Rushi/Post-Process/test/post_processing"
run_bat_path = sys.argv[2] #"E:/Rushi/Post-Process/test/post_processing/run_stat.bat"
run_postprocessing_path = sys.argv[3] #"E:/Rushi/Post-Process/test/post_processing/run_postprocessing.bat"
doc_type = sys.argv[4]#'URLA' #Tax Returns, Loan Estimate, IRS W_2
input_directory = sys.argv[5]#'//10.25.20.68/e$/Stress_Testing/Post_Processing/1st_Lot_1K'
output_directory = sys.argv[6]

print("Location of Post Processing Tool ",postprocessing_tool_path)
print("Path of run_stat.bat : ",run_bat_path)
print("Path of run_postprocessing.bat : ",run_postprocessing_path)
print("Doc Type : ",doc_type)
print("Data Input Directory : ",input_directory)
print("Data Output Directory : ",output_directory)

totalDir = 0
totalFiles = 0
for root, dirs, files in os.walk(input_directory):
    totalsubDir = 0
    totalsubFiles = 0
    filescancount = 0

    for directories in dirs:
        if (directories.find("FX")):
            totalsubDir += 1
    totalDir = totalDir + totalsubDir

    for Files in files:
        if Files.endswith('.json'):
            continue
        else:
            totalsubFiles += 1
    totalFiles = totalFiles + totalsubFiles

print('Total Folders to be scanned : ', totalDir)
print('Total Files to be scanned : ', totalFiles)


# CALL THE POST PROCESSING SCRIPTS
s=subprocess.call([run_bat_path,input_directory,postprocessing_tool_path])

print("run_stat.bat Exit Code : ",s)
if s==0:
    s1=subprocess.call([run_postprocessing_path,input_directory,doc_type,output_directory,postprocessing_tool_path])


for root, dirs, files in os.walk(input_directory):
    for filename in files:    
    #-----Write to MongoDB ----------------------------------------------------------------------            

        #MiscDT200281351_FX200238_Tax_returns_MI200281_000418_C0-000002
        MIItemID_match_mongodb = 'MI'+re.search(r'MI(.*?)_', filename).group(1)
        JsonNo_match_mongodb = re.search(r'.*?\_\d(.*?)_.*', filename).group(1)
        pageno_section_mongodb = filename.split('_')
        tokens=len(pageno_section_mongodb)
        actual_page_number_mongodb = pageno_section_mongodb[tokens-1].split('.')[0].split('-')[1]
        file_extension_mongodb = pageno_section_mongodb[tokens-1].split('.')[1]

        "MI200281_000418_C0-000002"
        mongo_pageid = MIItemID_match_mongodb+'_'+JsonNo_match_mongodb+'_C0-'+actual_page_number_mongodb
        print(mongo_pageid)
        
        if use_mongo_db=='1':
            mycol.update_one({"PAGEID":mongo_pageid},{"$set":{"postprocessing_flag":"1","fileloc":root}})
#-----------------------------------------------------------------------------------------    


