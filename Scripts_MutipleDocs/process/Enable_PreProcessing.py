#!/usr/bin/env python
# coding: utf-8
# ------------------------------------------------------------------------------#
# Script: Calling Pre Processing Script from VM79
# Initial Author: Omkar Sonar
# Date: 18/02/2022

    
# Revisions:
# ----------------------------------------------------------------
# Date    |    Author       | Description
# ----------------------------------------------------------------
'''
1. Main Folder
			Decrypted Folder (input folder) - - > run_stat.bat - - > json files is created.
2. Conversion Folder
			Decrypted Folder (input folder) - - > run.bat - - > output (Conversion Folder) - - > New folder is created by script "asdajkja238378hwdjqhjs" - - > each folder will contain 999 folders and might contain .txt files.            
3. Deskew Folder
			Conversion Folder (input folder) - - > deskew.bat (is located on another folder) - - > output (Deskew Folder) - - > Folder should be created by current date (20220123_010000) - - > the new created by python should have only  1000 folders oruser input how many files should be dump. - - > two .txt files are created automatically need to move outside of created folder by python.
			
            DeskewImage.exe "E:\Rushi\Pre-Process\Testing\2_Conversion\49f6d739-36b8-46f3-ac0b-02f5448af99f" "E:\Rushi\Pre-Process\Testing\3_Deskew\20220202_010000"
            
            date_string = re.replace(sys.currentdate(),'\','')
            deskew_batch_size  = 1000
            
            
            
            C.   Option to select no. of files for pre-process
            
            
A.   At every step thum.db files should be check and need to delete those files before running/calling new tool

B.   In step 1 and 2 need to check page count if match then proceed or stop and restart the process for only missing pages. Same should be consider for step 2 and 3. At end of step 3 final page count should be displayed.



Need to create table to locate the file status for every step so that it will be easy to restart the process in problem "B" if occurred. And able to check the status of file even if it was excluded in "Deskew"

FX123 
     DT111-CD
     DT222-LE
     DT333-Tax
     DT444-URLA


mainfolder_bat_path = F:\MLOPSDataBank_1.0\Databank_Pipeline_Suite_Repository\Tools\Pre-Process\!Pre-Processing\toolDataConverter\run_stat.bat
conversion_bat_path = F:\MLOPSDataBank_1.0\Databank_Pipeline_Suite_Repository\Tools\Pre-Process\!Pre-Processing\toolDataConverter\run.bat
deskew_bat_path = F:\MLOPSDataBank_1.0\Databank_Pipeline_Suite_Repository\Tools\Pre-Process\!Pre-Processing\DeskewImage\run.bat

doc_type = ''
decrypted_folder_path = ''
conversion_folder_path = ''
deskew_folder_path = ''

decryption

buckets
    1120
    1140
'''

# ------------------------------------------------------------------------------#


# IMPORT PACKAGES

import os              # for handling folder creation, folder traversing
import numpy
import shutil          # for file copy
from datetime import datetime
import subprocess
import sys
import re
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
''' preprocess_tool_path = 'F:\MLOPSDataBank_1.0\Databank_Pipeline_Suite_Repository\Tools\Pre-Process'
mainfolder_bat_path = preprocess_tool_path+'\!Pre-Processing\toolDataConverter\run_stat.bat'
conversion_bat_path = preprocess_tool_path+'\!Pre-Processing\toolDataConverter\run.bat'
deskew_bat_path = preprocess_tool_path+'\!Pre-Processing\DeskewImage\run.bat'

#jira_id = ''
databank_path = 'F:/MLOPSDataBank'
doc_type = 'URLA'
pdf2png="false"
decrypted_folder_path = databank_path+'/Data Bank Inventory/Amerihome/ADS_1111_01012022_01012022_01022022/1_Decryption'
conversion_folder_path = databank_path+'/Data Bank Inventory/Amerihome/ADS_1111_01012022_01012022_01022022/3_Pre-Processing/1_Conversion'
deskew_folder_path = databank_path+'/Data Bank Inventory/Amerihome/ADS_1111_01012022_01012022_01022022/3_Pre-Processing/2_Deskew'''

preprocess_tool_path = sys.argv[1]
mainfolder_bat_path = sys.argv[2]
conversion_bat_path = sys.argv[3]
deskew_tool_path = sys.argv[4]
deskew_bat_path = sys.argv[5]
doc_type = sys.argv[6]
pdf2png=sys.argv[7]
decrypted_folder_path = sys.argv[8]
conversion_folder_path = sys.argv[9]
deskew_folder_path = sys.argv[10]

print("Location of Pre Processing Tool ",preprocess_tool_path)
print("Path main folder bat file :ve ",mainfolder_bat_path)
print("Path conversion bat file : ",conversion_bat_path)
print("Doc Type : ",doc_type)
print("Decrypted Folder : ",decrypted_folder_path)
print("Conversion Folder : ",conversion_folder_path)
print("Deskew Folder : ",deskew_folder_path)

#--mainfolder_bat_path-----------------------------------------------------------------------------------------------
totalDir = 0
totalFiles = 0
for root, dirs, files in os.walk(decrypted_folder_path):
    totalsubDir = 0
    totalsubFiles = 0
    filescancount = 0

    for directories in dirs:
        if (directories.find("FX")):
            totalsubDir += 1
    totalDir = totalDir + totalsubDir

    for filename in files:
        if filename.endswith('.db'):
            os.remove(os.path.join(root,filename))
            continue                 
        if filename.endswith('.json'):
            continue
        else:
            totalsubFiles += 1
    totalFiles = totalFiles + totalsubFiles

print('Total Folders to be scanned : ', totalDir)
print('Total Files to be scanned : ', totalFiles)


# CALL THE PRE PROCESSING SCRIPTS
s=subprocess.call([mainfolder_bat_path,decrypted_folder_path,preprocess_tool_path])
print('Pre-Processing --> toolDataConverter --> run_stat.bat Executed')
print("run_stat.bat Exit Code : ",s)


if s==0:
    s1=subprocess.call([conversion_bat_path,decrypted_folder_path,conversion_folder_path,doc_type,pdf2png,preprocess_tool_path])
    print('Pre-Processing --> toolDataConverter --> run.bat Executed ')
#---deskew_bat_path----------------------------------------------------------------------------------------------
totalDir = 0
totalFiles = 0
for root, dirs, files in os.walk(conversion_folder_path):
    totalsubDir = 0
    totalsubFiles = 0
    filescancount = 0

    for directories in dirs:
        if (directories.find("FX")):
            totalsubDir += 1
    totalDir = totalDir + totalsubDir

    for filename in files:
        if filename.endswith('.db'):
            os.remove(os.path.join(root,filename))
            continue                 
        if filename.endswith('.json'):
            continue
        else:
            totalsubFiles += 1
    totalFiles = totalFiles + totalsubFiles

print('Total Folders to be scanned : ', totalDir)
print('Total Files to be scanned : ', totalFiles)

import datetime
now = datetime.datetime.now()
datestr_for_folder = now.strftime("%Y-%m-%d")
i=10000
for files in os.scandir(conversion_folder_path):
    if files.is_dir():
        print(conversion_folder_path+'\\'+files.name)
        deskew_input_batch_folder_path = conversion_folder_path#+'\\'+files.name
        output_datewise_batch_folder_name = datestr_for_folder+'_0'+str(i)
        output_datewise_batch_folder_path=deskew_folder_path+'\\'+output_datewise_batch_folder_name
        i=i+10000
        print(deskew_bat_path,' ',deskew_input_batch_folder_path,' ',deskew_folder_path,' ',deskew_tool_path)
#-------------------------------------------------------------------------------------------------
        s2=subprocess.call([deskew_bat_path,deskew_input_batch_folder_path,deskew_folder_path,deskew_tool_path])
        print('Pre-Processing --> toolDataConverter --> run.bat Executed for ', files)

for root, dirs, files in os.walk(deskew_folder_path):
    for deskewedfile in files:
        if deskewedfile.endswith('.txt'):
            continue
        if deskewedfile.endswith('.json'):
            continue
        if deskewedfile.endswith('.png'):
            continue            
        if deskewedfile.endswith('.layout'):
            continue                        
        if deskewedfile.endswith('.db'):
            continue                        
#-----Write to MongoDB ----------------------------------------------------------------------            

        #MiscDT200281351_FX200238_Tax_returns_MI200281_000418_C0-000002
        MIItemID_match_mongodb = 'MI'+re.search(r'MI(.*?)_', deskewedfile).group(1)
        JsonNo_match_mongodb = re.search(r'.*?\_\d(.*?)_.*', deskewedfile).group(1)
        pageno_section_mongodb = deskewedfile.split('_')
        tokens=len(pageno_section_mongodb)
        actual_page_number_mongodb = pageno_section_mongodb[tokens-1].split('.')[0].split('-')[1]
        file_extension_mongodb = pageno_section_mongodb[tokens-1].split('.')[1]

        "MI200281_000418_C0-000002"
        mongo_pageid = MIItemID_match_mongodb+'_'+JsonNo_match_mongodb+'_C0-'+actual_page_number_mongodb
        print(mongo_pageid)
        
        if use_mongo_db=='1':
            mycol.update_one({"PAGEID":mongo_pageid},{"$set":{"preprocessing_flag":"1","fileloc":root}})
#-----------------------------------------------------------------------------------------         