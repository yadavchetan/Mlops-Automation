#!/usr/bin/env python
# coding: utf-8
# ------------------------------------------------------------------------------#
# Script: Handling Documents with Invalidated XML
# Initial Author: Omkar Sonar
# Date: 22/11/2021

# Aim: 
#     - Step 1: UnZipping the folder in same location
#     - Step 2 : Delete the zip files
#     - 3 : List all XML tags dataPoint under one DT
#                - 3.1 : set 
# Revisions:
# ----------------------------------------------------------------
# Date    |    Author       | Description
# ----------------------------------------------------------------
#MI21062329_000009_C0-000009.pdf.bronze.xml
#MI21062329_000009_C0-000009.layout.xml

# ------------------------------------------------------------------------------#


# IMPORT PACKAGES

import json            # for handling json data
import pandas as pd    # for handling data
import os              # for handling folder creation, folder traversing
import numpy
import shutil          # for file copy
#import seaborn as sns  # for Bar-chart generation
import sys
from zipfile import ZipFile
from  bs4 import BeautifulSoup
from datetime import datetime
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
    

print("Started at : ",datetime.now())

# USER INPUTS        
#directory = "F:/Invalidated_XML/Sample/2021-09-28/FX2106107"
directory = sys.argv[1]#"E:/Stress_Testing/Invalidated_XML/1st_Lot_1K"
dump_dir = sys.argv[2]#"E:/Stress_Testing/Invalidated_XML/1st_Lot_1K/Invalidated_FX_DT_Pages"  #Invalidated_FX_DT_Pages
if  os.path.exists(os.path.join(dump_dir)):
    shutil.rmtree(dump_dir)
os.mkdir(os.path.join(dump_dir))

print("The directory under process is :", directory)
print("The Invalidated Dump Directory under process is :", dump_dir)

unzip_count = 0
# UNZIP ALL THE FOLDERS IN SAME DIRECTORY
for root, dirs, files in os.walk(directory):
    for filename in files:
        if filename.endswith('.zip'):
            path = os.path.join(root, filename)

            with ZipFile(path, 'r') as zipObj:
                unzip_path = os.path.splitext(path)[0]
                unzip_count=unzip_count+1

                if  os.path.exists(unzip_path):
                    shutil.rmtree(unzip_path)
                os.mkdir(unzip_path)
                zipObj.extractall(unzip_path)

# SPOT INVALIDATED XMLS, MOVE ITS DT ALONG WITH ITS FX FOLDER TO DUMP DIR

invalidted_count = 0
accepted_count = 0
for root, dirs, files in os.walk(directory):

    invalidatedval_set = set()
    
    x = root.split('/')
    x = x[len(x)-1]
    x = x.split('\\')
    folder = x[len(x)-2]
    doc = x[len(x)-1]
    
    path_tuple = (folder, doc)
    folder_name_dir = folder
    dt_name_dir = doc
    
    if 'Invalidated_FX_DT_Pages' in root:
        continue

    for filename1 in files:
        if filename.endswith('.json'):
            continue
        if filename.endswith('.csv'):
            continue 
        if filename.endswith('.db'):
            os.remove(os.path.join(root,filename1))
            continue      
        if filename1.endswith('.zip'):
            os.remove(os.path.join(root,filename1))        
            continue
        if filename1.endswith('.txt'):
            continue            
        # ---CHECK XMLs FOR INVALIDTED STATUS IN DT FOLDER AND GENERATE A FLAG---
        if filename1.endswith('.xml'):
            xmlpath = os.path.join(root, filename1)

            with open(xmlpath,'r') as f:
                data = f.read()
                
            bsxml = BeautifulSoup(data,"xml")
            for ele in bsxml.find_all('dataPoint'):
                invalidatedval_set.add(ele.get('invalidated'))
         # -------------------------------------------------------------------------------------------------------
    #print(invalidatedval_set)
    
    if doc.startswith('DT'):
        for s in invalidatedval_set:
            if s=='true':
                invalidted_count=invalidted_count+1
                print('Invalidated XML DT folder : ', os.path.join(directory,folder_name_dir,dt_name_dir))
                if not os.path.isdir(os.path.join(dump_dir, folder_name_dir)):
                    os.mkdir(os.path.join(dump_dir, folder_name_dir))
            
                shutil.move(os.path.join(directory,folder_name_dir,dt_name_dir),os.path.join(dump_dir,folder_name_dir),copy_function=shutil.copytree)
            else:
                accepted_count=accepted_count+1
                print('Accepted DT folder : ', os.path.join(directory,folder_name_dir,dt_name_dir))              
        
print('--Summary----------------------------------------------------')
print('Total DTZip Files Unzipped : ', unzip_count)
print('Total Invalidated XML DT Folders : ', invalidted_count)
print('Total Accepted DT Folders : ', accepted_count)
print('-------------------------------------------------------------')

invalid_list = open(directory+"zip_listing.txt","a")

# TAKE DIR LISTING OF INVALLIDATED DT
for root, dirs, files in os.walk(dump_dir):
    for filename2 in files:
        if filename2.endswith('.txt'):
            continue
        if filename2.endswith('.json'):
            continue
        if filename2.endswith('.png'):
            continue            
        if filename2.endswith('.layout'):
            continue                        
        invalid_list.write(os.path.join(root,filename2)+ os.linesep)
#-----Write to MongoDB ----------------------------------------------------------------------            

        #MiscDT200281351_FX200238_Tax_returns_MI200281_000418_C0-000002
        MIItemID_match_mongodb = 'MI'+re.search(r'MI(.*?)_', filename2).group(1)
        JsonNo_match_mongodb = re.search(r'.*?\_\d(.*?)_.*', filename2).group(1)
        pageno_section_mongodb = filename2.split('_')
        tokens=len(pageno_section_mongodb)
        actual_page_number_mongodb = pageno_section_mongodb[tokens-1].split('.')[0].split('-')[1]
        file_extension_mongodb = pageno_section_mongodb[tokens-1].split('.')[1]

        "MI200281_000418_C0-000002"
        mongo_pageid = MIItemID_match_mongodb+'_'+JsonNo_match_mongodb+'_C0-'+actual_page_number_mongodb
        print(mongo_pageid)
        
        if use_mongo_db=='1':
            mycol.update_one({"PAGEID":mongo_pageid},{"$set":{"invalidatedxml_flag":"1","fileloc":root,"invalidated_status":"true"}})
#-----------------------------------------------------------------------------------------         

# MAKE ENTRY INTO MONGODB FOR VALID XMLS
for root, dirs, files in os.walk(directory):
    for filename3 in files:
        if filename3.endswith('.txt'):
            continue
        if filename3.endswith('.json'):
            continue
        if filename3.endswith('.png'):
            continue
        if filename3.endswith('.layout'):
            continue
#-----Write to MongoDB ----------------------------------------------------------------------            

        #MiscDT200281351_FX200238_Tax_returns_MI200281_000418_C0-000002
        MIItemID_match_mongodb = 'MI'+re.search(r'MI(.*?)_', filename3).group(1)
        JsonNo_match_mongodb = re.search(r'.*?\_\d(.*?)_.*', filename3).group(1)
        pageno_section_mongodb = filename3.split('_')
        tokens=len(pageno_section_mongodb)
        actual_page_number_mongodb = pageno_section_mongodb[tokens-1].split('.')[0].split('-')[1]
        file_extension_mongodb = pageno_section_mongodb[tokens-1].split('.')[1]

        "MI200281_000418_C0-000002"
        mongo_pageid = MIItemID_match_mongodb+'_'+JsonNo_match_mongodb+'_C0-'+actual_page_number_mongodb
        print(mongo_pageid)
        
        if use_mongo_db=='1':
            mycol.update_one({"PAGEID":mongo_pageid},{"$set":{"invalidatedxml_flag":"1","fileloc":root,"invalidated_status":"false"}})
#-----------------------------------------------------------------------------------------                 
# REMOVE EMPTY FOLDERS
for root, dirs, files in os.walk(directory):    
    if len(os.listdir(root))==0:
        shutil.rmtree(root)

invalid_list.close()

print("----------------------------------------------------------------------------------------")
print("Ended at : ",datetime.now())

# ------ END  --------------------------