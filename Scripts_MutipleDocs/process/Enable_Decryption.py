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
1. Decrypted data Location

"""

# ------------------------------------------------------------------------------#


# IMPORT PACKAGES

import os              # for handling folder creation, folder traversing
import numpy
import shutil          # for file copy
from datetime import datetime
import subprocess
import re
import sys
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
#hostname =	localhost,port	 =	27017,dbstring = 'MLOPSDATABANK'
#-------------------------------------------------------------------------------
proddownload_data_path = sys.argv[1]
decryption_path = sys.argv[2]
doctype_list = sys.argv[3]
#proddownload_data_path = "E:/ADR_Area51_Automation/Data/Decrypted"

print("Location of Production Download Documents : ",proddownload_data_path)

for root, dirs, files in os.walk(proddownload_data_path):

    x = root.split('/')
    x = x[len(x)-1]
    x = x.split('\\')
    folder = x[len(x)-2]
    doc = x[len(x)-1]
    
    path_tuple = (folder, doc)
    folder_name_dir = folder
    dt_name_dir = doc
    
    if doc.startswith('DT'):
        print(doc)
        suffix_doctype = doc.split('-')[1]
        print(suffix_doctype) 
        print(os.path.join(proddownload_data_path,folder_name_dir,doc),' --> ',os.path.join(decryption_path,suffix_doctype,folder_name_dir,doc))
        if not os.path.isdir(os.path.join(decryption_path,suffix_doctype, folder_name_dir)):
            os.mkdir(os.path.join(decryption_path,suffix_doctype, folder_name_dir))
#            
        shutil.move(os.path.join(proddownload_data_path,folder_name_dir,doc),os.path.join(decryption_path,suffix_doctype,folder_name_dir),copy_function=shutil.copytree)


totalDir = 0
totalFiles = 0
for root, dirs, files in os.walk(proddownload_data_path):
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
        if Files.endswith('.db'):
            continue
        else:
            totalsubFiles += 1
    totalFiles = totalFiles + totalsubFiles

print('Total Folders to be scanned : ', totalDir)
print('Total Files to be scanned : ', totalFiles)

for root, dirs, files in os.walk(proddownload_data_path): #Search the path in detal
    for f in files:
        print(f)
        if f.endswith('.json'):
            continue
        if f.endswith('.db'):
            continue
        op = os.path.join(root,f)
                
        data = op.split('\\')
#         print(data)
#         print(len(data) )
#FHA Mortgage Credit Analysis Worksheet-MI210870684_000364_C0-000001.png
        FX_match = data[len(data)-3]
        DT_match = re.search(r'^(.*)-', data[len(data)-2]).group(1)
        doctype_match = re.search(r'-(.*)$', data[len(data)-2]).group(1)
        pagename = data[len(data)-1]
        MI_match = re.search(r'-(.*?)_', pagename).group(1)
        JsonNo_match = re.search(r'.*?\_\d(.*?)_', pagename).group(1)
        pageno_section = pagename.split('_')
        tokens=len(pageno_section)
        actual_page_number = pageno_section[tokens-1].split('.')[0].split('-')[1]
        file_extension = pageno_section[tokens-1].split('.')[1]
        page_id = MI_match+'_'+JsonNo_match+'_C0-'+actual_page_number
        
        #print(FX_match)
        #print(DT_match)
        #print(MI_match)
        #print(doctype_match)
        #print(pagename)
        #print(JsonNo_match)
        #print(actual_page_number)
        #print(file_extension)
        #print(page_id)

        rec = {
        "PAGEID":page_id,
        "FXID":FX_match,
        "DTID":DT_match,
        "MIID":MI_match,
        "DOCTYPE":doctype_match,
        "JSONNUMBER":JsonNo_match,
        "PAGENUMBER":actual_page_number,
        "EXTENSION":file_extension,
        "download_flag":"1",
        "prerenaming_flag":"0",
        "preclassification_flag":"0",
        "postrenaming_flag":"0",
        "postclassification_flag":"0",
        "invalidatedxml_flag":"0",
        "preprocessing_flag":"0",
        "postprocessing_flag":"0",
        "uploadtoaiq_flag":"0",
        "adr_ar51_transfer_flag":"0",
		"document_process_mode":"Current"
        }
        #print(rec)

        mycol.insert_one(rec)
