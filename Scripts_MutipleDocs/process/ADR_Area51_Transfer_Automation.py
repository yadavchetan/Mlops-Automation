#!/usr/bin/env python
# coding: utf-8
# ------------------------------------------------------------------------------#
# Script: ADR Area51 Transfer
# Initial Author: Vinay Dhavan  / Omkar Sonar
# Date: 24/01/2022

    
# Revisions:
# ----------------------------------------------------------------
# Date    |    Author       | Description
# ----------------------------------------------------------------
"""
1. Decrypted data Location
2. Encryption Tool: (Requires 2 paths)
	encryption-tool_21.1.2.0\bin\encryption-tool.bat
	
	Parameters
	1.Decrypted files paths
	2.Destination Path
	Conversion >> .pdf/.png/.json to .pdf.enc/.png.enc/.json.enc
3. Deletion of Json file after encryption
	(json format doesnt fit into Area51 compatability)
    
    3.1 > cleaning of filename & renaming orginal filename with cleaned one
        Create Filename_Cleaning Function
        (filename in forloop)--> Filename_Cleaning() --> Cleaned filename
            Regular expression package use
	
4. Zipping of DT ID's without storing any relative paths
	FX>DT>MI.pdf.enc to FX>DT.zip
	
5. Deletion of DT ID's after zipping (Files no longer required)

6. Pasting Data to Area51/(other accessible location)

**After pasting the data into Area51 some files goes into Archive Errors Due to not
  following area51 compatability i.e Unwanted characters in file name.
  Example>>  . @ & * etc ***
  
Need to Automate this step of removing unwanted characters from filename & DT ID name.
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
from datetime import date
today = date.today()
update_date = today.strftime("%d/%m/%Y")

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
encryption_tool_path = sys.argv[3]
encryption_tool_folder = sys.argv[4]
decrypted_data_path = sys.argv[1]
encrypted_data_output_path = sys.argv[2]
sitename = sys.argv[5]
adr_jiraid = sys.argv[6]

print("Location of Encryption Tool ",encryption_tool_path)
print("Location of Decrypted Data : ",decrypted_data_path)
print("Encrypted Data Dump Location : ",encrypted_data_output_path)
#print("Doc Type : ",doc_type)
#print("Data Input Directory : ",input_directory)

def clean_filename():
    for root, dirs, files in os.walk(encrypted_data_output_path):
        for filename in files:
            if filename.endswith('.png.enc'):
                filename_ext = re.sub('\.png.enc','', filename)
                filename_cleaned = filename_ext.replace('.','')
                finalname = (filename_cleaned + '.png.enc')
                os.rename(os.path.join(root,filename),os.path.join(root,finalname))

def clean_DT():
    for root, dirs, files in os.walk(encrypted_data_output_path):
        for subdirs in dirs:
            if subdirs.startswith("DT"):
                subdirs_cleaned = subdirs.replace('.','')
                os.rename(os.path.join(root,subdirs),os.path.join(root,subdirs_cleaned))
totalDir = 0
totalFiles = 0
for root, dirs, files in os.walk(decrypted_data_path):
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

for root, dirs, files in os.walk(decrypted_data_path): #Search the path in detal
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
        #print(page_id,"-",update_date)


        rec = {
        "PAGEID":page_id,
        "FXID":FX_match,
        "DTID":DT_match,
        "MIID":MI_match,
        "DOCTYPE":doctype_match,
        "JSONNUMBER":JsonNo_match,
        "PAGENUMBER":actual_page_number,
        "EXTENSION":file_extension,
        "adr_flag":"1",
        "update_date":update_date,
        "sitename":sitename
        "adr_jiraid":adr_jiraid
        "adr_document_process_mode":"Current"
        }
        #print(rec)

        mycol.insert_one(rec)

#
#CALL THE ENCRYPTION TOOL
s=subprocess.call([encryption_tool_path,encryption_tool_folder])
#
print("encryption_tool Exit Code : ",s)
#if s==0:
#    s1=subprocess.call([run_postprocessing_path,input_directory,doc_type,postprocessing_tool_path])


for root, dirs, files in os.walk(encrypted_data_output_path):
    for filename in files:
        if filename.endswith('.db'):
            os.remove(os.path.join(root,filename))
            continue
        if filename.endswith('.json.enc'):
            os.remove(os.path.join(root,filename))

clean_filename()
clean_DT()

for root, dirs, files in os.walk(encrypted_data_output_path):
    for subdirs in dirs:
        if subdirs.startswith("DT"):
            #print(root,subdirs)
            DTID_folder_path = os.path.join(root, subdirs)
            #print(DTID_folder_path)
            shutil.make_archive(DTID_folder_path,'zip',DTID_folder_path)
            shutil.rmtree(os.path.join(root, DTID_folder_path))