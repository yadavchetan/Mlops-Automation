#!/usr/bin/env python
# coding: utf-8
# ------------------------------------------------------------------------------#
# Script: Renaming File, Folders and Documents post AIQ download.
# Initial Author: Omkar Sonar
# Date: 23/08/2021

    
# Revisions:
# ----------------------------------------------------------------
# Date    |    Author       | Description
# ----------------------------------------------------------------
"""
Pending
Input: Only 1 folder name format; FX_DT. Doc type (from user), required Prefix string (from user). Only 1 file name format: doctype_MIid.pdf (rarely .png,. layout, .xml, thumb.db,. json   rename and dump to another folder before zipping)

Process: first rename folder to  adding prefix to DT ID: <prefix>DT_FX and appending doc type: <prefix>DT_FX_doctype. Now rename filename by prepending <prefix>DT_FX_doctype-MI to exiting name.
-    Rename and Remove other extension than .pdf dump to another location. 
-    Zip the folder.

Output: Generate listing before zipping, generate listing after zipping.

Code:
- path to directory
- user rinput for doc type
- file/directory walk
- consider only pdf
- copy other file formats to dump location - need one dump location
- renaming functionality for both file and folder
- zip functionality

"""

# ------------------------------------------------------------------------------#


# IMPORT PACKAGES

import json            # for handling json data
import pandas as pd    # for handling data
import os              # for handling folder creation, folder traversing
import numpy
import shutil          # for file copy
import seaborn as sns  # for Bar-chart generation
from datetime import datetime
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

pd.set_option("display.max_columns",None)

# assign directory
doc_type = sys.argv[3]#'Urla' #Tax Returns, Loan Estimate, IRS W_2
prefix = sys.argv[4]#'Urla' #88825,
#directory = 'F:/ngade-new/Omkar/Additional_Borrower_Before_Upload - Copy'
output_dir = sys.argv[2]#"E:/Stress_Testing/Pre_Renaming/20220214_010000"
report_directory = sys.argv[1]#'E:/Stress_Testing/Pre_Renaming/20220214_010000'
preprocess_path = sys.argv[5]

for root, dirs, files in os.walk(preprocess_path):
    totalsubDir = 0
    if (root.find("FX")>0):
        #print(root)
        shutil.move(root,report_directory,copy_function=shutil.copytree)
        totalsubDir += 1

print('Total Folders Copied : ',totalsubDir)


nonxmlfile_dt_dump_path = os.path.join(output_dir,'nonxmldt_dump')
if os.path.isdir(nonxmlfile_dt_dump_path):
    shutil.rmtree(nonxmlfile_dt_dump_path,ignore_errors=True)
os.mkdir(nonxmlfile_dt_dump_path)


zip_path = os.path.join(report_directory,'zip')
if os.path.isdir(zip_path):
    shutil.rmtree(zip_path,ignore_errors=True)
os.mkdir(zip_path)

#"sprint("Source Directory Before Uploading: ",directory)
print("Target Directory for renaming After Downloading: ",report_directory)
print("DT with non XML files will be dumped to  : ",nonxmlfile_dt_dump_path)

#--------------------------------------------------------------------------------------------------------------------------
#8825DT456789_FX123456_TR_MI-
#--------------------------------------------------------------------------------------------------------------------------
#result_list_downloaded = []
# Prepare a column header list. Remember, if any field is getting added in the table, its column name should also get added here.
#column_tuple = ('FileName','New_folder','New_Document','New_MailItemID','New_json_page_number','New_actual_page_number','File_extension','pageno_wise_rank')
#result_list_downloaded.append(column_tuple)

#before_list = open("before_listing.txt","a")
after_list = open(report_directory+"after_listing.txt","a")
zip_list = open(report_directory+"zip_listing.txt","a")

for root, dirs, files in os.walk(report_directory):
    page_count = 0
    print(root)
    #before_list.write(root+ os.linesep)
    if root == report_directory:
        continue
    if root == zip_path:
        continue        
    for filename in files:
        #before_list.write(filename+ os.linesep)
        if filename.endswith('.pdf'):
            page_count = page_count+1
        if filename.endswith('.zip'):
            continue
        if filename.endswith('.txt'):
            continue
        if filename.endswith('.json'):
            shutil.move(os.path.join(root,filename),os.path.join(nonxmlfile_dt_dump_path),copy_function=shutil.copytree)
            continue
        if filename.endswith('.csv'):
            continue  
        if filename.endswith('.db'):
            #print(os.path.join(root,filename))
            os.remove(os.path.join(root,filename))
            continue             
        #D:\ADE\FX\DT\\page*.json
        print(root)
        #print(dirs)
        #print(filename)
        x = root.split("\\")
        #print(x)
        #x=["ade","fx","dt"]
        x = x[len(x)-1]
        #print('x - ',x)
        x = x.split('_')
        folder = x[0]
        doc = x[1]
        #print(folder)
        #print(doc)
        print(filename,' ---> ',prefix+doc+"_"+folder+"_"+doc_type+"_"+filename)
        renamed_filename = prefix+doc+"_"+folder+"_"+doc_type+"_"+filename
        
        original_file_path = os.path.join(root, filename)
        renamed_file_path = os.path.join(root, renamed_filename)

        if  os.path.exists(original_file_path):
            os.rename(original_file_path, renamed_file_path)
            print(original_file_path," --> ",renamed_file_path, "  :: renamed")
            
            
        #print('**** ',root,'----',d)
        if filename.endswith('.xml'):
            shutil.move(renamed_file_path,os.path.join(nonxmlfile_dt_dump_path),copy_function=shutil.copytree)
        if filename.endswith('.png'):
            shutil.move(renamed_file_path,os.path.join(nonxmlfile_dt_dump_path),copy_function=shutil.copytree)
        if filename.endswith('.layout'):
            shutil.move(renamed_file_path,os.path.join(nonxmlfile_dt_dump_path),copy_function=shutil.copytree)

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
            mycol.update_one({"PAGEID":mongo_pageid},{"$set":{"prerenaming_flag":"1","prerenaming_fileloc":root,"prerenaming_original_filename":filename,"prerenaming_renamed_filename":renamed_filename}})
#-----------------------------------------------------------------------------------------
            
    y = root.split("\\")
    orig_fn = y[len(y)-1]
    y = y[len(y)-1]
    y = y.split('_')
    folder1 = y[0]
    doc1 = y[1]  
    
    renamed_foldername = prefix+doc1+"_"+folder1+"_"+doc_type+"_"+str(page_count)
    print(orig_fn,' ---> ',renamed_foldername)
    
    original_folder_path = root
    renamed_folder_path=original_folder_path.replace(orig_fn,renamed_foldername)
    
    print(original_folder_path)
    print(renamed_folder_path)
    if  os.path.exists(original_folder_path):
        os.rename(original_folder_path, renamed_folder_path)
        print(original_folder_path," --> ",renamed_folder_path, "  :: renamed")
        
    shutil.make_archive(renamed_folder_path,'zip',renamed_folder_path)


for root, dirs, files in os.walk(report_directory):
    if root == zip_path:
        continue
    for filename in files:
        if filename.endswith('.txt'):
            continue
        if filename.endswith('.zip'):
            shutil.move(os.path.join(root,filename),os.path.join(zip_path),copy_function=shutil.copytree)
        after_list.write(os.path.join(root,filename)+ os.linesep)

for root, dirs, files in os.walk(zip_path):
    for filename in files:
        if filename.endswith('.txt'):
            continue
        zip_list.write(os.path.join(root,filename)+ os.linesep)
        
#before_list.close()
after_list.close()
zip_list.close()

if use_mongo_db=='1':
    client.close()