#!/usr/bin/env python
# coding: utf-8
# ------------------------------------------------------------------------------#
# Script: Automatic_Data_Extraction_From_JSON_File
# Initial Author: Omkar Sonar
# Date: 15/06/2021

# Aim: 
#     - Profile the Json information and page pdfs
#     - Bucketise the Pages as per the classname and headers classification.
#     - Get summary reports and page wise information
    
# Revisions:
# ----------------------------------------------------------------
# Date    |    Author       | Description
# ----------------------------------------------------------------


# ------------------------------------------------------------------------------#


# IMPORT PACKAGES

import json            # for handling json data
import pandas as pd    # for handling data
import os              # for handling folder creation, folder traversing
import numpy
import shutil          # for file copy
#import seaborn as sns  # for Bar-chart generation
import sys
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
    

# USER INPUTS
    #directory = 'D:/ADE'
    #report_directory = 'D:/ADE_Output'

directory = sys.argv[1]
report_directory = sys.argv[2]
doc_type = sys.argv[3]

print("Started at : ",datetime.now())

print("The directory under process is :", directory)
print("The Outputs will be recorded at location :", report_directory)
print("The Documents are of type :", doc_type)


# READ THE HEADER OR PROFILE OF INFORMATION OF THE DOCUMENT. This does not contain the page wise parsing.

def read_json(path):
    f = open(path,)
    Json_content = json.load(f)
    pageInfoList_json = Json_content['pageInfoList']
    del Json_content['pageInfoList']
    
    # READ THE HEADER 
    header_info_tuple = (Json_content['friendlyId'],Json_content['typeName'],Json_content['typeDescription'],Json_content['pageCount'])
    return tuple(header_info_tuple), pageInfoList_json;


# User dont have to create the folder directory. User simple needs to mention the folder name and path in input.
if  os.path.exists(report_directory):
    shutil.rmtree(report_directory)
os.mkdir(report_directory)

# This is parent bucket folder where all classname wise buckets would be created
class_namewise_doc_directory = 'class_namewise_documents'
class_namewise_doc_directory_path = os.path.join(report_directory, class_namewise_doc_directory)

if  os.path.exists(class_namewise_doc_directory_path):
    shutil.rmtree(class_namewise_doc_directory_path)
os.mkdir(class_namewise_doc_directory_path)

print("The Bucket Parent Directory is created at location :", class_namewise_doc_directory_path)

result_list = []

# set of letters - each classname will be noted only once. If the new arriving class name is not present in this set, then only the folder will get created
class_name_set = set()


# Prepare a column header list. Remember, if any field is getting added in the table, its column name should also get added here.
column_tuple = ('FolderName', 'DocumentFolder','friendlyID', 'typeName', 'typeDescription','total_pageCount','page_No', 'className','bucketName','ClassVariantFound', 'header','Filename','FileLocation')
result_list.append(column_tuple)

# ==============================================================================================================================
# MAIN SECTION - Traverse through all folders, document folders, Jsons and Page PDFs.

totalDir = 0
totalFiles = 0
for root, dirs, files in os.walk(directory):
    totalsubDir = 0
    totalsubFiles = 0
    filescancount = 0

    for directories in dirs:
        totalsubDir += 1
    totalDir = totalDir + totalsubDir

    for Files in files:
        if Files.endswith('.pdf'):
            totalsubFiles += 1
    totalFiles = totalFiles + totalsubFiles

print('Total Folders to be scanned : ', totalDir)
print('Total Pages to be classified : ', totalFiles)

count = 0

def incremental_doc(filename):

    if use_mongo_db=='1':
        if filename.endswith('.pdf'):
            MIItemID_match_mongodb = 'MI'+re.search(r'MI(.*?)_', filename).group(1)
            JsonNo_match_mongodb = re.search(r'.*?\_\d(.*?)_.*', filename).group(1)
            pageno_section_mongodb = filename.split('_')
            tokens=len(pageno_section_mongodb)
            actual_page_number_mongodb = pageno_section_mongodb[tokens-1].split('.')[0].split('-')[1]
            file_extension_mongodb = pageno_section_mongodb[tokens-1].split('.')[1]

            "MI200281_000418_C0-000002"
            mongo_pageid = MIItemID_match_mongodb+'_'+JsonNo_match_mongodb+'_C0-'+actual_page_number_mongodb        
            abc = mycol.find({'PAGEID':mongo_pageid}).forEach(printjson)
            if len(list(abc))>0:
                incrementall_flag=0
            else:
                incrementall_flag=1
        return incrementall_flag

for root, dirs, files in os.walk(directory):
        
    for filename in files:
        inc_flag = incremental_doc(filename)
        if inc_flag == 0:
            print('Page ',filename,' already processed')
            continue
        
        if count%1000==0:
            print(count, ' pages classified at ',datetime.now())
        
        x = root.split('/')
        x = x[len(x)-1]
        x = x.split('\\')
        folder = x[len(x)-2]
        doc = x[len(x)-1]
        
        # This tuple stores the folder level column values of the final report
        path_tuple = (folder, doc)
        folder_name_dir = folder
        dt_name_dir = doc
        if filename.endswith('.json'):
            path = os.path.join(root, filename)
            
            # METHOD to extract the Document level information for the final report. One document has many pages.
            (header_info_tuple,pageInfoList_json) = read_json(path)
            
            # This tuple stores the document level information
            record = path_tuple+header_info_tuple
            
            # --------------------------------------------------------------------------------------------------------------
            # This section will traverse through all pages, extract each page level details, copy the page to the respective classname bucket
            # This will merge the page level information to the document level data and create the final report for one page
            folder_name_set = set()
            dt_name_set = set()
            page_no = 1   # initiate the page no calculation
            for ele in pageInfoList_json:
                
                # Create the classname bucket folder if not already present. The same directory name will be use to copy the file.
                if 'className' in ele['meta']:
                    class_name_dir = ele['meta']['className']
                    bucket_dir = class_name_dir
                else:
                    class_name_dir = 'UnClassified'
                    bucket_dir = class_name_dir

                if 'header' in ele['meta']:
                    header=ele['meta']['header']
                else:
                    header='header_unknown'
                    
                #--Enchancement 25June--handle Schedule classification exemptions--------------
                import csv
                with open('../../Lib/classification_exemption_list.csv', mode='r') as infile_exemption:
                    reader_exemption = csv.reader(infile_exemption)
                    mydict_exemption = {rows[0]:rows[1] for rows in reader_exemption}
                #--Enchancement 18June---------------------------------------------------------
                class_variant_found='None'
                reclassification_list = ['Misc-TaxReturns','Unknown','UnClassified','IRS1120']
                f = open("../../Lib/classname_correction_mappings.csv", "r")
                with open('../../Lib/classname_correction_mappings.csv', mode='r') as infile:
                    reader = csv.reader(infile)
                    mydict = {rows[0]:rows[1] for rows in reader}
                #print(mydict)
                if (class_name_dir in reclassification_list):
                    #print(class_name_dir)
                    #header = "abcdef Form 1040XN fnfioeienec"
                    if [key for key,val in mydict.items() if (header.find(key) != -1)]:
                        #print(max([val for key,val in mydict.items() if (header.find(key) != -1)],key=len))
                        class_variant_found = max([key for key,val in mydict.items() if (header.find(key) != -1)],key=len)
                        bucket_dir = max([val for key,val in mydict.items() if (header.find(key) != -1)],key=len)

                        # Remove from classification bucket because of exception. This page has been exempted from classification. (25June)
                        if [key for key,val in mydict_exemption.items() if (header.find(key) != -1)]:
                            #print(max([val for key,val in mydict.items() if (header.find(key) != -1)],key=len))
                            class_variant_found = 'None'
                            bucket_dir = class_name_dir

                #-----------------------------------------------------------
                #print(bucket_dir)
                if bucket_dir not in class_name_set:
                    os.mkdir(os.path.join(class_namewise_doc_directory_path, bucket_dir))
                    #print('Folder ',os.path.join(class_namewise_doc_directory_path, class_name_dir),' created')
                    class_name_set.add(bucket_dir)
                 
                if not os.path.isdir(os.path.join(class_namewise_doc_directory_path, bucket_dir,folder_name_dir)):
                    os.mkdir(os.path.join(class_namewise_doc_directory_path, bucket_dir,folder_name_dir))
                    #print('Folder ',os.path.join(class_namewise_doc_directory_path, bucket_dir,folder_name_dir),' created')                

                if not os.path.isdir(os.path.join(class_namewise_doc_directory_path, bucket_dir,folder_name_dir,dt_name_dir)):
                    os.mkdir(os.path.join(class_namewise_doc_directory_path, bucket_dir,folder_name_dir,dt_name_dir))
                    #print('Folder ',os.path.join(class_namewise_doc_directory_path, bucket_dir,folder_name_dir,dt_name_dir),' created')

                    
                print(class_name_set)
                
                # ---------------------------------------------------------------------------------------------------------
                # Json does not have file name with pageno. - Hence we will have to compute the qualified file name here.
                if page_no < 10:
                    page_no_str = '00000'+str(page_no)
                    page_no = page_no+1
                elif page_no >= 10 and page_no < 100:
                    page_no_str = '0000'+str(page_no)
                    page_no = page_no+1
                elif page_no >= 100 and  page_no < 1000:
                    page_no_str = '000'+str(page_no)
                    page_no = page_no+1
                elif page_no >= 1000:
                     page_no_str = '00'+str(page_no)
                     page_no = page_no+1

                file_name = ele['id'] +"-"+ page_no_str + ".pdf"
                #print(file_name)
                if file_name.find('#')!= -1:
                    file_name = file_name.split('#')
                    file_name = doc_type+'-'+ file_name[1]
                else:
                    file_name = doc_type+'-'+ file_name
                #print(file_name)
                # ---------------------------------------------------------------------------------------------------------
                
                # Copy the file to the current classname bucket
                new_file_name = path_tuple[0]+"_"+path_tuple[1]+"_"+file_name
                new_file_path = os.path.join(class_namewise_doc_directory_path,bucket_dir,folder_name_dir,dt_name_dir)+"\\"+new_file_name
                shutil.copyfile(root+"\\"+file_name, os.path.join(class_namewise_doc_directory_path, bucket_dir,folder_name_dir,dt_name_dir)+"\\"+new_file_name)
                
                # Create the page level record
                pageleve_info_tuple = (page_no_str,class_name_dir,bucket_dir,class_variant_found,header, file_name,new_file_path)               
                final_record = record+pageleve_info_tuple
                result_list.append(final_record)
        #-----Write to MongoDB ----------------------------------------------------------------------            
                #print(filename)
                #MiscDT200281351_FX200238_Tax_returns_MI200281_000418_C0-000002
                if file_name.endswith('.pdf'):
                    MIItemID_match_mongodb = 'MI'+re.search(r'MI(.*?)_', file_name).group(1)
                    JsonNo_match_mongodb = re.search(r'.*?\_\d(.*?)_.*', file_name).group(1)
                    pageno_section_mongodb = file_name.split('_')
                    tokens=len(pageno_section_mongodb)
                    actual_page_number_mongodb = pageno_section_mongodb[tokens-1].split('.')[0].split('-')[1]
                    file_extension_mongodb = pageno_section_mongodb[tokens-1].split('.')[1]

                    "MI200281_000418_C0-000002"
                    mongo_pageid = MIItemID_match_mongodb+'_'+JsonNo_match_mongodb+'_C0-'+actual_page_number_mongodb
                    print(mongo_pageid)
                    
                    if use_mongo_db=='1':
                        mycol.update_one({"PAGEID":mongo_pageid},{"$set":{"postclassification_flag":"1","fileloc":root,"classname":class_name_dir,"bucket_name":bucket_dir,"classified_new_location":new_file_path}})
        #-----------------------------------------------------------------------------------------                
        count=count+1
# =================================================================================================================================

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
# Create final report Dataframe
df = pd.DataFrame(result_list[1:len(result_list)],columns =list(result_list[0]))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

# Charts Representations ClassName vs Frequency
#ax = sns.countplot(x="className",data=df)
#fig = ax.get_figure()

# CREATE REPORTS -----------------------------------------------------------------------------
folderwise_summary_report = df.pivot_table(index ='FolderName', columns ='className', values ='friendlyID', aggfunc='count')

unique_classNames_freq = df.pivot_table(index =['className','bucketName'], values ='friendlyID', aggfunc='count').reset_index()
#unique_classNames_freq.columns=[['ClassName','Frequency']]

unique_header_freq = df.pivot_table(index =['header'], values ='friendlyID', aggfunc='count').reset_index()
#unique_header_freq.columns=[['Header','Frequency']]

unique_classNames_header_combi_freq = df.pivot_table(index =['className','header'], values ='friendlyID', aggfunc='count').reset_index()
#unique_classNames_header_combi_freq.columns=[['ClassName','Header','Frequency']]

unclassified_pagelist = df[df.className == 'UnClassified']

# SAVE THE REPORT FILES -----------------------------------------------------------------------

#fig.savefig(report_directory+"/output.png")
df.to_csv(report_directory+'/report.csv')
folderwise_summary_report.to_csv(report_directory+'/folderwise_summary_report.csv')
unique_classNames_freq.to_csv(report_directory+'/unique_classNames_bucket_freq.csv')
unique_header_freq.to_csv(report_directory+'/unique_header_freq.csv')
unique_classNames_header_combi_freq.to_csv(report_directory+'/unique_classNames_header_combi_freq.csv')
unclassified_pagelist.to_csv(report_directory+'/unclassified_pagelist.csv')

print("Successfully Completed at : ",datetime.now())
# ------ END ----------------------------------------------------------------------------------------------