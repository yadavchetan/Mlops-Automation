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


# ------------------------------------------------------------------------------#

import sys
import configparser
from write_logs import writetolog
import os
import getpass

JiraID = sys.argv[1]

read_config = configparser.ConfigParser()
read_config.read('../../Config/'+JiraID+'_config.ini')
print('../../Config/'+JiraID+'_config.ini')

preprocess_tool_path = read_config.get("ToolInfo","preprocess_tool_path")
mainfolder_bat_path = read_config.get("ToolInfo","preprocess_mainfolder_bat_path")
conversion_bat_path = read_config.get("ToolInfo","preprocess_conversion_bat_path")
deskew_tool_path = read_config.get("ToolInfo","preprocess_deskew_tool_path")
deskew_bat_path = read_config.get("ToolInfo","preprocess_deskew_bat_path")
doc_type = read_config.get("PreProcess","preprocess_doctype")
pdf2png = read_config.get("PreProcess","pdf2png")
preprocess_path = read_config.get("PreProcess","preprocess_loc")
decrypted_folder_path = read_config.get("PreProcess","preprocess_decryption_input_loc")

conversion_folder_path = os.path.join(preprocess_path,doc_type,'1_Conversion')
deskew_folder_path = os.path.join(preprocess_path,doc_type,'2_Deskew')
deduplication_folder_path = os.path.join(preprocess_path,doc_type,'3_De-Duplication')
decryption_doctype_path = os.path.join(decrypted_folder_path,doc_type)

print('Document Type for preprocessing : ',doc_type)
print('The data is read from location : ',decryption_doctype_path)
print('Conversion folder path : ',conversion_folder_path)
print('Deskew folder path : ',deskew_folder_path)


#conversion_folder_path = read_config.get("PreProcess","preprocess_conversion_loc")
#deskew_folder_path = read_config.get("PreProcess","preprocess_deskew_loc")
 
totalDir = 0
totalFiles = 0
for root, dirs, files in os.walk(decryption_doctype_path):
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

print('../process/Enable_PreProcessing.py "', preprocess_tool_path,'" "',mainfolder_bat_path,'" "' ,conversion_bat_path,'" "',deskew_tool_path,'" "' ,deskew_bat_path,'" "' ,doc_type,'" ' ,pdf2png,' "' ,decryption_doctype_path,'" "' ,conversion_folder_path,'" "',deskew_folder_path,'"')
os.system('python ../process/Enable_PreProcessing.py "'+preprocess_tool_path+'" "'+mainfolder_bat_path+'" "'+conversion_bat_path+'" "'+deskew_tool_path+'" "'+deskew_bat_path+'" "'+doc_type+'" '+pdf2png+' "'+decryption_doctype_path+'" "'+conversion_folder_path+'" "'+deskew_folder_path+'"')
#writetolog("Script enable_pre_renaming.py executed successfully at"+datetime())
#writetolog('['+os.getlogin() +']'+ os.path.basename(__file__)+' Run Successfully - JiraID: '+JiraID+'  DocType: '+doc_type+'  Folders: '+str(totalDir)+'Pages: '+str(totalFiles))