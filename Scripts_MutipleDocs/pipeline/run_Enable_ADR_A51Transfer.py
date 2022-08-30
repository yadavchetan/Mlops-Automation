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

encryption_tool_path = read_config.get("ToolInfo","encryption_tool_path")
encryption_tool_folder = read_config.get("ToolInfo","encryption_tool_folder")

decrypted_data_path = read_config.get("AdrA51Transfer","decrypted_data_path")
encrypted_data_output_path = read_config.get("AdrA51Transfer","encrypted_data_output_path")

site = read_config.get("JiraInfo","site")

print('Document being read from Location for ADR : ',decrypted_data_path)
print('Encrypted Data copied to Locatin : ',encrypted_data_output_path)



 
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

print('../process/ADR_Area51_Transfer_Automation.py "', decrypted_data_path,'" "',encrypted_data_output_path,'" "' ,encryption_tool_path,'" "',encryption_tool_folder,'" "',site,'" "',JiraID,'"')
os.system('python ../process/ADR_Area51_Transfer_Automation.py "'+decrypted_data_path+'" "'+encrypted_data_output_path+'" "'+encryption_tool_path+'" "'+encryption_tool_folder+'" "'+site+'" "'+JiraID+'"')
#writetolog("Script enable_pre_renaming.py executed successfully at"+datetime())
writetolog('['+os.getlogin() +']'+ os.path.basename(__file__)+' Run Successfully - JiraID: '+JiraID+'  DocType: '+doc_type+'  Folders: '+str(totalDir)+'Pages: '+str(totalFiles))