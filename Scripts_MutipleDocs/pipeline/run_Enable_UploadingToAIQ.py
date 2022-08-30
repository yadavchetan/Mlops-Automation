#!/usr/bin/env python
# coding: utf-8
# ------------------------------------------------------------------------------#
# Script: 
# Initial Author: Omkar Sonar
# Date: 

    
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

prenamed_zip_source_path = read_config.get("aiqupload","aiqupload_source")
doc_type = read_config.get("JiraInfo","useriput_ade_aiq_upload_document_type")

 
totalDir = 0
totalFiles = 0
for root, dirs, files in os.walk(prenamed_zip_source_path):
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

print('../process/Enable_UploadingToAIQ.py ', prenamed_zip_source_path)
os.system('python ../process/Enable_UploadingToAIQ.py '+prenamed_zip_source_path)
#writetolog("Script enable_pre_renaming.py executed successfully at"+datetime())
writetolog('['+os.getlogin() +']'+ os.path.basename(__file__)+' Run Successfully - JiraID: '+JiraID+'  DocType: '+doc_type+'  Folders: '+str(totalDir)+'Pages: '+str(totalFiles))