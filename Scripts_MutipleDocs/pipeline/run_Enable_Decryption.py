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

import sys
import configparser
from write_logs import writetolog
import os
#import getpass

JiraID = sys.argv[1]

read_config = configparser.ConfigParser()
read_config.read('../../Config/'+JiraID+'_config.ini')
print('../../Config/'+JiraID+'_config.ini')

input_directory = read_config.get("Decryption", "production_download_directory")
output_directory = read_config.get("Decryption", "decryption_directory")
doc_type_list = read_config.get("JiraInfo", "doctype")

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

print('../process/Enable_Decryption.py ', input_directory,' ',output_directory,' ',doc_type_list)
os.system('python ../process/Enable_Decryption.py '+input_directory+' '+output_directory+' '+doc_type_list)
#writetolog("Script enable_pre_renaming.py executed successfully at"+datetime())
writetolog('['+os.getlogin() +']'+ os.path.basename(__file__)+' Run Successfully - JiraID: '+JiraID+'  DocType: '+doc_type_list+'  Folders: '+str(totalDir)+'Pages: '+str(totalFiles))
print('Logged Successfuly')