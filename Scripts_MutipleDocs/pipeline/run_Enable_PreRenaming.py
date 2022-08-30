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

input_directory = read_config.get("PreRenaming", "prerenaming_input_directory")
output_dir = read_config.get("PreRenaming", "output_dir")
prefix = read_config.get("PreRenaming", "prerenaming_prefix")
doc_type = read_config.get("PreRenaming", "prerenaming_doctype")
preprocessed_doc_loc = read_config.get("PreRenaming", "preprocessed_doc_loc")

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

print('../process/Enable_PreRenaming.py ', input_directory,' ', output_dir,'  ', doc_type,'  ', prefix,' ',preprocessed_doc_loc)
os.system('python ../process/Enable_PreRenaming.py '+' '+input_directory+' '+output_dir+' '+doc_type+' '+prefix+' '+preprocessed_doc_loc)
#writetolog("Script enable_pre_renaming.py executed successfully at"+datetime())
writetolog('['+os.getlogin() +']'+ os.path.basename(__file__)+' Run Successfully - JiraID: '+JiraID+'  DocType: '+doc_type+'  Folders: '+str(totalDir)+'Pages: '+str(totalFiles))