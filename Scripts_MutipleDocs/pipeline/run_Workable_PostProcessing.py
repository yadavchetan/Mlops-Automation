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
import shutil
from shutil import copytree, ignore_patterns

JiraID = sys.argv[1]

read_config = configparser.ConfigParser()
read_config.read('../../Config/'+JiraID+'_config.ini')
print('../../Config/'+JiraID+'_config.ini')

postrenaming_output = read_config.get("PostRenaming","postrenaming_output_dir")
postprocessing_tool_path = read_config.get("PostProcessing","postprocessing_tool_path")
run_bat_path = read_config.get("PostProcessing","postprocessing_run_stat")
run_postprocessing_path = read_config.get("PostProcessing","postprocessing_run_postprocessing")
doc_type = read_config.get("PostProcessing","postprocessing_doctype")
input_directory = read_config.get("PostProcessing","input_loc")
output_directory = read_config.get("PostProcessing","output_loc")

if  os.path.exists(postrenaming_output):
    if  os.path.exists(input_directory):
        shutil.copytree(postrenaming_output, input_directory,dirs_exist_ok=True,ignore=ignore_patterns('*.csv', 'nonxml*'))

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

print('../process/Workable_PostProcessing.py', postprocessing_tool_path,' ', run_bat_path,' ',run_postprocessing_path,' ',doc_type,' ',input_directory,' ',output_directory)
os.system('python ../process/Workable_PostProcessing.py '+' '+ postprocessing_tool_path+' '+ run_bat_path+' '+run_postprocessing_path+' '+doc_type+' '+input_directory+' '+output_directory)
#writetolog("Script enable_pre_renaming.py executed successfully at"+datetime())
writetolog('['+os.getlogin() +']'+ os.path.basename(__file__)+' Run Successfully - JiraID: '+JiraID+'  DocType: '+doc_type+'  Folders: '+str(totalDir)+'Pages: '+str(totalFiles))