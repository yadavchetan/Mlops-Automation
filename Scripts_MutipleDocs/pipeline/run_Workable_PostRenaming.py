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
import sys
import getpass
import shutil
import codecs

JiraID = sys.argv[1]

read_config = configparser.ConfigParser()
read_config.read('../../Config/'+JiraID+'_config.ini')
print('../../Config/'+JiraID+'_config.ini')

report_directory = read_config.get("PostRenaming", "postrenaming_input_directory")
output_dir = read_config.get("PostRenaming", "postrenaming_output_dir")
prefix = read_config.get("PostRenaming", "postrenaming_prefix")
doc_type = read_config.get("PostRenaming", "postrenaming_doctype")
ftp_tracker_path = read_config.get("PostRenaming", "postrenaming_ftptracker")
table1_source_dir_path = read_config.get("PostRenaming", "postrenaming_sourcelisting")
postprocessing_input_path = read_config.get("PostProcessing","input_loc")


#report_directory = repr(report_directory)
#output_dir = repr(output_dir)

print("Preparing to copy data from After AIQ download to Post Renaming Location.")
print(report_directory, " ---> ",output_dir)

#if  os.path.exists(report_directory.replace('\\','/').replace('"','')):
#    if  os.path.exists(output_dir.replace('\\','/').replace('"','')):
#        shutil.copytree(report_directory.replace('\\','/').replace('"',''), output_dir.replace('\\','/').replace('"',''),dirs_exist_ok=True)
#        print("Folders copied for Post Renaming")

#if  os.path.isdir(report_directory):
    #print("------------------------------------------------>")
    #if  os.path.isdir(output_dir):
#shutil.copytree(r'%s'%report_directory, r'%s'%output_dir,dirs_exist_ok=True)
#shutil.copytree(report_directory, output_dir,dirs_exist_ok=True)
#shutil.copytree(report_directory.replace('\\','/').replace('"',''), output_dir.replace('\\','/').replace('"',''),dirs_exist_ok=True)
shutil.copytree(report_directory.replace('"',''), output_dir.replace('"',''),dirs_exist_ok=True)
       #shutil.copytree("\\qumsfs.sfsecure.net\ML_Ops_Data_FS\MLOPSDataBank\ADE\E-Sign_Audit_Log\ADS-1767_ControlSet_350\4_Documents_Downloaded","\\qumsfs.sfsecure.net\ML_Ops_Data_FS\MLOPSDataBank\ADE\E-Sign_Audit_Log\ADS-1767_ControlSet_350\7_PostRenaming",dirs_exist_ok=True)
print("Folders copied for Post Renaming")

totalDir = 0
totalFiles = 0
for root, dirs, files in os.walk(report_directory):
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

print('../process/Workable_PostRenaming.py ', output_dir,' ', output_dir,'  ', doc_type,'  ', prefix,' ',ftp_tracker_path,'  ',table1_source_dir_path)
os.system('python ../process/Workable_PostRenaming.py '+output_dir+' '+output_dir+' '+doc_type+' '+prefix+' '+ftp_tracker_path+' '+table1_source_dir_path)


#--------------------------------------------------------------------------------------------------------------------------


print("---------------",output_dir,"--------------")
for root, dirs, files in os.walk(output_dir.replace('\\','/').replace('"','')):
    for directories in dirs:
        #print(root)
        if (directories.startswith("DT")):
            #print(os.path.join(root,directories))
            #print(root)
            x = root.split("\\")
            fx_name = x[len(x)-1]
            #print(fx_name)
            postprocess_zip_copy_fx_path = os.path.join(postprocessing_input_path,fx_name)
            print("#### >>>",postprocess_zip_copy_fx_path)
            if os.path.isdir(postprocess_zip_copy_fx_path.replace('"','')):
                shutil.rmtree(postprocess_zip_copy_fx_path.replace('"',''),ignore_errors=True)
            os.mkdir(postprocess_zip_copy_fx_path)

            #shutil.make_archive(root,'zip',os.path.join(postprocessing_input_path,x).replace('\\','/').replace('"',''))
            shutil.make_archive(os.path.join(root,directories),'zip',os.path.join(root,directories))

for root, dirs, files in os.walk(output_dir.replace('\\','/').replace('"','')):
    for filename in files:
        if filename.endswith('.txt'):
            continue
        if filename.endswith('.xlsx'):
            continue
        if filename.endswith('.json'):
            continue
        if filename.endswith('.xlsx'):
            continue
        if filename.endswith('.zip'):
            x = root.split("/")
            #print(x)
            x = x[len(x)-1]
            x = x.split('\\')
            folder = x[len(x)-1]
            #print(folder)
            print(os.path.join(root,filename) ,"-->",os.path.join(postprocessing_input_path.replace('\\','/').replace('"',''),folder))
            dtzip_src = os.path.join(root,filename)
            zipcopy_location = os.path.join(postprocessing_input_path,folder)
            shutil.move(dtzip_src.replace('\\','/').replace('"',''),zipcopy_location.replace('\\','/').replace('"',''),copy_function=shutil.copytree)

'''

for root, dirs, files in os.walk(rename_dt_path):
    if root == zip_path:
        continue
    for filename in files:
        if filename.endswith('.txt'):
            continue
        if filename.endswith('.zip'):
            shutil.move(os.path.join(root,filename),os.path.join(zip_path),copy_function=shutil.copytree)
        after_list.write(os.path.join(root,filename)+ os.linesep)
'''
#--------------------------------------------------------------------------------------------------------------------------

#writetolog("Script enable_pre_renaming.py executed successfully at"+datetime())
writetolog('['+os.getlogin() +']'+ os.path.basename(__file__)+' Run Successfully - JiraID: '+JiraID+'  DocType: '+doc_type+'  Folders: '+str(totalDir)+'Pages: '+str(totalFiles))