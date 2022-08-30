#!/usr/bin/env python
# coding: utf-8
# ------------------------------------------------------------------------------#
# Script: Segregate production downloaded multi document types documents into doc type wise folders
# Initial Author: Omkar Sonar
# Date: 02/05/2022

    
# Revisions:
# ----------------------------------------------------------------
# Date    |    Author       | Description
# ----------------------------------------------------------------


# ------------------------------------------------------------------------------#


# IMPORT PACKAGES

import os              # for handling folder creation, folder traversing
import numpy
import shutil          # for file copy
from datetime import datetime
import subprocess
import re
import sys
import configparser

#-------------------------------------------------------------------------------
proddownload_data_path = "\\\qumsfs.sfsecure.net\ML_Ops_Data_FS\Data_Bank\Segrigation_Testing\Decrypted"
segregation_output_path = "\\\qumsfs.sfsecure.net\ML_Ops_Data_FS\Data_Bank\Segrigation_Testing\Result1"

print(proddownload_data_path)
print(segregation_output_path)

doctype_bucket_set = set()

for root, dirs, files in os.walk(proddownload_data_path):
    print('--------------------------')
    x = root.split('/')
    x = x[len(x)-1]
    x = x.split('\\')
    folder = x[len(x)-2]
    doc = x[len(x)-1]
    
    path_tuple = (folder, doc)
    folder_name_dir = folder
    dt_name_dir = doc
    #print(path_tuple)
    print(doctype_bucket_set)
    if doc.startswith('DT'):
        #print(doc)
        suffix_doctype = doc.split('-')[1]
        
        if suffix_doctype not in doctype_bucket_set:
            os.mkdir(os.path.join(segregation_output_path, suffix_doctype))
            #print('Folder ',os.path.join(class_namewise_doc_directory_path, class_name_dir),' created')
            doctype_bucket_set.add(suffix_doctype)
                    
        print(doctype_bucket_set,' ** ',suffix_doctype) 
        print(os.path.join(proddownload_data_path,folder_name_dir,doc),' --> ',os.path.join(segregation_output_path,suffix_doctype,folder_name_dir,doc))
        if not os.path.isdir(os.path.join(segregation_output_path,suffix_doctype, folder_name_dir)):
            os.mkdir(os.path.join(segregation_output_path,suffix_doctype, folder_name_dir))
#            
        shutil.move(os.path.join(proddownload_data_path,folder_name_dir,doc),os.path.join(segregation_output_path,suffix_doctype,folder_name_dir),copy_function=shutil.copytree)
        
# REMOVE EMPTY FOLDERS
for root, dirs, files in os.walk(proddownload_data_path):    
    if len(os.listdir(root))==0:
        shutil.rmtree(root)