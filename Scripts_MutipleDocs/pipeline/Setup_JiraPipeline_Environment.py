#!/usr/bin/env python
# coding: utf-8
# ------------------------------------------------------------------------------#
# Script: Setup JiraRequest_Environment
# Initial Author: Omkar Sonar
# Date: 11/02/2022

# Aim: 
#     - Read selection criteria xml file
#     - Collect jiraid, dates, site and doctype info from either filename or xml content
#     - ask user to opt any one pipeline either ADE download, 
#     - "purpose" input from user
#     - frame the jira workspace folder name and create the folder structure inside the opted pipeline by user
#     - Create a config file - pointing to jirarequest folder and all path variables pointing to locations inside it
#     - 
    
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
import re
from datetime import datetime
import configparser
from bs4 import BeautifulSoup
import mysql.connector

read_config = configparser.ConfigParser()
read_config.read('../../Config/central_config.ini')
print('../../Config/central_config.ini')

databank_location = read_config.get("databank", "databank_location")
selectionxmlFile_location = read_config.get("databank", "prod_selectionxmlFile_location")
doc_name_location = read_config.get("databank", "ade_location")

host_name = read_config.get("mysql_database","hostname")
db_name = read_config.get("mysql_database","dbstring")
user_name = read_config.get("mysql_database","username")
Password = read_config.get("mysql_database","password")

conn = mysql.connector.connect(host=host_name, database=db_name, user=user_name, password=Password)

if conn.is_connected():
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to database: ", record)

def read_seleciton_criteria_xml(selection_criteria_xmlfile_loc):
    # Reading data from the xml file
    with open(selection_criteria_xmlfile_loc, 'r') as f:
        data = f.read()
     
    # Passing the data of the xml
    # file to the xml parser of
    # beautifulsoup
    bs_data = BeautifulSoup(data, 'xml')

    # Collect Jira Request Information
    xmlfilename = os.path.basename(f.name)
    print('xml file name :',xmlfilename)
    var_jiraid = re.search('^(.*?)_', xmlfilename).group(1)
    var_site = re.search('_(.*?)_', xmlfilename).group(1)
    var_server = re.search('(.*)_(.*).xml', xmlfilename).group(2)

    var_startdate = bs_data.find('start-date').text
    var_enddate = bs_data.find('end-date').text
    #var_doctype = bs_data.find('type-description').text
    ele_list = bs_data.find_all('include-document-with-type-description')
    for item in ele_list:
        var_doctype_list = item.text.split('\n')
    var_doctype_list = [ele for ele in var_doctype_list if ele.strip()]

    print('Jira ID : ',var_jiraid)
    print('Start Date : ',var_startdate)
    print('End Date : ',var_enddate)
    print('Document Type : ',var_doctype_list)
    print('Site : ',var_site)
    print('Server : ',var_server)
    #print('Purpose : ',Purpose)
    
    return var_jiraid,var_startdate,var_enddate,var_doctype_list,var_site,var_server
    #ade_dwnld_jira_id_workspace_name = var_jiraid+"_"+var_startdate+"_"+var_enddate
    #ade_aiqupload_jira_id_workspace_name = var_jiraid+"_"+var_startdate+"_"+var_enddate
    #adr_jira_id_workspace_name = var_jiraid
    
# USER INPUTS
    #directory = 'D:/ADE'
    #report_directory = 'D:/ADE_Output'
    #E:/MLOPSDataBank/Prod_Selection_Criteria/ADS-1467_Amerihome_DSPMR.xml

Purpose = 'Initial Labeling'#sys.argv[2]
#XML file = sys.argv[2]
#databank_location = "E:\MLOPSDataBank"
print("Started at : ",datetime.now())

print('Databank is located at : ',databank_location)

print('\n Select any one option to setup a pipeline as per Jira Request')
print('1. ADR')
print('2. ADE Download')
print('3. ADE AIQ Upload \n')
val = input("Enter the pipeline # you wish to execute: ")
print(val,'\n\n')

#user input - selectioncriteria xml file
#'D:/OneDrive - Persistent Systems Limited/PROJECTS/Ellie Mae/POC_DataBank_Automation/DataBank/ADS-1454_Amerihome_DSPMR.xml'

#mysql - insert data

if val=='1':

    useriput_selectioncrtieria_filename = input("Enter the XML Selection Criteria filename: ")
    print(useriput_selectioncrtieria_filename,'\n\n')
    
    selection_criteria_xmlfile_loc = selectionxmlFile_location+"/"+useriput_selectioncrtieria_filename#'/ADS-1401_Amerihome_DSPMR.xml'#sys.argv[1]
    var_jiraid,var_startdate,var_enddate,var_doctype_list,var_site,var_server = read_seleciton_criteria_xml(selection_criteria_xmlfile_loc)
    ade_dwnld_jira_id_workspace_name = var_jiraid+"_"+var_startdate+"_"+var_enddate
    var_doctype_list_for_config = ','.join([docele for docele in var_doctype_list])
    
    adr_jira_id_workspace_name = var_jiraid
    
    adr_site_wise_name = os.path.join(databank_location, 'ADR',var_site)    
    if not os.path.exists(adr_site_wise_name):
        os.mkdir(adr_site_wise_name)

    adr_jira_loc = os.path.join(adr_site_wise_name,adr_jira_id_workspace_name)
    os.mkdir(adr_jira_loc.replace('\\','/').replace('"',''))
    proc_adr_productiondownload_loc = os.path.join(adr_jira_loc,'ProductionDownload')
    os.mkdir(proc_adr_productiondownload_loc)
    proc_adr_decryption_loc = os.path.join(adr_jira_loc,'Decryption')
    os.mkdir(proc_adr_decryption_loc)
    
    proc_adr_encryption_loc = os.path.join(adr_jira_loc,'Encrypted')
    os.mkdir(proc_adr_encryption_loc)
    
    
    write_config = configparser.ConfigParser()
    
    write_config.add_section("JiraInfo")
    write_config.set("JiraInfo","jira_id",var_jiraid)
    write_config.set("JiraInfo","doctype",var_doctype_list_for_config)
    write_config.set("JiraInfo","start_date",var_startdate)
    write_config.set("JiraInfo","end_date",var_enddate)
    write_config.set("JiraInfo","site",var_site)

    write_config.add_section("ToolInfo")
    write_config.set("ToolInfo","encryption_tool_folder", os.path.abspath('../../Tools/encryption-tool_21.1.2.0/bin'))
    write_config.set("ToolInfo","encryption_tool_path",os.path.join(os.path.abspath('../../Tools/encryption-tool_21.1.2.0/bin'),'encryption-tool.bat'))
    
    write_config.add_section("AdrA51Transfer")
    write_config.set("AdrA51Transfer","decrypted_data_path",proc_adr_decryption_loc)
    write_config.set("AdrA51Transfer","encrypted_data_output_path",proc_adr_encryption_loc)    

    cfgfile = open("../../Config/"+var_jiraid+"_config.ini",'w')
    write_config.write(cfgfile)
    cfgfile.close()
    #global_doctype_list=var_doctype_list_for_config    
    jirarow = (var_jiraid,None,adr_jira_loc,None,var_site,var_startdate,var_enddate,datetime.now(),var_doctype_list_for_config,'ADR')

if val=='2':
    useriput_selectioncrtieria_filename = input("Enter the XML Selection Criteria filename: ")
    print(useriput_selectioncrtieria_filename,'\n\n')
    
    selection_criteria_xmlfile_loc = selectionxmlFile_location+"/"+useriput_selectioncrtieria_filename#'/ADS-1401_Amerihome_DSPMR.xml'#sys.argv[1]
    var_jiraid,var_startdate,var_enddate,var_doctype_list,var_site,var_server = read_seleciton_criteria_xml(selection_criteria_xmlfile_loc)
    ade_dwnld_jira_id_workspace_name = var_jiraid+"_"+var_startdate+"_"+var_enddate
    var_doctype_list_for_config = ','.join([docele for docele in var_doctype_list])

    db_inventory_site_wise_name = os.path.join(databank_location, 'Data_Bank_Inventory',var_site)    
    if not os.path.exists(db_inventory_site_wise_name):
        os.mkdir(db_inventory_site_wise_name)
    
    ade_dwnld_jira_loc = os.path.join(databank_location, 'Data_Bank_Inventory',var_site,ade_dwnld_jira_id_workspace_name)
    os.mkdir(ade_dwnld_jira_loc)
    proc_productiondownload_loc = os.path.join(ade_dwnld_jira_loc,'a_ProductionDownload')
    os.mkdir(proc_productiondownload_loc)
    proc_decryption_loc = os.path.join(ade_dwnld_jira_loc,'b_Decryption')
    os.mkdir(proc_decryption_loc)
    
    proc_Classification_loc = os.path.join(ade_dwnld_jira_loc,'c_Classification')
    os.mkdir(proc_Classification_loc)
    proc_Preprocessing_loc = os.path.join(ade_dwnld_jira_loc,'d_Preprocessing')
    os.mkdir(proc_Preprocessing_loc)
    #proc_Preprocessing_Conversion_loc = os.path.join(proc_Preprocessing_loc,'1_Conversion')
    #os.mkdir(proc_Preprocessing_Conversion_loc)
    #proc_Preprocessing_deskew_loc = os.path.join(proc_Preprocessing_loc,'2_Deskew')
    #os.mkdir(proc_Preprocessing_deskew_loc)
    #proc_Preprocessing_deduplicate_loc = os.path.join(proc_Preprocessing_loc,'3_De-Duplication')
    #os.mkdir(proc_Preprocessing_deduplicate_loc)


    write_config = configparser.ConfigParser()

    write_config.add_section("Decryption")
    write_config.set("Decryption","production_download_directory",proc_productiondownload_loc)
    write_config.set("Decryption","decryption_directory",proc_decryption_loc)
        
    write_config.add_section("JiraInfo")
    write_config.set("JiraInfo","jira_id",var_jiraid)
    write_config.set("JiraInfo","doctype",var_doctype_list_for_config)
    write_config.set("JiraInfo","start_date",var_startdate)
    write_config.set("JiraInfo","end_date",var_enddate)
    write_config.set("JiraInfo","site",var_site)

    write_config.add_section("ToolInfo")
    write_config.set("ToolInfo","preprocess_tool_path",os.path.abspath('../../Tools/Pre-Process/!Pre-Processing/toolDataConverter'))
    write_config.set("ToolInfo","preprocess_mainfolder_bat_path", os.path.join(os.path.abspath('../../Tools/Pre-Process/!Pre-Processing/toolDataConverter'),'run_stat.bat'))
    write_config.set("ToolInfo","preprocess_conversion_bat_path",os.path.join(os.path.abspath('../../Tools/Pre-Process/!Pre-Processing/toolDataConverter'),'run.bat'))
    write_config.set("ToolInfo","preprocess_deskew_tool_path",os.path.abspath('../../Tools/Pre-Process/!Pre-Processing/DeskewImage'))
    write_config.set("ToolInfo","preprocess_deskew_bat_path",os.path.join(os.path.abspath('../../Tools/Pre-Process/!Pre-Processing/DeskewImage'),'run.bat'))
    write_config.set("ToolInfo","all_tools_path",os.path.abspath('../../Tools'))

    write_config.add_section("PreClassification")
    write_config.set("PreClassification","input_loc",proc_decryption_loc)
    write_config.set("PreClassification","output_loc",proc_Classification_loc)

    write_config.add_section("PreProcess")
    write_config.set("PreProcess","preprocess_doctype","")
    write_config.set("PreProcess","pdf2png",'false')
    write_config.set("PreProcess","preprocess_loc",proc_Preprocessing_loc)
    write_config.set("PreProcess","preprocess_decryption_input_loc",proc_decryption_loc)
    #write_config.set("PreProcess","preprocess_conversion_loc",proc_Preprocessing_Conversion_loc)
    #write_config.set("PreProcess","preprocess_deskew_loc",proc_Preprocessing_deskew_loc)

    for doctypenames in var_doctype_list:
        decryption_doctypewise_path = os.path.join(proc_decryption_loc,doctypenames)
        os.mkdir(decryption_doctypewise_path)    

        preclassification_doctypewise_path = os.path.join(proc_Classification_loc,doctypenames)
        os.mkdir(preclassification_doctypewise_path)
        
        preprocessing_doctypewise_path = os.path.join(proc_Preprocessing_loc,doctypenames)
        os.mkdir(preprocessing_doctypewise_path)
        #write_config.set("Decryption",doctypenames+"_decryption_input_directory",doctype_path)

        proc_Preprocessing_Conversion_loc = os.path.join(preprocessing_doctypewise_path,'a_Conversion')
        os.mkdir(proc_Preprocessing_Conversion_loc)
        proc_Preprocessing_deskew_loc = os.path.join(preprocessing_doctypewise_path,'b_Deskew')
        os.mkdir(proc_Preprocessing_deskew_loc)
        proc_Preprocessing_deduplicate_loc = os.path.join(preprocessing_doctypewise_path,'c_De-Duplication')
        os.mkdir(proc_Preprocessing_deduplicate_loc)
   # decrypted_folder_path = databank_path+'/Data Bank Inventory/Amerihome/ADS_1111_01012022_01012022_01022022/1_Decryption'
    #conversion_folder_path = databank_path+'/Data Bank Inventory/Amerihome/ADS_1111_01012022_01012022_01022022/3_Pre-Processing/1_Conversion'
    #deskew_folder_path = databank_path+'/Data Bank Inventory/Amerihome/ADS_1111_01012022_01012022_01022022/3_Pre-Processing/2_Deskew'

    cfgfile = open("../../Config/"+var_jiraid+"_config.ini",'w')
    write_config.write(cfgfile)
    cfgfile.close()

    jirarow = (var_jiraid,None,ade_dwnld_jira_loc,None,var_site,var_startdate,var_enddate,datetime.now(),var_doctype_list_for_config,'Prod Download')
    
    # ----------------- MySQL - feed the doctype list into database ------------------------------#
    # FIrst load the list of newly arrived selection criteria file doc types into a stagging area of mysql db.
    cursor.execute("TRUNCATE TABLE databankdb.STG_DOCTYPE;")
    #global_doctype_list = ["paystub","urla"]
    sql= "INSERT INTO databankdb.STG_DOCTYPE(DOCTYPE_NAME) VALUES (%s);"

    for doctyp_name in var_doctype_list:
        print(doctyp_name)
        cursor.execute(sql, (doctyp_name,))
    # Second append incrementally into the target lkp doc type.
    sql_etl = "INSERT INTO databankdb.LKP_DOCTYPE(DOCTYPE_NAME) SELECT s.doctype_name FROM databankdb.STG_DOCTYPE s LEFT OUTER JOIN databankdb.LKP_DOCTYPE l on s.doctype_name  = l.doctype_name WHERE l.DOCTYPE_NAME is null;"

    cursor.execute(sql_etl)
    conn.commit()
    # disconnecting from server
    print(cursor.rowcount, " New Doctype encountered and added to the master list")

if val=='3':
    useriput_ade_aiq_upload_jira_id = input("Enter the ADE AIQ Upload Jira Ticket ID: ")
    useriput_ade_aiq_upload_document_type = input("Enter the Document Type: ")
    useriput_ade_aiq_upload_purpose = input("Enter the Purpose for AIQ Upload: ")
    useriput_upload_count = input("Enter the amount of  documents to be uploaded: ")
    print(useriput_ade_aiq_upload_jira_id)
    print(useriput_upload_count)
    print(useriput_ade_aiq_upload_purpose,'\n\n')

    var_jiraid = useriput_ade_aiq_upload_jira_id
    var_doctype = useriput_ade_aiq_upload_document_type
    ade_aiqupload_jira_id_workspace_name = useriput_ade_aiq_upload_jira_id+"_"+useriput_ade_aiq_upload_purpose+"_"+useriput_upload_count
    document_dir_wise_name = os.path.join(doc_name_location,var_doctype)
    
    if not os.path.exists(document_dir_wise_name):
        os.makedirs(document_dir_wise_name)

    
    #if  os.path.exists(document_dir_wise_name):
        #shutil.rmtree(document_dir_wise_name)
    #os.mkdir(document_dir_wise_name)
    
    
    
    ade_aiq_jira_loc = os.path.join(document_dir_wise_name,ade_aiqupload_jira_id_workspace_name)
    os.mkdir(ade_aiq_jira_loc)
    proc_preprocess_loc = os.path.join(ade_aiq_jira_loc,'a_PreProcessing')
    os.mkdir(proc_preprocess_loc)
    #proc_readydocforupload_loc = os.path.join(ade_aiq_jira_loc,'2_Document_tobe_Uploaded')
    #os.mkdir(proc_readydocforupload_loc)
    proc_prerenaming_loc = os.path.join(ade_aiq_jira_loc,'b_PreRenaming')
    os.mkdir(proc_prerenaming_loc)
    proc_docuploaded_loc = os.path.join(ade_aiq_jira_loc,'c_Documents_Uploaded')
    os.mkdir(proc_docuploaded_loc)
    proc_docdownloaded_loc = os.path.join(ade_aiq_jira_loc,'d_Documents_Downloaded')
    os.mkdir(proc_docdownloaded_loc)
    proc_invalidxml_loc = os.path.join(ade_aiq_jira_loc,'e_Invalidated_XML')
    proc_invalidxml_dump = os.path.join(proc_invalidxml_loc,'dump')
    os.mkdir(proc_invalidxml_loc)
    os.mkdir(proc_invalidxml_dump)
    proc_postclassification_loc = os.path.join(ade_aiq_jira_loc,'f_Classification')
    os.mkdir(proc_postclassification_loc)
    proc_postrenaming_loc = os.path.join(ade_aiq_jira_loc,'g_PostRenaming')
    os.mkdir(proc_postrenaming_loc)
    proc_postprocessing_loc = os.path.join(ade_aiq_jira_loc,'h_PostProcessing')
    os.mkdir(proc_postprocessing_loc)
    proc_transfera51_loc = os.path.join(ade_aiq_jira_loc,'i_TransferToArea51')
    os.mkdir(proc_transfera51_loc)
    ftp_tracker_loc = os.path.join(ade_aiq_jira_loc,'FTP_Tracker')
    os.mkdir(ftp_tracker_loc)
    source_listing_loc = os.path.join(ade_aiq_jira_loc,'Source_Listing')
    os.mkdir(source_listing_loc)

    write_config = configparser.ConfigParser()
    
    write_config.add_section("JiraInfo")
    write_config.set("JiraInfo","useriput_ade_aiq_upload_jira_id",useriput_ade_aiq_upload_jira_id)
    write_config.set("JiraInfo","useriput_upload_count",useriput_upload_count)
    write_config.set("JiraInfo","useriput_ade_aiq_upload_purpose",useriput_ade_aiq_upload_purpose)
    write_config.set("JiraInfo","useriput_ade_aiq_upload_document_type",useriput_ade_aiq_upload_document_type)
    
    
    write_config.add_section("PreRenaming")
    write_config.set("PreRenaming","prerenaming_prefix","Misc")
    write_config.set("PreRenaming","prerenaming_input_directory",proc_prerenaming_loc)
    write_config.set("PreRenaming","output_dir",proc_prerenaming_loc)
    write_config.set("PreRenaming","prerenaming_doctype",useriput_ade_aiq_upload_document_type)
    write_config.set("PreRenaming","preprocessed_doc_loc",proc_preprocess_loc)

    write_config.add_section("aiqupload")
    write_config.set("aiqupload","aiqupload_source",os.path.join(proc_prerenaming_loc,'zip'))
 
    write_config.add_section("InvalidatedXML")
    write_config.set("InvalidatedXML","invalidatedxml_input_directory",proc_docdownloaded_loc)
    write_config.set("InvalidatedXML","invalidatedxml_dump_dir",proc_invalidxml_dump)
    
    write_config.add_section("PostClassification")
    write_config.set("PostClassification","input_loc",proc_docdownloaded_loc)
    write_config.set("PostClassification","output_loc",proc_postclassification_loc)    
    
    write_config.add_section("PostRenaming")
    write_config.set("PostRenaming","postrenaming_prefix","Misc")
    write_config.set("PostRenaming","postrenaming_input_directory",proc_docdownloaded_loc)
    write_config.set("PostRenaming","postrenaming_output_dir",proc_postrenaming_loc)
    write_config.set("PostRenaming","postrenaming_doctype",useriput_ade_aiq_upload_document_type)
    write_config.set("PostRenaming","postrenaming_ftptracker",os.path.join(ftp_tracker_loc,'FTP_Report_'+useriput_ade_aiq_upload_jira_id+'.csv'))
    write_config.set("PostRenaming","postrenaming_sourcelisting",os.path.join(source_listing_loc,'Source_'+useriput_ade_aiq_upload_jira_id+'.txt'))
    

    write_config.add_section("PostProcessing")
    write_config.set("PostProcessing","postprocessing_tool_path", os.path.abspath('../../Tools/post_processing'))
    write_config.set("PostProcessing","postprocessing_run_stat", os.path.join(os.path.abspath('../../Tools/post_processing'),'run_stat.bat'))
    write_config.set("PostProcessing","postprocessing_run_postprocessing", os.path.join(os.path.abspath('../../Tools/post_processing'),'run_postprocessing.bat'))
    write_config.set("PostProcessing","postprocessing_doctype",useriput_ade_aiq_upload_document_type)
    write_config.set("PostProcessing","input_loc",proc_postprocessing_loc)
    write_config.set("PostProcessing","output_loc",proc_transfera51_loc)
    
    
    
    cfgfile = open("../../Config/"+var_jiraid+"_config.ini",'w')
    write_config.write(cfgfile)
    cfgfile.close()    
#jirarow = (var_jiraid,None,ade_aiq_jira_loc,None,var_site,None,None,datetime.now(),None,'ADE')
    jirarow = (var_jiraid,None,ade_aiq_jira_loc,useriput_ade_aiq_upload_purpose,None,None,None,datetime.now(),useriput_ade_aiq_upload_document_type,'ADE')
'''
#Jira ID :  ADS-1454
#Start Date :  2021-06-28
#End Date :  2021-07-06
#Document Type :  Tax Returns
#Site :  Amerihome
#Server :  DSPMR

#OPTION

#1. Enter '1' to select ADR pipeline
#2  Enter '2' to select ADE download pipeline
#3  Enter '3' to select ADE AIQ upload pipeline
'''

#-----------------------------------------MYSQL----------------------------------------------
# Insert the newly arrived jira request in to master jira details table in mysql db

sql = "INSERT INTO databankdb.MST_TBL_JIRA_DETAILS VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
cursor.execute(sql, jirarow)
print(cursor.rowcount, "Jira Request Entry Insered")
conn.commit()

conn.close()

# Create the config file using Jira ID
# populate all paths 

print("Successfully Completed at : ",datetime.now())



# ------ END ----------------------------------------------------------------------------------------------



