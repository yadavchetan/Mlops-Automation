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


# IMPORT PACKAGES

import json            # for handling json data
import pandas as pd    # for handling data
import os              # for handling folder creation, folder traversing
import sys
import numpy
import shutil          # for file copy
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
    

pd.set_option("display.max_columns",None)

# assign directory
report_directory = sys.argv[1]#'E:/Stress_Testing/Post_Renaming/1st_Lot_1K'
output_dir = sys.argv[2]#"E:/Stress_Testing/Post_Renaming/1st_Lot_1K/output"
doc_type = sys.argv[3]#'Urla' #Tax Returns, Loan Estimate, IRS W_2
prefix = sys.argv[4]#'URLA' #8825, 1040DT, PrimDT, AddBorrowerDT, SchEDT, schCDT, DT, IRSW2DT, 1065DT, MiscDT, TaxDT
#directory = 'F:/ngade-new/Omkar/Additional_Borrower_Before_Upload - Copy'
ftp_tracker_path = sys.argv[5]#"E:/Stress_Testing/FTP/FTP_Report.csv"
table1_source_dir_path = sys.argv[6]#"E:/Stress_Testing/FTP/combine_list.txt"
#ftp_tracker_path = "F:/ngade-new/FTP_Trackers_Parsed/ngadeadditionalborrower-hourly.csv"
#renaming_mapping_sheet= "E:/Post_Re-Name_Test/2021-10-14/mapping_renaming.csv"

nonxmlfile_dt_dump_path = os.path.join(output_dir,'nonxmldt_dump')
if os.path.isdir(nonxmlfile_dt_dump_path):
    shutil.rmtree(nonxmlfile_dt_dump_path,ignore_errors=True)
os.mkdir(nonxmlfile_dt_dump_path)

#"sprint("Source Directory Before Uploading: ",directory)
print("Target Directory for renaming After Downloading: ",report_directory)
print("FTP tracker sheet After Downloading is referred from : ",ftp_tracker_path)
print("DT with non XML files will be dumped to  : ",nonxmlfile_dt_dump_path)

#--------------------------------------------------------------------------------------------------------------------------
#--> TABLE 1 For GT30
table1 = pd.read_csv(table1_source_dir_path)
table1.columns = ["sourcelisting"]

#table1['FileName'] =table1["sourcelisting"].str.split("\\").str[-1]
table1['FileName'] = table1["sourcelisting"].str.extract(r'(?!.*\\)(.*?).pdf.*')+".pdf"
table1=table1[['FileName']]
table1 = table1.dropna()
table1.to_csv(output_dir+'/table1_stg.csv')

table1['Original_folder'] = "FX"+table1["FileName"].str.extract(r'.*?\_FX(.*?)_')
table1['Original_MailItemID'] = "MI"+table1["FileName"].str.extract(r'.*?\_MI(.*?)_')
table1['Original_Document'] = "DT"+table1["FileName"].str.extract(r'DT(.*?)_.*')


table1['original_json_page_number'] =table1["FileName"].str.split("_").str[-2]
table1['Original_CO_page_number'] = table1["FileName"].str.extract(r'.*?\_C(?:0|1|2|3|4)-(.*?).pdf.*')

table1['Original_C0_format'] = table1["FileName"].str.split("_").str[-1].str.split('.').str[0].str.split('-').str[0]
table1['src_page_rank'] = table1.groupby(by=['Original_folder','Original_Document','Original_MailItemID'])['original_json_page_number'].transform(lambda x: x.rank(method='dense'))
table1['src_page_rank']=table1['src_page_rank'].apply(int)

print("table1 -",table1.shape)
table1.to_csv(output_dir+'/table1.csv')
#--------------------------------------------------------------------------------------------------------------------------
# TABLE 2 : AFTER DOWNLOAD
result_list_downloaded = []
# Prepare a column header list. Remember, if any field is getting added in the table, its column name should also get added here.
column_tuple = ('FileName','New_folder','New_Document','New_MailItemID','New_json_page_number','New_actual_page_number','File_extension','pageno_wise_rank')
result_list_downloaded.append(column_tuple)

for root, dirs, files in os.walk(report_directory):
    for filename in files:
        if filename.endswith('.zip'):
            continue
        if filename.endswith('.txt'):
            continue
        if filename.endswith('.json'):
            os.remove(os.path.join(root,filename))
            continue
        if filename.endswith('.csv'):
            continue  
        if filename.endswith('.db'):
            #print(os.path.join(root,filename))
            os.remove(os.path.join(root,filename))
            continue             
        #D:\ADE\FX\DT\\page*.json
#         print(root)
        x = root.split("/")
        #x=["ade","fx","dt"]
        x = x[len(x)-1]
        x = x.split('\\')
        folder = x[len(x)-2]
        doc = x[len(x)-1]
        #FX210911709_DT21099580-Tax Returns_Tax Returns-MI210912876_000005_C0-000003.pdf
        #URLA-MI210812512_000001_C0-000001.pdf
        pat = r'.*?\-MI(.*?)_.*'             #See Note at the bottom of the answer
        dt_pat = r'DT(.*?)-.*'
        jsonno_pat = r'.*?\_\d(.*?)_.*'
        s = str(filename)
        d = str(doc)
        #print('**** ',root,'----',d)
        if os.path.isdir(root):
            if not any(fname.endswith('.xml') for fname in os.listdir(root)):
                print(root,"  --- does not have xml file")
                shutil.move(root,os.path.join(nonxmlfile_dt_dump_path,d),copy_function=shutil.copytree)
        else:
            continue
        #print(s)
        MIItemID_match = "MI"+re.search(pat, s).group(1)
        FX_match = "FX"+re.search(pat, s).group(1)
        DT_match = "DT"+re.search(dt_pat, d).group(1)
        JsonNo_match = re.search(jsonno_pat, s).group(1)
        pageno_section = filename.split('_')
        tokens=len(pageno_section)
        actual_page_number = pageno_section[tokens-1].split('.')[0].split('-')[1]
        pageno_wise_rank = int(JsonNo_match.lstrip('0'))
        file_extension = pageno_section[tokens-1].split('.')[1]
#         print(actual_page_number)
#         if match:
#             print("MI"+match.group(1))

        #This tuple stores the before uploading metadata
        path_tuple = (filename,folder,DT_match,MIItemID_match,JsonNo_match,actual_page_number,file_extension,pageno_wise_rank)
        #print("downloaded:",path_tuple)
        
        result_list_downloaded.append(path_tuple)

# Create final report Dataframe
table2 = pd.DataFrame(result_list_downloaded[1:len(result_list_downloaded)],columns =list(result_list_downloaded[0]))

table2['downloaded_page_rank'] = table2.groupby(by=['New_folder','New_MailItemID'])['New_json_page_number'].transform(lambda x: x.rank(method='dense'))
table2['duplicate_download_rank'] = table2.groupby(by=['New_folder'])['New_MailItemID'].transform(lambda x: x.rank(method='dense'))

table2 = table2[table2['duplicate_download_rank']==1]
#table2["group_rank"] = table2.groupby(['New_folder','New_MailItemID'])['New_actual_page_number'].rank(ascending=1,method='dense')
#table2["page_rank"] = table2[["New_folder","New_MailItemID","New_actual_page_number"]].apply(tuple,axis=1).rank(method='min',ascending=True)
#table2["page_rank"] = table2.groupby(["New_folder","New_MailItemID"])['New_actual_page_number'].rank(method='dense',ascending=True)
table2.to_csv(output_dir+"/after_download_report.csv")
#print(table2.columns)
print("table2 -",table2.shape)
#--------------------------------------------------------------------------------------------------------------------------

#TABLE 3 : FTP
tracker = pd.read_csv(ftp_tracker_path,low_memory=False)
print("tracker -",tracker.shape)
#tracker = tracker[tracker['Processing Status']=="SUCCESS"]

#tracker.drop_duplicates()
#--------------------------------------------------------------------------------------------------------------------------

tracker['Filename'] = tracker["File Name"]
#MiscDT20021109606_FX20027457_Tax_Returns_MI200244542_000279_C0-000003.pdf

table3 = pd.DataFrame()
#--------------------------------------------------------------------------------------------------------------------------
#table3 = tracker[['Filename','Folder Identifier','Mailitem ID','Context Id']]
#table3.columns = ["FileName","Original_Document","Mailitem ID","New_Folder"]

table3 = tracker[['Filename','Mailitem ID','Context Id']]
table3.columns = ["FileName","Mailitem ID","New_Folder"]
print("Flag1: ftp_tracker_reduced_columns -",table3.shape)
table3 = (table3.set_index(table3.columns.drop('FileName',1).tolist())['FileName'].str.split(',', expand=True).stack()
   .reset_index()
   .rename(columns={0:'FileName'})
   .loc[:, table3.columns]
)
print("Flag2: ",table3.shape)
table3 = (table3.set_index(table3.columns.drop('FileName',1).tolist())['FileName']
   .str.split('\\', expand=True)
   .stack()
   .reset_index()
   .rename(columns={0:'FileName'})
   .loc[:, table3.columns]
)
print("Flag3: Expand filenames - denormalise -",table3.shape)
table3 = table3[~table3['FileName'].str.contains('.zip')]
table3 = table3[~table3['FileName'].str.contains('.db')]
#table3 = table3[~table3['FileName'].str.contains('TaxReturn')]
table3 = table3[table3['FileName'].str.contains(prefix)] #8825, 1040DT, PrimDT, AddBorrowerDT, SchEDT, schCDT, DT, IRSW2DT, 1065DT, MiscDT, TaxDT


print("Flag4: Filter out FTP based on prefix -",table3.shape)
table3['Original_folder'] = "FX"+table3["FileName"].str.extract(r'.*?\_FX(.*?)_')
table3['Original_MailItemID'] = "MI"+table3["FileName"].str.extract(r'.*?\_MI(.*?)_')
table3['Original_Document'] = "DT"+table3["FileName"].str.extract(r'DT(.*?)_.*')
#print(tablleName"].values)
#table3['actual_page_number'] = table3["FileName"].str.split('_')[2].split('.')[0].split('-')[1]
#table3['original_json_page_number'] = table3["FileName"].str.extract(r'.*?\_\d(.*?)_.*')
table3['original_json_page_number'] =table3["FileName"].str.split("_").str[-2]
table3['Original_CO_page_number'] = table3["FileName"].str.extract(r'.*?\_C(?:0|1|2|3|4)-(.*?).pdf.*')
table3['Original_C0_format'] = table3["FileName"].str.split("_").str[-1].str.split('.').str[0].str.split('-').str[0]
print("flag5 -",table3.shape)
#original_json_page_number
#AddBorrowerDT21072080035_FX21043998_URLA_MI21071230059_000024_C0-000009.pdf
#.*?\_\d(.*?)_.*
#Original_CO_page_number
#.*?\_CO-(.*?).pdf.*

#print("2 - Parsed the FTP tracker sheet which has mapping between the original names and after download changed names")
#--------------------------------------------------------------------------------------------------------------------------
#result_df = table1.merge(table3,left_on=['Original_folder','Original_MailItemID'],right_on=['Original_folder','Original_MailItemID'])

table3['ftp_page_rank'] = table3.groupby(by=['Original_folder','Original_MailItemID','New_Folder'])['original_json_page_number'].transform(lambda x: x.rank(method='dense'))
table3['ftp_page_rank']=table3['ftp_page_rank'].apply(int)

print("flag6 -",table3.shape)
table3.to_csv(output_dir+"/ftp_tracking_report.csv")

#table3.to_csv(output_dir+"/table3.csv")
print("table3 -",table3.shape)

#----> GT30 Logic. Append the information of pages greater than 30 in the existing ftp report to make it compliant with after download listing.
rename_lookup_table = table3[['Original_folder','Original_MailItemID','Original_Document','New_Folder','Mailitem ID']].drop_duplicates()
rename_lookup_table.drop_duplicates(keep='first',inplace=True)
rename_lookup_table.to_csv(output_dir+'/old_new_lookup.csv')


table3_report = table3[['FileName','Mailitem ID','New_Folder','Original_folder','Original_MailItemID','Original_Document','original_json_page_number','Original_CO_page_number','Original_C0_format','ftp_page_rank']].drop_duplicates()
table3_new_ftp_tracker = table1.merge(table3_report,right_on=['FileName','ftp_page_rank'],left_on=['FileName','src_page_rank'],how='left')
table3_new_ftp_tracker.to_csv(output_dir+'/table3_new_ftp_tracker.csv')


table3_new_ftp_tracker_filled = table3_new_ftp_tracker.merge(rename_lookup_table,right_on=['Original_folder','Original_MailItemID','Original_Document'],left_on=['Original_folder_x','Original_MailItemID_x','Original_Document_x'],how='inner')
table3_new_ftp_tracker_filled.to_csv(output_dir+'/table3_new_ftp_tracker_filled.csv')
table3_with_page_gt30 = table3_new_ftp_tracker_filled[['FileName','Mailitem ID_y','New_Folder_y','Original_folder_x', 'Original_MailItemID_x','Original_Document_x','original_json_page_number_x','Original_CO_page_number_x', 'Original_C0_format_x','src_page_rank']]

result_df_final = table3_with_page_gt30.merge(table2,right_on=['New_folder','New_MailItemID','pageno_wise_rank'],left_on=['New_Folder_y','Mailitem ID_y','src_page_rank'])
result_df_final.to_csv(output_dir+'/result_df_final_stg.csv')

#--------------------------------------------------------------------------------------------------------------------------
print(doc_type+"-"+result_df_final['Original_MailItemID_x'].astype(str)+"_"+result_df_final['original_json_page_number_x'].astype(str)+"_"+result_df_final['Original_C0_format_x'].astype(str)+"-"+result_df_final['Original_CO_page_number_x'].astype(str)+"."+result_df_final['File_extension'].astype(str))
result_df_final['Renamed_FileName'] = doc_type+"-"+result_df_final['Original_MailItemID_x'].astype(str)+"_"+result_df_final['original_json_page_number_x'].astype(str)+"_"+result_df_final['Original_C0_format_x'].astype(str)+"-"+result_df_final['Original_CO_page_number_x'].astype(str)+"."+result_df_final['File_extension'].astype(str)

#Original_folder_x    Original_MailItemID_x    Original_Document_x    original_json_page_number_x    Original_CO_page_number_x    Original_C0_format_x

#FX210911709_DT21099580-Tax Returns_Tax Returns-MI210912876_000005_C0-000003.pdf
#result_df_final['Renamed_FileName'] = result_df_final['Original_folder']+"_"+result_df_final['Original_Document']+"-"+doc_type+"_"+doc_type+"-"+result_df_final['Original_MailItemID']+"_0"+result_df_final['original_json_page_number']+"_C0-"+result_df_final['Original_CO_page_number']+"."+result_df_final['File_extension']

#result_df_final['Renamed_DT'] = result_df_final['Original_Document']+"_"+"-"+doc_type
result_df_final['Renamed_DT'] = result_df_final['Original_Document_x']+"-"+doc_type
result_df_final['Downloaded_DT'] = result_df_final['New_Document']+"-"+doc_type

#print(result_df_final.columns)
print("table_res_2",result_df_final.shape)

print("5 - Final File-folders renaming mapping sheet is successfully constructed")
#--------------------------------------------------------------------------------------------------------------------------
result_df_final.to_csv(output_dir+"/renaming_mapping_sheet.csv")
#--------------------------------------------------------------------------------------------------------------------------

print("\n6 - Actual Renaming of pages, folders and documnent is Started at : ",datetime.now())

rename_page_mapping_df = result_df_final[['FileName_y','Renamed_FileName']].drop_duplicates()
#print(rename_page_mapping_df.values)
rename_page_dictionary = rename_page_mapping_df.set_index('FileName_y')['Renamed_FileName'].to_dict()
#rename_page_dictionary = dict(zip(result_df_final.FileName, result_df_final.Renamed_FileName))
#print(rename_page_dictionary)
rename_fx_dictionary = dict(zip(result_df_final.New_folder, result_df_final.Original_folder_x))
rename_dt_dictionary = dict(zip(result_df_final.Downloaded_DT, result_df_final.Renamed_DT))

count=0
dt_count=0
fx_count=0
for root, dirs, files in os.walk(report_directory):
#         print(root)
        for filename in files:
#             print(root)
            if [os.path.join(root, str(value)) for key, value in rename_page_dictionary.items() if filename in key]:
                if count%1000==0:
                    print("         -",count, ' pages renamed at ',datetime.now())
                original_file_path = os.path.join(root, filename)
                renamed_filename =  [str(value) for key, value in rename_page_dictionary.items() if filename in key][0]
                renamed_file_path = [os.path.join(root, str(value)) for key, value in rename_page_dictionary.items() if filename in key][0]
                count=count+1
                print(original_file_path," :: ", renamed_file_path)
            else:
                continue
            #if  os.path.exists(renamed_file_path):
            #    os.remove(renamed_file_path)
            renamed_status = "not renamed"
            if  os.path.exists(original_file_path):
                os.rename(original_file_path, renamed_file_path)
                print(original_file_path," --> ",renamed_file_path, "  :: renamed")
                renamed_status = "renamed"
            
    #-----Write to MongoDB ----------------------------------------------------------------------            

            #MiscDT200281351_FX200238_Tax_returns_MI200281_000418_C0-000002
            MIItemID_match_mongodb = 'MI'+re.search(r'MI(.*?)_', renamed_filename).group(1)
            JsonNo_match_mongodb = re.search(r'.*?\_\d(.*?)_.*', renamed_filename).group(1)
            pageno_section_mongodb = renamed_filename.split('_')
            tokens=len(pageno_section_mongodb)
            actual_page_number_mongodb = pageno_section_mongodb[tokens-1].split('.')[0].split('-')[1]
            file_extension_mongodb = pageno_section_mongodb[tokens-1].split('.')[1]

            "MI200281_000418_C0-000002"
            mongo_pageid = MIItemID_match_mongodb+'_'+JsonNo_match_mongodb+'_C0-'+actual_page_number_mongodb
            print(mongo_pageid)
            
            if use_mongo_db=='1':
                mycol.update_one({"PAGEID":mongo_pageid},{"$set":{"postrenaming_flag":"1","fileloc":root,"postaiq_changed_filename":filename,"renamed_to_original_filename":renamed_file_path,"renamed_status":renamed_status}})
    #-----------------------------------------------------------------------------------------  
                
print("         -",count, ' pages renamed at ',datetime.now())
print("    6.1 - All pages renamed : ",datetime.now())
        
for root, dirs, files in os.walk(report_directory):
#         print(root)        
        for dt in dirs:
            #print("--------------",root," -- ",dt)
            #print(rename_dt_dictionary)
            if [os.path.join(root, value) for key, value in rename_dt_dictionary.items() if dt in key]:
                if dt_count%100==0:
                    print("         -",dt_count, ' documents renamed at ',datetime.now())
                original_dt_path = os.path.join(root, dt)
                renamed_dt_path = [os.path.join(root, value) for key, value in rename_dt_dictionary.items() if dt in key][0]
                dt_count=dt_count+1
            else:
                continue
            #if  os.path.exists(renamed_dt_path):
            #    shutil.rmtree(renamed_dt_path)
            #if  os.path.exists(original_dt_path):
            #    os.rename(original_dt_path, renamed_dt_path)
                
            if  os.path.exists(original_dt_path):
                if  os.path.exists(renamed_dt_path):
                    shutil.copytree(original_dt_path, renamed_dt_path,dirs_exist_ok=True)#,dirs_exist_ok=True E:/Post_Re-Name_Test/2021-10-11/Post_Bucketing/1040 Schedule D\\FX2108154\\DT2108387-Tax Returns'
                    #shutil.move(original_dt_path,renamed_dt_path,copy_function=shutil.copytree)                    
                    print(original_dt_path," --> ",renamed_dt_path, "  :: copied")
                    shutil.rmtree(original_dt_path)
                else:
                    os.rename(original_dt_path, renamed_dt_path)
                    print(original_dt_path," --> ",renamed_dt_path, "  :: renamed")
                    
print("         -",dt_count, ' documents renamed at ',datetime.now())
print("    6.2 - All document folders renamed : ",datetime.now())

for root, dirs, files in os.walk(report_directory):
#         print(root)        
        for fx in dirs:
#             print(root)
#             print(dt)
            if [os.path.join(root, value) for key, value in rename_fx_dictionary.items() if fx in key]:
                if fx_count%100==0:
                    print("         -",fx_count, ' folders renamed at ',datetime.now())
                original_fx_path = os.path.join(root, fx)
                renamed_fx_path = [os.path.join(root, value) for key, value in rename_fx_dictionary.items() if fx in key][0]
                fx_count=fx_count+1
            else:
                continue
            #if  os.path.exists(renamed_fx_path):
            #    shutil.rmtree(renamed_fx_path)               
#renaming from f:/fx1234 --> f:/fx56788

            if  os.path.exists(original_fx_path):
                if  os.path.exists(renamed_fx_path):
                    shutil.copytree(original_fx_path, renamed_fx_path,dirs_exist_ok=True)
                    print(original_fx_path," --> ",renamed_fx_path, "  :: copied")
                    shutil.rmtree(original_fx_path)
                else:
                    os.rename(original_fx_path, renamed_fx_path)
                    print(original_fx_path," --> ",renamed_fx_path, "  :: renamed")
print("         -",fx_count, ' folders renamed at ',datetime.now())
print("    6.3 - All Loan folders renamed : ",datetime.now())        
#--------------------------------------------------------------------------------------------------------------------------
#2022-07-14


print("\n7 - Renaming of pages, folders and documnent is completed successfully at : ",datetime.now())

#table1["page_rank"] = table1.sort_values(['original_actual_page_number'], ascending=[True]).groupby(['Original_folder','Original_MailItemID']).cumcount() + 1
#table2["page_rank"] = table2.sort_values(['New_actual_page_number'], ascending=[True]).groupby(['New_folder','New_MailItemID']).cumcount() + 1
#result_df["page_rank"] = result_df.sort_values(["original_actual_page_number"], ascending=[True]).groupby(["Original_folder","Original_MailItemID","Mailitem ID","New_Folder"]).cumcount() + 1
#df['Rank'] = df.groupby(by=['C1'])['C2'].transform(lambda x: x.rank())

