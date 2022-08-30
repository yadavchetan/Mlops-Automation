#!/usr/bin/env python
# coding: utf-8

# In[91]:


import os #work as OS functionality
import pandas as pd #Work on data
import numpy 
import csv # Implement CSV operations
import json #Implement JSON files
import pymongo as pm # Connection for Mongo DB
import sys #System functionality implement 
import shutil
import re
from write_logs import writetolog
import configparser
from datetime import datetime
import time
import pymongo # need to import module Mongo DB
from pymongo import MongoClient

read_config = configparser.ConfigParser()
read_config.read('../../Config/central_config.ini')
print('../../Config/central_config.ini')

#---- MySQL COnnection -------------------------------------------------------

databank_location = read_config.get("databank", "databank_location")
host_name = read_config.get("mysql_database","hostname")
#Port_No = read_config.get("mysql_database","port")
db_name = read_config.get("mysql_database","dbstring")
USER_NAME = read_config.get("mysql_database","USERNAME")
PASSWORD = read_config.get("mysql_database","PASSWORD")

#---- MongoDB COnnection -------------------------------------------------------
import configparser

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

#location of Directory, Folders, Type Of Files.

process_list = ["1_Decryption","2_Classification","3_Preprocessing","1_PreProcessing","2_PreRenaming","3_Documents_Uploaded","4_Documents_Downloaded","5_Invalidated_XML","6_Classification","7_PostRenaming","8_PostProcessing","9_TransferToArea51"]


doctype_list = ["Profit_&_Loss","Tax_Return","URLA","Note"]
pipeline_list = ["ADE", "ADR", "Data_Bank_Inventory"]
client_list = ["Amerihome", "Homepoint","Loancare"]



'''
#Make listing of path
path= databank_location
l=os.listdir(path)
#print(l)
# Actual_list=('Pipeline','Client_Name','Reference_Key','','','')
result_list=[]
snapshot_list=[]
# Here to find the process of DFS (Defth First Search Graph)
download_flag=0
prerenaming_flag=0
preclassification_flag=0
postrenaming_flag=0
postclassification_flag=0
invalidatedxml_flag=0
preprocessing_flag=0
postprocessing_flag=0
uploadtoaiq_flag=0
adr_ar51_transfer_flag = 0

for root, dirs, files in os.walk(path): #Search the path in detal
    for filename in files:
        dtset=set()
        fxset=set()
        jiraidset=set()
        doctype=''
        process=''
        pipeline=''
        client=''
        if filename.endswith('.zip'):
            continue
        if filename.endswith('.txt'):
            continue
        if filename.endswith('.json'):
            continue
        if filename.endswith('.csv'):
            continue  
        if filename.endswith('.db'):
            #print(os.path.join(root,filename))
            os.remove(os.path.join(root,filename))
            continue            
        op = os.path.join(root,filename)
        #print(op)
        data = op.split('\\')
        data.pop(0)
        #print(data)
        client=None
        if data[1] in pipeline_list:
            for n in data:
                #print('#### 1 # :',n)
                if re.search(r'FX([0-9]+)', n):
                    FX_match = re.search(r'FX([0-9]+)', n).group(1)
                    #print('#### 2 # :',FX_match)
                    fxset.add(FX_match)
    #                 print(dtset)

                if re.search(r'DT([0-9]+)', n):
                    DT_match = re.search(r'DT([0-9]+)', n).group(1)
                    #print('#### 2 # :',DT_match)
                    dtset.add(DT_match)
    #                 print(dtset)
                if re.search(r'ADS[-_]?[0-9]+', n):
                    Jira_match = re.search(r'ADS[-_]?[0-9]+', n).group(0)
                   # print('#### 2 # :',Jira_match)
                    jiraidset.add(Jira_match)
#                     print(jiraidset)
                if n in doctype_list:
                    doctype = n
                    #print('#### 2 # :',doctype)
                if n in process_list:
                    process = n
                    #print('#### 2 # :',process)
                    
                    download_flag="0"
                    prerenaming_flag="0"
                    preclassification_flag="0"
                    postrenaming_flag="0"
                    postclassification_flag="0"
                    invalidatedxml_flag="0"
                    preprocessing_flag="0"
                    postprocessing_flag="0"
                    uploadtoaiq_flag="0"
                    adr_ar51_transfer_flag = "0"

                    #print('-----------------',process)
                    if process == '1_Decryption':
                        download_flag="1"
                        flag_json={"download_flag":download_flag}
                    elif process == '2_Classification':
                        preclassification_flag="1"
                        flag_json={"preclassification_flag":preclassification_flag}                        
                    elif process == '2_PreRenaming':
                        prerenaming_flag="1"
                        flag_json={"prerenaming_flag":prerenaming_flag}
                    elif process == '3_Preprocessing':
                        preprocessing_flag="1"
                        flag_json={"preprocessing_flag":preprocessing_flag}                        
                    elif process == '3_Documents_Uploaded':
                        uploadtoaiq_flag="1"
                        flag_json={"uploadtoaiq_flag":uploadtoaiq_flag}                        
                    elif process == '5_Invalidated_XML':
                        invalidatedxml_flag="1"
                        flag_json={"invalidatedxml_flag":invalidatedxml_flag}                       
                    elif process == '6_Classification':
                        postclassification_flag="1"
                        flag_json={"postclassification_flag":postclassification_flag}                        
                    elif process == '7_PostRenaming':
                        postrenaming_flag="1"
                        flag_json={"postrenaming_flag":postrenaming_flag}
                    elif process == '8_PostProcessing':
                        postprocessing_flag="1"
                        flag_json={"postprocessing_flag":postprocessing_flag}                        
                    elif process == '9_TransferToArea51':
                        adr_ar51_transfer_flag="1"
                        flag_json={"adr_ar51_transfer_flag":adr_ar51_transfer_flag}    					
                if n in pipeline_list:
                    pipeline = n
                   # print('#### 2 # :',pipeline)
                if n in client_list:
                    client = n
                    #print('#### 2 # :',client)
                    
           
        PAGEID = 'MI'+re.search(r'MI(.*)', filename).group(1)
        FXID = 'FX'+[fx for fx in fxset][0]        
        DTID = 'DT'+[dt for dt in dtset][0]
        JIRAID = [jira for jira in jiraidset][0]
        MIItemID_match_mongodb = 'MI'+re.search(r'MI(.*?)_', filename).group(1)
        JsonNo_match_mongodb = re.search(r'.*?\_\d(.*?)_.*', filename).group(1)
        pageno_section_mongodb = filename.split('_')
        tokens=len(pageno_section_mongodb)
        actual_page_number_mongodb = pageno_section_mongodb[tokens-1].split('.')[0].split('-')[1]
        file_extension_mongodb = pageno_section_mongodb[tokens-1].split('.')[1]
'''
        table1 = pd.read_csv('E:\MLOPSDataBank\Listing\DL_ADS-728_Decrypted_data.txt')
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
        table1['mongo_pageid'] = table1['Original_MailItemID']+"_"+table1['original_json_page_number']+"_C0-"+table1['Original_CO_page_number']
        "MI200281_000418_C0-000002"
        #mongo_pageid = MIItemID_match_mongodb+'_'+JsonNo_match_mongodb+'_C0-'+actual_page_number_mongodb
        #print(mongo_pageid)
        
        #file_tuple = (pipeline,client,JIRAID,process,None,doctype,FXID,DTID,PAGEID)
        file_tuple = (pipeline,client,JIRAID,process,mongo_pageid,FXID,DTID,MIItemID_match_mongodb,doctype,JsonNo_match_mongodb,actual_page_number_mongodb,file_extension_mongodb,download_flag,prerenaming_flag,preclassification_flag,postrenaming_flag,postclassification_flag,invalidatedxml_flag,preprocessing_flag,postprocessing_flag,uploadtoaiq_flag,adr_ar51_transfer_flag)
        #print(file_tuple)
        snapshot_list.append(file_tuple)
    
    
    
        rec = {
        "PAGEID":mongo_pageid,
        "FXID":FXID,
        "DTID":DTID,
        "MIID":MIItemID_match_mongodb,
        "DOCTYPE":doctype,
        "JSONNUMBER":JsonNo_match_mongodb,
        "PAGENUMBER":actual_page_number_mongodb,
        "EXTENSION":file_extension_mongodb,
        "download_flag":download_flag,
        "prerenaming_flag":prerenaming_flag,
        "preclassification_flag":preclassification_flag,
        "postrenaming_flag":postrenaming_flag,
        "postclassification_flag":postclassification_flag,
        "invalidatedxml_flag":invalidatedxml_flag,
        "preprocessing_flag":preprocessing_flag,
        "postprocessing_flag":postprocessing_flag,
        "uploadtoaiq_flag":uploadtoaiq_flag,
        "adr_ar51_transfer_flag":adr_ar51_transfer_flag,
        "document_process_mode":"Historical"
        }
        #print(rec)
        if use_mongo_db=='1':
            if filename.endswith('.pdf'):
                abc = mycol.find({'PAGEID':mongo_pageid})
                if len(list(abc))>0:
                    #print(process,' = ',filename)
                    #print("update")
                    #print(mongo_pageid)
                    mycol.update_one({"PAGEID":mongo_pageid},{"$set":flag_json})
                else:
                    mycol.insert_one(rec)
                #print("insert")


#df = pd.DataFrame(snapshot_list,columns=['PIPLELINE','CLIENT_NAME','REFERENCE_KEY','PROCESS','SUB_PROCESS','DOCTYPE','LOAN_ID','DOCUMENT_ID','PAGE_ID'])
df = pd.DataFrame(snapshot_list,columns=['PIPLELINE','CLIENT_NAME','REFERENCE_KEY','PROCESS','PAGEID','FXID','DTID','MIID','DOCTYPE','JSONNUMBER','PAGENUMBER','EXTENSION','download_flag','prerenaming_flag','preclassification_flag','postrenaming_flag','postclassification_flag','invalidatedxml_flag','preprocessing_flag','postprocessing_flag','uploadtoaiq_flag','adr_ar51_transfer_flag'])
        
df.to_csv("E:/snapshot.csv",mode='a',index=False,header=True)




