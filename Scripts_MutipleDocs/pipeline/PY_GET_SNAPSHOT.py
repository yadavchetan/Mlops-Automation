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
import mysql.connector

read_config = configparser.ConfigParser()
read_config.read('../../Config/central_config.ini')
print('../../Config/central_config.ini')

databank_location = "E:\MLOPSDataBank_Snapshot" ##read_config.get("databank", "databank_location")
#databank_location = "\\qumsfs.sfsecure.net\ML_Ops_Data_FS\MLOPSDataBank"
host_name = read_config.get("mysql_database","hostname")
#Port_No = read_config.get("mysql_database","port")
db_name = read_config.get("mysql_database","dbstring")
USER_NAME = read_config.get("mysql_database","USERNAME")
PASSWORD = read_config.get("mysql_database","PASSWORD")

#location of Directory, Folders, Type Of Files.
conn = mysql.connector.connect(host=host_name, database=db_name, user=USER_NAME, password=PASSWORD) 
if conn.is_connected():
    cursor = conn.cursor()
    all_doctype_list=[]
    cursor.execute("SELECT DOCTYPE_NAME FROM LKP_DOCTYPE")

    myresult = cursor.fetchall()
    for x in myresult:
        all_doctype_list.append(x[0])
    print(all_doctype_list)
    conn.close()

process_list = ["0_ProductionDownload","1_Decryption","2_Classification","3_Preprocessing","1_PreProcessing","2_PreRenaming","3_Documents_Uploaded","4_Documents_Downloaded","5_Invalidated_XML","6_Classification","7_PostRenaming","8_PostProcessing","9_TransferToArea51","a_ProductionDownload","b_Decryption","c_Classification","d_Preprocessing","a_PreProcessing","b_PreRenaming","c_Documents_Uploaded","d_Documents_Downloaded","e_Invalidated_XML","f_Classification","g_PostRenaming","h_PostProcessing","i_TransferToArea51"]
doctype_list = all_doctype_list
pipeline_list = ["ADE", "ADR", "Data_Bank_Inventory"]
client_list = ["Amerihome", "Homepoint","Loancare"]




#Make listing of path
path= databank_location
l=os.listdir(path)
print(l)
# Actual_list=('Pipeline','Client_Name','Reference_Key','','','')
result_list=[]
snapshot_list=[]
# Here to find the process of DFS (Defth First Search Graph)

cnt=0
for root, dirs, files in os.walk(path): #Search the path in detal
    for filename in files:
        cnt = cnt+1
        if cnt%5000==0:
            print(cnt)
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
        #print(data[1],"--",pipeline_list)
        client=None
        if data[1] in pipeline_list:
            for n in data:
                #print(n)
                if re.search(r'FX([0-9]+)', n):
                    FX_match = re.search(r'FX([0-9]+)', n).group(1)
    #                 print(DT_match)
                    fxset.add(FX_match)
    #                 print(dtset)

                if re.search(r'DT([0-9]+)', n):
                    DT_match = re.search(r'DT([0-9]+)', n).group(1)
#                     print(DT_match)
                    dtset.add(DT_match)
    #                 print(dtset)
                if re.search(r'ADS[-_]?[0-9]+', n):
                    Jira_match = re.search(r'ADS[-_]?[0-9]+', n).group(0)
#                     print(Jira_match)
                    jiraidset.add(Jira_match)
#                     print(jiraidset)
                if n in doctype_list:
                    doctype = n
                if n in process_list:
                    process = n
                if n in pipeline_list:
                    pipeline = n
                if n in client_list:
                    client = n
                    
           
            PAGEID = 'MI'+re.search(r'MI(.*)', filename).group(1)
            FXID = 'FX'+[fx for fx in fxset][0]        
            DTID = 'DT'+[dt for dt in dtset][0]
            JIRAID = [jira for jira in jiraidset][0]
           
            
            file_tuple = (pipeline,client,JIRAID,process,None,doctype,FXID,DTID,PAGEID)
            #print(file_tuple)
            snapshot_list.append(file_tuple)

totalDir = 0
totalFiles = 0
for root, dirs, files in os.walk(path):
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

#os.mkdir('E:/MLOPSDataBank_1.0Databank_Pipeline_Suite_Repository/Snapshot'+today.strftime('%Y-%m-%d'))
# print(snapshot_list)
df = pd.DataFrame(snapshot_list,columns=['PIPLELINE','CLIENT_NAME','REFERENCE_KEY','PROCESS','SUB_PROCESS','DOCTYPE','LOAN_ID','DOCUMENT_ID','PAGE_ID'])
        
df.to_csv("E:/MLOPSDataBank_1.0/Databank_Pipeline_Suite_Repository/snapshot.csv",mode='a',index=False,header=True)


# In[ ]:


# print(result_list)
# dirName=(path)
# #listOfEmptyDirs=[]
# result_list1=[]
#  # the directory tree and check if directory is empty.
# for (dirpath, dirnames, filenames) in os.walk(dirName):
#     if len(dirnames) == 0 and len(filenames) == 0 :
            
#         print(dirnames)
#         listOfEmptyDirs = dirpath.split('\\')
#         listOfEmptyDirs.pop(0)
#         result_list.append(listOfEmptyDirs)  
    
#     #  empty directories and print it
#         #for elem in listOfEmptyDirs:
#             #print(elem)    


# #print(result_list1)

# result_list3 = result_list + result_list1

# #print(result_list3)

# # # Here the all data inserted in CSV using Pandas DataFrame 
# df = pd.DataFrame(result_list3,columns=[1,2,3,4,5,6,7,8,9])
# df.to_csv('E:\\ADE_REPORT.csv', mode='a', index=False, header=True)
# print(result_list3)

# Conect to MySQL and load Snapshot Data


conn = mysql.connector.connect(host=host_name, database=db_name, user=USER_NAME, password=PASSWORD) 
if conn.is_connected():
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to database: ", record)
    
#         cursor.execute('DROP TABLE IF EXISTS ProductionData1;')
#         print('Creating table....')
#         cursor.execute("CREATE TABLE ProductionData1 (Folder_Name Varchar(25), FX_IDS varchar(23), DT_IDS varchar(34), Document_Name varchar(150))")
#         print("ProductionData1 table is created....")
    ins_cnt = 0
    for i,row in df.iterrows():
        sql = "INSERT INTO databankdb.SRC_TBL_PIPELINE VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
        ins_cnt = ins_cnt+1
        if ins_cnt%5000==0:
            print(ins_cnt," Records inserted")
        conn.commit()
        conn.close()

conn = mysql.connector.connect(host=host_name, database=db_name, user=USER_NAME, password=PASSWORD) 
if conn.is_connected():
    cursor = conn.cursor()
    cursor.execute("select count(*) from databankdb.SRC_TBL_PIPELINE")
    Production_count = cursor.fetchone()[0]
    print(Production_count)
    conn.close()

# Update Log file
writetolog('['+os.getlogin() +']'+ os.path.basename(__file__)+' Snapshot Stored In Your Respective DataBase-> '+ ' Total Scaned Folders : '+str(totalDir)+' Total Scan Pages: '+str(totalFiles)+' Total Rows Inserted In your Database: '+str(Production_count))


