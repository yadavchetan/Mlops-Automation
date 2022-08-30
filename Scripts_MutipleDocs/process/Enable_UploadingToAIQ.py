#!/usr/bin/env python
# coding: utf-8




import paramiko
import os
import sys
import time
import socket
from zipfile import ZipFile
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

HOST = read_config.get("aiqupload","host")
USERNAME = read_config.get("aiqupload","username")
PASSWORD = read_config.get("aiqupload","password")
PORT = read_config.get("aiqupload","port")
src=sys.argv[1]
dest=read_config.get("aiqupload","destination")

'''
HOST = "10.25.20.226"
USERNAME = "ngadetest1-hourly"
PASSWORD = "123456a!"
PORT = 22
src="E:\\HemantTestConnection\\";
dest="/";
'''


startTime=round(time.time()*1000)
print(HOST,' ',PORT)
transport = paramiko.Transport((HOST,int(PORT)))
transport.connect(username=USERNAME,password=PASSWORD)
sftp = paramiko.SFTPClient.from_transport(transport)

# 1) Change to given directory path
# 2) Get list of all the files which you want to send

items=os.listdir(src)
print(items)
sftp.chdir(dest)
for item in items:
    print(item)
    sftp.put(os.path.join(src,item), item)

# 3) Loop through all the files and call spft.put(src ; sourcefile, destFileName);

#sftp.put('E:\\Hemant', '/test1_ADR')



sftp.close()
endTime=round(time.time()*1000)
print ("Connected to your location")
print("total Time required : " + str(endTime-startTime) +" MS")



