import pymongo # need to import module Mongo DB
import json  # need to import json 
import pandas as pd # need pandas
import os
from pymongo import MongoClient

client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client ["MLOPSDATABANK"]

parent_path = "E:/Hemant/AMERIHOME/ADS-1300/DECRYPTION"
# tab = db["MLOPSDATABANK_DIARY"]
# print(collection_name)

for root, dirs, files in os.walk(parent_path): #Search the path in detal
    for f in files:
        if f.endswith('.json'):
            continue        
        op = os.path.join(root,f)
                
        data = op.split('\\')
#         print(data)
#         print(len(data) )
#FHA Mortgage Credit Analysis Worksheet-MI210870684_000364_C0-000001.png
        FX_match = data[len(data)-3]
        DT_match = re.search(r'^(.*)-', data[len(data)-2]).group(1)
        doctype_match = re.search(r'-(.*)$', data[len(data)-2]).group(1)
        pagename = data[len(data)-1]
        MI_match = re.search(r'-(.*?)_', pagename).group(1)
        JsonNo_match = re.search(r'.*?\_\d(.*?)_', pagename).group(1)
        pageno_section = pagename.split('_')
        tokens=len(pageno_section)
        actual_page_number = pageno_section[tokens-1].split('.')[0].split('-')[1]
        file_extension = pageno_section[tokens-1].split('.')[1]
        page_id = MI_match+'_'+JsonNo_match+'_C0-'+actual_page_number

        print(FX_match)
        print(DT_match)
        print(MI_match)
        print(doctype_match)
        print(pagename)
        print(JsonNo_match)
        print(actual_page_number)
        print(file_extension)
        print(page_id)

        rec = {
        "PAGEID":page_id,
        "FXID":FX_match,
        "DTID":DT_match,
        "MIID":MI_match,
        "DOCTYPE":doctype_match,
        "JSONNUMBER":JsonNo_match,
        "PAGENUMBER":actual_page_number,
        "EXTENSION":file_extension,
        "prerenaming_flag":"0",
        "preclassification_flag":"0",
        "postrenaming_flag":"0",
        "postclassification_flag":"0",
        "invalidatedxml_flag":"0",
        "preprocessing_flag":"0",
        "postprocessing_flag":"0",
        "uploadtoaiq_flag":"0"            
        }

        db.MLOPSDATABANK_DIARY.insert_one(rec)




# db.ProductionDownloadingData.updateOne({FILE_TYPE:"Tax Returns - MI111234567_000305_C0-000001.pdf",Duration:"HomePoint"},{$set:{Duration:"HomePoint1",newtag:"ABCD1"}})

prerenaming_path = 'E:/MLOPSDataBank/ADE/Tax_Return/ADSAIQ_Complete_DTID/2_PreRenaming'
use_mongo_db=1
prefix = "Misc"
doc_type='URLA'


for root, dirs, files in os.walk(prerenaming_path):
    page_count = 0
    print(root)
    #before_list.write(root+ os.linesep)
    if root == prerenaming_path:
        continue
    if root == prerenaming_path:
        continue        
    for filename in files:
        #before_list.write(filename+ os.linesep)
        if filename.endswith('.pdf'):
            page_count = page_count+1
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
        #D:\ADE\FX\DT\\page*.json
        print(root)
        #print(dirs)
        #print(filename)
        x = root.split("\\")
        #print(x)
        #x=["ade","fx","dt"]
        x = x[len(x)-1]
        #print('x - ',x)
        x = x.split('_')
        folder = x[0]
        doc = x[1]
        #print(folder)
        #print(doc)
        
        pat = r'.*?\_MI(.*?)_.*'             #See Note at the bottom of the answer
        jsonno_pat = r'.*?\_\d(.*?)_.*'
        s = str(filename)
        d = str(doc)
        print(s)
        MIItemID_match = "MI"+re.search(pat, s).group(1)
        JsonNo_match = re.search(jsonno_pat, s).group(1)
        pageno_section = filename.split('_')
        tokens=len(pageno_section)
        actual_page_number = pageno_section[tokens-1].split('.')[0].split('-')[1]
        file_extension = pageno_section[tokens-1].split('.')[1]
        
        print(filename,' ---> ',prefix+doc+"_"+folder+"_"+doc_type+"_"+filename)
        renamed_filename = prefix+doc+"_"+folder+"_"+doc_type+"_"+filename
        
        original_file_path = os.path.join(root, filename)
        renamed_file_path = os.path.join(root, renamed_filename)

        "Tax Returns - MI111234567_000305_C0-000001.pdf"
        mongo_pageid = MIItemID_match+'_'+JsonNo_match+'_C0-'+actual_page_number
        print(mongo_pageid)
        
        if use_mongo_db==1:
            db.MLOPSDATABANK_DIARY.update_one({"PAGEID":mongo_pageid},{"$set":{"prerenaming_flag":"1","prerenaming_fileloc":root,"prerenaming_original_filename":filename,"prerenaming_renamed_filename":renamed_filename}})
            # db.ProductionDownloadingData.updateOne({FILE_TYPE:"Tax Returns - MI111234567_000305_C0-000001.pdf",Duration:"HomePoint"},{$set:{Duration:"HomePoint1",newtag:"ABCD1"}})

