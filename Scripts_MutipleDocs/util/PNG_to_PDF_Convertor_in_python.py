#!/usr/bin/env python
# coding: utf-8

# In[33]:

import warnings
#from PIL import Image
#warnings.simplefilter('ignore', Image.DecompressionBombWarning)
from PIL import Image
MAX_IMAGE_PIXELS = None
#933120000 
#211680000)
import os
from datetime import datetime

# In[34]:
#(base) F:\Python_Script_bkp>python PNG_to_PDF_Convertor_in_python.py
#Total Folders to be scanned :  41131
#Total PNG files to be converted to PDF :  340754
#         - 0  png files converted to pdf at  2021-09-21 00:08:38.359958
#         - 100  png files converted to pdf at  2021-09-21 00:09:04.663920
#         - 200  png files converted to pdf at  2021-09-21 00:09:32.293990
#         - 300  png files converted to pdf at  2021-09-21 00:11:21.398208


directory = 'E:/SSA_1467'

totalDir = 0
totalFiles = 0
for root, dirs, files in os.walk(directory):
    totalsubDir = 0
    totalsubFiles = 0
    filescancount = 0

    for directories in dirs:
        totalsubDir += 1
    totalDir = totalDir + totalsubDir

    for Files in files:
        if Files.endswith('.png'):
            totalsubFiles += 1
    totalFiles = totalFiles + totalsubFiles

print('Total Folders to be scanned : ', totalDir)
print('Total PNG files to be converted to PDF : ', totalFiles)
count=0
for root, dirs, files in os.walk(directory):
    for filename in files:
        #print(root, files)
        #print(filename)
        if filename.endswith('.png'):
            if count%100==0:
                print("         -",count, ' png files converted to pdf at ',datetime.now())
#             print(filename.split(".")[0])
            filename_without_ext = filename.split(".")[0]
#             print(root, filename)
#             print(os.path.join(root, filename))
            image1 = Image.open(os.path.join(root, filename))
            im1 = image1.convert('RGB')
            im1.save(root+"/"+filename_without_ext+'.pdf')
            
            os.remove(os.path.join(root, filename))
            count=count+1


#filename  = 'abc.png'
#a = filename.split()
#a = ['abc','png']




