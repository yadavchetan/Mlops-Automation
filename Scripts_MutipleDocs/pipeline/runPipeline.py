#!/usr/bin/env python
# coding: utf-8
# ------------------------------------------------------------------------------#
# Script: Executes all pipeline process in sequence based on the its on/off switch
# Initial Author: Omkar Sonar
# Date: 13/12/2021

    
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
from datetime import datetime
import re

pd.set_option("display.max_columns",None)

import configparser
parser=configparser.ConfigParser()

parser.read('F:/Data_Bank_Automation/Databank_Pipeline_Suite_Repository/Config/central_config.ini')

renaming = parser['pipeline']['prerenaming_swt']

if renaming ==1:
    exec("F:/Data_Bank_Automation/Databank_Pipeline_Suite_Repository/Scripts/databank/process/Renaming_files_post_aiqDownload_newStructure_postbucketing_vgt30.py")
