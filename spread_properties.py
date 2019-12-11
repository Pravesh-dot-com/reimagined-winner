# -*- coding: utf-8 -*-
"""
Created on Wed Aug 21 19:37:27 2019

@author: pjoshi
"""

db =  {'usr': 'BMR', 'pwd' : 'BMRDEV123', 'host' : 'GWPE05.GWPDEV.SEIC.COM', 'port' : '1521', 'svc' : 'DBDM01.SEIC.COM'}

generic_columns = {'num_of_columns' : 35,'headers' : ['COLUMN_4','COLUMN_5','COLUMN_6','COLUMN_7','COLUMN_8','COLUMN_9','COLUMN_10','COLUMN_11','COLUMN_12','COLUMN_13','COLUMN_14','COLUMN_15','COLUMN_16','COLUMN_17','COLUMN_18','COLUMN_19','COLUMN_20','COLUMN_21','COLUMN_22','COLUMN_23','COLUMN_24','COLUMN_25','COLUMN_26','COLUMN_27','COLUMN_28','COLUMN_29','COLUMN_30','COLUMN_31','COLUMN_32','COLUMN_33','COLUMN_34','COLUMN_35','COLUMN_36','COLUMN_37','COLUMN_38']}

sheet_list = { 'exclude_list' : ['FIRM INFO','Instruction','Revision','Values','ValidationResult (NOT TO FILL)','DBCAMS','PORTFOLIO CENTER','FACTSET','INBOUND Files (WIP)']}

xlsb_file = "TRIAL_NAME-SI-Config-Request-FIRM_NAME.xlsb" #add complete path here

col_fi_mandatory = ['IMPLEMENTATION REQUEST NAME','ACELA IMPS MANAGER','Contact Email Id','FEATURE NUMBER','Trial#','Instance','PO ID','PO BATCH REFERENCE','PO NAME','FIRM ID','FIRM NAME','FIRM BATCH REFERENCE','SUBFIRM ID(S)','SUBFIRM NAMES','PRODUCTION LIVE DATE']

log_file_path = "C:/Users/pjoshi/Desktop/spreadsheet.log"

FI = 'FIRM INFO'
