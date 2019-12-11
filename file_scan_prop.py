# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 13:55:27 2019

@author: pjoshi
"""

from sqlalchemy import types 

home_path = 'E:/Pravesh_v1/Work/files/file_scan'#/2019/01-Jan'
home_path_2 = r'\\atlas/gipp/GWMP Directory/Performance Engineering/BigData/Data/SPTC/TestFolder'

log_file_path = {'home' : "E:/Pravesh_v1/Work/py/py_log/filescan", 'away' : ""}

platform = { 'S' : 'SWP', 'T' : 'T3K'}

mon_wkdy = 1 #buffer_period_in_months for weekdays
mon_wknd = 11 #buffer_period_in_months for weekends


db =  {'usr': 'BMR', 'pwd' : 'BMRDEV123', 'host' : 'GWPE05.GWPDEV.SEIC.COM', 'port' : '1521', 'svc' : 'DBDM01.SEIC.COM'}

col_list_mandatory = ['TASKS', 'DUE', 'OWNER', 'QC', 'INITIALS&TIMESTAMP', 'TIMELINE', 'ACCURACY', 'COMMENTS']
col_list_optional = {'BATCH(S)' : 1}
num_col = 9

col_to_check_inval_data = 15


master_tab = 'stg_sptc_ebr_stmt_file_hdr'
child_tab = 'stg_sptc_ebr_stmt_file_dtl'
log_tab = 'stg_sptc_ebr_stmt_file_log'

df_file_stat_col_list = ['FILE_STAT_ID','FILE_NAME','FILE_SIZE_IN_BYTES','FILE_CREATE_DT','FILE_MODIFICATION_DT','FILE_LAST_ACCESS_DT','FILE_HEADER','FILE_PATH','FILE_PLATFORM','DS_CREATE_DT']
df_file_col_list = ['FILE_STAT_ID','TASK','BATCH', 'DUE', 'OWNER', 'QC', 'INITIALS_TIMESTAMP', 'TIMELINE', 'ACCURACY', 'COMMENTS','BUSINESS_DAY', 'DS_CREATE_DT','ORDER_NUM']


dtype_master = {'FILE_STAT_ID' : types.VARCHAR, 'FILE_NAME' : types.VARCHAR, 'FILE_SIZE_IN_BYTES' : types.Integer, 'FILE_CREATE_DT' : types.DateTime, 'FILE_MODIFICATION_DT' : types.DateTime, 'FILE_LAST_ACCESS_DT' : types.DateTime, 'FILE_HEADER' : types.VARCHAR, 'FILE_PATH' : types.VARCHAR, 'FILE_PLATFORM' : types.VARCHAR, 'DS_CREATE_DT' : types.DateTime}
dtype_child = {'FILE_STAT_ID' : types.VARCHAR, 'TASK': types.VARCHAR,'BATCH': types.VARCHAR, 'DUE': types.VARCHAR, 'OWNER': types.VARCHAR, 'QC': types.VARCHAR, 'INITIALS_TIMESTAMP': types.VARCHAR, 'TIMELINE': types.VARCHAR, 'ACCURACY': types.VARCHAR, 'COMMENTS': types.VARCHAR, 'BUSINESS_DAY': types.VARCHAR, 'DS_CREATE_DT' : types.DateTime, 'ORDER_NUM' : types.Integer}


