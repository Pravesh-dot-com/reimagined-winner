# -*- coding: utf-8 -*-
"""
Created on Fri Aug  9 12:59:12 2019

@author: pjoshi
"""
import pandas as pd
from pyxlsb import open_workbook as open_xlsb
from datetime import datetime
import sys
import numpy as np
from sqlalchemy import types, create_engine
from spread_properties import db, generic_columns as gen_col, sheet_list, xlsb_file, col_fi_mandatory 

##############################################################################
#                               Validation check
##############################################################################
def validate_check():
    #--1. Check DB Connection and find max no of columns in Table 2
    try:
        conn.connect()
        print('DB Connection successful ')
        sql = 'select count(1) from all_tab_columns where table_name = :sid '
        cursor = conn.execute(sql,{'sid' : tab_other.upper()})
        result = cursor.fetchone()
        max_cols = result[0] - 7 #mandatory columns
        cursor.close() 
        return max_cols
    except Exception as e:
        print('FATAL Error ! DB Connection failed ! Continue logging errors and Warnings. Error desc - ',e.args)
        global data_load_in_table, log_err_in_table 
        data_load_in_table,log_err_in_table = 'N', 'N'
        return 0

##############################################################################
#                               log_table update
##############################################################################
def log_upd(sql, e_code, sev, e_desc, sid = 0, oid =0, tab = 'NA', sec = 'NA', can_log='Y'):
    
    if can_log == 'Y':
        global SI_SPREADSHEET_LOAD_STATUS_ID
        cursor = conn.execute(sql,{'llid' : SI_SPREADSHEET_LOAD_STATUS_ID, 'lsid' : sid, 'loid' : oid , 'ltab' : tab, 'lsec' : sec, 'le_code' : e_code, 'lsev' : sev, 'le_desc' : e_desc})
        cursor.close() 
        #print('log id : ',SI_SPREADSHEET_LOAD_STATUS_ID)
        SI_SPREADSHEET_LOAD_STATUS_ID += 1
    else:
        print('Log cannot be written to DB table since the DB is down or not accessible.')
    
    return  

##############################################################################
#                              Create DataFrame for Table 2
##############################################################################
def insert_df(df,SI_OBND_IBND_INTERFACES_ID, tab_name, FIS = 'N'):
    
    header_list=gen_col['headers']
    #['COLUMN_4','COLUMN_5','COLUMN_6','COLUMN_7','COLUMN_8','COLUMN_9','COLUMN_10','COLUMN_11','COLUMN_12','COLUMN_13','COLUMN_14','COLUMN_15','COLUMN_16','COLUMN_17','COLUMN_18','COLUMN_19','COLUMN_20','COLUMN_21','COLUMN_22','COLUMN_23','COLUMN_24','COLUMN_25','COLUMN_26','COLUMN_27','COLUMN_28','COLUMN_29','COLUMN_30','COLUMN_31','COLUMN_32','COLUMN_33','COLUMN_34','COLUMN_35','COLUMN_36','COLUMN_37','COLUMN_38']
    
    for i in range(len(df.columns),gen_col['num_of_columns']): #35
        col_name = 'COLUMN_'+str(i+1)
        df[col_name] = None
    df.columns = header_list
    df = df.dropna(axis = 0,how='all')
    section1_name = 'Section 1'#df['COLUMN_4'][0]
    df = df[3:]        
    df = df.reset_index(drop=True)
    df = df.astype({'COLUMN_4' : str})
    x = df.index[df['COLUMN_4'].str.match('Section 2')].tolist()
        
    
    x = len(x)
    
    if x > 0:
        section2_name = 'Section 2'#df['COLUMN_4'][x]
        df.insert(loc=0,column='SECTION_NAME',value = None)
        df['SECTION_NAME'][:x] = section1_name
        df['SECTION_NAME'][x+3:] = section2_name
        stat = 'Section 2 found for sheet_name : ' + tab_name 
        print(stat)
        log_upd(sql_insert_load_log,e_code_0,sev_info,stat,sid=SI_SPREADSHEET_DETAILS_ID, tab = tab_name, sec = 'Section 2' ,can_log=log_err_in_table)
        
    else:
        df.insert(loc=0,column='SECTION_NAME',value = section1_name)
        if FIS == 'N':
            stat = 'Section 2 not found for sheet_name : '+tab_name
            print(stat)
            log_upd(sql_insert_load_log,e_code_2,sev_warn,stat,sid=SI_SPREADSHEET_DETAILS_ID, tab = tab_name, sec = 'Section 2' ,can_log=log_err_in_table)
    
    df.insert(loc=0,column='TAB_NAME',value = tab_name)
    df.insert(loc=0,column='SI_SPREADSHEET_DETAILS_ID',value = SI_SPREADSHEET_DETAILS_ID)
    df.insert(loc=0,column='SI_OBND_IBND_INTERFACES_ID',value = None)
    df['AUDIT_USERID'] = 0    
    df['DM_LSTUPDDT'] = sysdate
    df = df.astype({'DM_LSTUPDDT' : np.datetime64 })    
    df['SI_OBND_IBND_INTERFACES_VER'] = VER_NO
    df = df.reset_index(drop=True)
    df['SI_OBND_IBND_INTERFACES_ID'] = df.index + SI_OBND_IBND_INTERFACES_ID+1
    
    if FIS == 'Y':
        df['SECTION_NAME'] = 'Section 2'
        dfs1 = None
        dfs2 = df
        ret_status  = 3
        return dfs1, dfs2,df['SI_OBND_IBND_INTERFACES_ID'].max(), ret_status
        
    if x > 0:
        dfs1 = df.iloc[:x, :]
        dfs2 = df.iloc[x+3:, :]
        dfsx = dfs1.append(dfs2,ignore_index = True)   
        dfsx = dfsx.reset_index(drop=True)
        dfsx['SI_OBND_IBND_INTERFACES_ID'] = dfsx.index + SI_OBND_IBND_INTERFACES_ID+1
        dfs1 = dfsx.iloc[:x+1, :]
        dfs2 = dfsx.iloc[x+1:, :]        
        ret_status = 2
    else:
        dfs1 = df
        dfs2 = None
        ret_status = 1
        
    return dfs1, dfs2,df['SI_OBND_IBND_INTERFACES_ID'].max(), ret_status

##############################################################################
#                               get_max_id
##############################################################################
def get_max_id():
    
    sql_tab1 = "select decode(max(SI_SPREADSHEET_DETAILS_ID),null,0,max(SI_SPREADSHEET_DETAILS_ID)) from SI_SPREADSHEET_DETAILS"
    sql_tab2 = "select decode(max(SI_OBND_IBND_INTERFACES_ID),null,0, max(SI_OBND_IBND_INTERFACES_ID)) from SI_OBND_IBND_INTERFACES"
    sql_ver = "select decode(max(SI_SPREADSHEET_DETAILS_VER),null,0,max(SI_SPREADSHEET_DETAILS_VER)) from SI_SPREADSHEET_DETAILS"
    sql_log_tab = "select decode(max(SI_SPREADSHEET_LOAD_STATUS_ID),null,0,max(SI_SPREADSHEET_LOAD_STATUS_ID)) from SI_SPREADSHEET_LOAD_STATUS"
    
    cur = conn.execute(sql_tab1)
    res1 = cur.fetchone()
    SI_SPREADSHEET_DETAILS_ID = res1[0] +1
    cur.close()
    
    cur = conn.execute(sql_tab2)
    res2 = cur.fetchone()
    SI_OBND_IBND_INTERFACES_ID = res2[0]
    cur.close()
    
    cur = conn.execute(sql_ver)
    res3 = cur.fetchone()
    SI_SPREADSHEET_DETAILS_VER = res3[0] + 1
    cur.close()
    
    cur = conn.execute(sql_log_tab)
    res4 = cur.fetchone()
    SI_SPREADSHEET_LOAD_STATUS_ID = res4[0] + 1
    cur.close()
    
    return SI_SPREADSHEET_DETAILS_ID, SI_OBND_IBND_INTERFACES_ID, SI_SPREADSHEET_DETAILS_VER, SI_SPREADSHEET_LOAD_STATUS_ID

##############################################################################
#                               FIRM_INFO DF Processing
##############################################################################        
def fi_process(df, pk):
    df_1 = df.T
    col_fi_sec_1 = list(df_1.iloc[0,:15])
    val_fi_sec_1 = list(df_1.iloc[1,:15])
    val_col_not_found = 0
    
    mand_col_not_exist = []
    for col in col_fi_mandatory:
        if col not in col_fi_sec_1:
            mand_col_not_exist.append(col)
        
    if len(mand_col_not_exist)>0:
        val_col_not_found = 1
        stat = FI+' : Section 1 - Missing '+str(len(mand_col_not_exist)) +' of the mandatory 15 fields in the sheet. Please find the field(s) as follows : '+str(mand_col_not_exist)
        print(stat)
        log_upd(sql_insert_load_log,e_code_1,sev_err,stat,sid=SI_SPREADSHEET_DETAILS_ID,  tab = FI, sec = 'Section 1' ,can_log=log_err_in_table)
        
    else:
        stat = FI+' : Section 1 - All the fields are exactly as expected. Good to go !'
        print(stat)
        log_upd(sql_insert_load_log,e_code_0,sev_info,stat,sid=SI_SPREADSHEET_DETAILS_ID,  tab = FI, sec = 'Section 1' ,can_log=log_err_in_table)
        
    cntr = 0
    
    for val in val_fi_sec_1:
        if val == '' or val == None:
            val_col_not_found = 1
            mand_col = col_fi_sec_1[cntr]
            stat = FI+' : Section 1 - '+str(mand_col)+' cannot be blank. Please enter some value and retry'
            print(stat)
            log_upd(sql_insert_load_log,e_code_1,sev_err,stat,sid=SI_SPREADSHEET_DETAILS_ID,  tab = FI, sec = 'Section 1' ,can_log=log_err_in_table)
        else:
            stat = FI+' : Section 1 - All the mandatory fields have values. Good to proceed!'
            print(stat)
            log_upd(sql_insert_load_log,e_code_0,sev_info,stat,sid=SI_SPREADSHEET_DETAILS_ID,  tab = FI, sec = 'Section 1' ,can_log=log_err_in_table)
        cntr += 1
     
    if val_col_not_found == 1:
        stat = FI+' : Section 1 - Problem with the mandatory fields. Check previous logs!'
        print(stat)
        log_upd(sql_insert_load_log,e_code_1,sev_err,stat,sid=SI_SPREADSHEET_DETAILS_ID,  tab = FI, sec = 'Section 1' ,can_log=log_err_in_table)
        sys.exit()
        
    df_1 = df_1.drop(df_1.index[0])
    df_1.insert(loc=0,column='id',value=pk)
    rename_col_tab1 = ['SI_SPREADSHEET_DETAILS_ID','IMPLEMENTATION_REQUEST_NAME','ACELA_IMPS_MANAGER','CONTACT_EMAIL_ID','FEATURE_NUMBER','TRIAL_NO','INSTANCE_ENV','PO_ID','PO_BATCH_REFERENCE','PO_NAME','FIRM_ID','FIRM_NAME','FIRM_BATCH_REFERENCE','SUBFIRM_IDS','SUBFIRM_NAMES','PRODUCTION_LIVE_DATE']
    df_1.columns = rename_col_tab1
    df_1 = df_1.assign(PROCESSING_STATUS = load_started,PROCESSING_STATUS_CD = [5000], LOAD_DATE = rpt_date,AUDIT_USERID = [0], DM_LSTUPDDT = sysdate,SI_SPREADSHEET_DETAILS_VER=VER_NO)
    df_1 = df_1.astype({'LOAD_DATE' : np.datetime64, 'DM_LSTUPDDT' : np.datetime64})    
    return df_1 

##############################################################################
#                               firm_info
##############################################################################
def firm_info(wb,firm_info_pk):
    with wb.get_sheet(FI) as sheet: 
        list_df = []   
        for row in sheet.rows(sparse=True):
            list_df.append([item.v for item in row])
        dff = pd.DataFrame(list_df)
        dff = dff[3:]                    
        dff = dff.dropna(axis=0,how='all')
        dff = dff.reset_index(drop=True)
        ind=int(dff[dff[0].str.match('Section 2')].index.values[0])
        
        if ind == 0:
            ret_status = 1
            df_1 = fi_process(dff,firm_info_pk)  #Invoke func
            df_2=None
            stat = 'Section 2 not found for sheet_name : ' + FI
            print(stat)
            log_upd(sql_insert_load_log,e_code_3,sev_warn,stat,sid=SI_SPREADSHEET_DETAILS_ID,  tab = FI, sec = 'Section 2' ,can_log=log_err_in_table)
            return df_1, df_2,ret_status
                        
        else:
            df_2 = dff.iloc[ind+3:].copy()
            s = df_2.iloc[0]
            s.name = 0
            for i in range(3):
                df_2 = df_2.append(s)
            df_2 = df_2.sort_index()
            df_2.iloc[0,0] = 'Section 1'
            df_2 = df_2.reset_index(drop=True)            
            df_x = dff.iloc[:ind-1,:2].copy()
            df_1 = fi_process(df_x,firm_info_pk)  #Invoke func2
            ret_status = 2
            stat = 'Section 2 found for sheet_name : '+FI 
            print(stat)
            log_upd(sql_insert_load_log,e_code_0,sev_info,stat,sid=SI_SPREADSHEET_DETAILS_ID, tab = FI, sec = 'Section 2' ,can_log=log_err_in_table)
            return df_1,df_2,ret_status

##############################################################################
#                               INSERT Into DB
##############################################################################
        
def to_DB (df, table_name, col_dtype,idx = False, tab_name  = 'NA', sec_name = 'NA'):
    
    try :
        df.to_sql( table_name, conn,  if_exists='append', chunksize=5000,index=idx,dtype = col_dtype)    
        stat = 'sheet_name : ' + tab_name+' : '+sec_name+' - Data Load completed'
        print(stat)
        log_upd(sql_insert_load_log,e_code_0,sev_info,stat,sid=SI_SPREADSHEET_DETAILS_ID, tab = tab_name, sec = sec_name)
        return 0
    except Exception as ex:
        err_type = type(ex)
        err_desc = ex.args
        stat1 = 'sheet_name : ' + tab_name+' : '+sec_name+' - Data Load Failed !!'
        stat2 = 'err_type : '+str(err_type)+' and err_desc : '+str(err_desc)
        print(stat1+stat2)
        log_upd(sql_insert_load_log,e_code_1,sev_err,stat1+stat2,sid=SI_SPREADSHEET_DETAILS_ID, tab = tab_name, sec = sec_name)
        return 1             
##############################################################################
#                               ROLLBACK DB Changes
##############################################################################      
    
    
def roll_back_DB_changes( tab, tab_id ):
    sql_tb1 = 'delete from SI_SPREADSHEET_DETAILS where SI_SPREADSHEET_DETAILS_ID = :id'
    sql_tb2 = 'delete from SI_OBND_IBND_INTERFACES where SI_SPREADSHEET_DETAILS_ID = :id'
    if tab == 1:
        try:
            cursor = conn.execute(sql_tb1,{'id' : tab_id})
            stat = 'DB changes have been rolled back for Table 1'
            print(stat)
            log_upd(sql_insert_load_log,e_code_0,sev_info,stat,sid=SI_SPREADSHEET_DETAILS_ID, can_log=log_err_in_table)
            cursor.close()
        except Exception as e:
            e_type = type(e)
            e_desc = e.args
            stat = 'DB changes were not rolled back for Table 1. Error type : '+str(e_type)+' and err_desc : '+str(e_desc)
            print(stat)
            log_upd(sql_insert_load_log,e_code_4,sev_err,stat,sid=SI_SPREADSHEET_DETAILS_ID,can_log=log_err_in_table)
            
    elif tab == 2:
        try:
            cursor = conn.execute(sql_tb2,{'id' : tab_id})
            stat = 'DB changes have been rolled back for Table 2'
            print(stat)
            log_upd(sql_insert_load_log,e_code_0,sev_info,stat,sid=SI_SPREADSHEET_DETAILS_ID, can_log=log_err_in_table)
            cursor.close()
        except Exception as e:
            e_type = type(e)
            e_desc = e.args
            stat = 'DB changes were not rolled back for Table 2. Error type : '+str(e_type)+' and err_desc : '+str(e_desc)
            print(stat)
            log_upd(sql_insert_load_log,e_code_4,sev_err,stat,sid=SI_SPREADSHEET_DETAILS_ID,can_log=log_err_in_table)
    

##############################################################################
#                               Update in DB
##############################################################################    

def to_update(sql,stat_id,idu):
    cursor = conn.execute(sql,{'status' : stat_id, 'for_id' : idu , 'lst_upd' : sysdate})
    cursor.close()        
        
##############################################################################
#                               MAIN
##############################################################################
sysdate = datetime.now()
rpt_time = sysdate.strftime('%Y-%m-%d %H:%M:%S')
rpt_date = sysdate.strftime('%Y-%m-%d')

db_usr, db_pwd, db_host, db_port, db_service = db['usr'], db['pwd'], db['host'], db['port'], db['svc']
conn =  create_engine('oracle+cx_oracle://'+str(db_usr)+':'+str(db_pwd)+'@'+str(db_host)+':'+str(db_port)+'/?service_name='+str(db_service))
tab_fi, tab_other, tab_log = 'si_spreadsheet_details', 'si_obnd_ibnd_interfaces', 'si_spreadsheet_load_status'

data_load_in_table, log_err_in_table = 'Y', 'Y'

sql_upd_spread_loaded = "update SI_SPREADSHEET_DETAILS set PROCESSING_STATUS = :status, DM_LSTUPDDT = :lst_upd where SI_SPREADSHEET_DETAILS_ID = :for_id"

dtype_tab_1 = {'SI_SPREADSHEET_DETAILS_ID' : types.Integer, 'IMPLEMENTATION_REQUEST_NAME' : types.VARCHAR, 'ACELA_IMPS_MANAGER' : types.VARCHAR, 'CONTACT_EMAIL_ID' : types.VARCHAR, 'FEATURE_NUMBER' : types.VARCHAR, 'TRIAL_NO' : types.VARCHAR, 'INSTANCE_ENV' : types.VARCHAR, 'PO_ID' : types.VARCHAR, 'PO_BATCH_REFERENCE' : types.VARCHAR, 'PO_NAME' : types.VARCHAR, 'FIRM_ID' : types.VARCHAR, 'FIRM_NAME' : types.VARCHAR, 'FIRM_BATCH_REFERENCE' : types.VARCHAR, 'SUBFIRM_IDS' : types.VARCHAR, 'SUBFIRM_NAMES' : types.VARCHAR, 'PRODUCTION_LIVE_DATE' : types.VARCHAR, 'PROCESSING_STATUS' : types.Integer, 'PROCESSING_STATUS_CD' : types.Integer, 'LOAD_DATE' : types.Date , 'AUDIT_USERID' : types.Integer, 'DM_LSTUPDDT' : types.DateTime , 'SI_SPREADSHEET_DETAILS_VER' : types.Integer}
dtype_tab_2 = {'SI_OBND_IBND_INTERFACES_ID' : types.Integer, 'SI_SPREADSHEET_DETAILS_ID' : types.Integer, 'TAB_NAME' : types.VARCHAR, 'SECTION_NAME' : types.VARCHAR, 'COLUMN_4' : types.VARCHAR, 'COLUMN_5' : types.VARCHAR, 'COLUMN_6' : types.VARCHAR, 'COLUMN_7' : types.VARCHAR, 'COLUMN_8' : types.VARCHAR, 'COLUMN_9' : types.VARCHAR, 'COLUMN_10' : types.VARCHAR, 'COLUMN_11' : types.VARCHAR, 'COLUMN_12' : types.VARCHAR, 'COLUMN_13' : types.VARCHAR, 'COLUMN_14' : types.VARCHAR, 'COLUMN_15' : types.VARCHAR, 'COLUMN_16' : types.VARCHAR, 'COLUMN_17' : types.VARCHAR, 'COLUMN_18' : types.VARCHAR,'COLUMN_19' : types.VARCHAR,'COLUMN_20' : types.VARCHAR,'COLUMN_21' : types.VARCHAR,'COLUMN_22' : types.VARCHAR,'COLUMN_23' : types.VARCHAR,'COLUMN_24' : types.VARCHAR,'COLUMN_25' : types.VARCHAR,'COLUMN_26' : types.VARCHAR,'COLUMN_27' : types.VARCHAR,'COLUMN_28' : types.VARCHAR,'COLUMN_29' : types.VARCHAR,'COLUMN_30' : types.VARCHAR,'AUDIT_USERID' : types.Integer, 'DM_LSTUPDDT' : types.DateTime, 'SI_OBND_IBND_INTERFACES_VER' : types.Integer}

sql_upd_main_status = "update SI_SPREADSHEET_DETAILS set PROCESSING_STATUS = :status, DM_LSTUPDDT = :lst_upd where SI_SPREADSHEET_DETAILS_ID = :for_id"
sql_insert_load_log = "insert into si_spreadsheet_load_status values ( :llid, :lsid, :loid, :ltab, :lsec, :le_code, :lsev, :le_desc)"

sev_err, sev_warn, sev_info = 'ERROR', 'WARNING', 'INFO'
e_code_0, e_code_1, e_code_2, e_code_3, e_code_4, e_code_5 = 0, 1, 2, 3, 4, 5

log_file_path = "C:/Users/pjoshi/Desktop/spreadsheet.log"

exclude_list = sheet_list['exclude_list']
exclude_list_first = exclude_list[1:]
#inclusion_list = sheet_list['inclusion_list']
#inclusion_list2 = sheet_list['inclusion_list2']

FI = 'FIRM INFO'
load_started, load_completed, load_completed_werr = 1,2,3

try: 
    fcon = open(log_file_path,'w'); saveout=sys.stdout ; sys.stdout=fcon
except Exception as e:
    print(' error logging in file could not be initiated. error_type - '+str(type(e))+'Err_desc - '+str(e.desc))       
    sys.exit()
    
try:
    max_col_in_db = validate_check()
except Exception as e:
    print('Basic validation for DB connection failed. Exiting the process ! error_type - '+str(type(e))+'Err_desc - '+str(e.desc))    
    sys.exit()
   
max_col_in_xl = 0

try:
    SI_SPREADSHEET_DETAILS_ID,SI_OBND_IBND_INTERFACES_ID,VER_NO, SI_SPREADSHEET_LOAD_STATUS_ID = get_max_id()
except Exception as e:
    print('Unable to fetch Primary Key IDs from DB tables. Exiting the process ! error_type - '+str(type(e))+'Err_desc - '+str(e.desc))    
    sys.exit()

with open_xlsb(xlsb_file) as wb:
    
    if FI not in wb.sheets:
        data_load_in_table = 'N'
        fi_sheet_avl = 'N'
        stat = 'FATAL ERROR !!! FIRM INFO not found in excel. Continue logging further errors(if any)'
        print(stat)
        log_upd(sql_insert_load_log,e_code_1,sev_err,stat,sid=SI_SPREADSHEET_DETAILS_ID, tab = FI, sec = 'Section 1 & Section 2',can_log=log_err_in_table)
    else:
        print("----------------------------------------------------------------")
        print(FI + ' - Validation for Section 1')
        print("----------------------------------------------------------------")
        stat = 'Validation Sectin 1 : FIRM INFO Sheet found in excel. Good to proceed !'
        print(stat)
        log_upd(sql_insert_load_log,e_code_0,sev_info,stat,sid=SI_SPREADSHEET_DETAILS_ID, tab = FI, sec = 'Section 1 & Section 2' ,can_log=log_err_in_table)
         
    for sheet_name in wb.sheets:
        if sheet_name not in exclude_list_first:
            print("----------------------------------------------------------------")
            print(sheet_name + ' - Validation for Section 1')
            print("----------------------------------------------------------------")
            with wb.get_sheet(sheet_name) as sheet:
                list_df = []
                for row in sheet.rows(sparse=True):
                    list_df.append([item.v for item in row])
            data_frame = pd.DataFrame(list_df, columns=list_df[0])
            df = data_frame.copy()
            max_col = len(df.columns)
            if max_col > max_col_in_xl:
                max_col_in_xl = max_col
            
            if df.iloc[0][0] == None:
                chk_section1 = 0
            else:
                chk_section1 = df.iloc[0][0].count('Section 1')
                
            if chk_section1 == 1:                
                stat = 'Validation Sectin 1 : Section 1 found for sheet : '+sheet_name 
                print(stat)
                log_upd(sql_insert_load_log,e_code_0,sev_info,stat,sid=SI_SPREADSHEET_DETAILS_ID, tab = sheet_name, sec = 'Section 1')
            
            else:
                data_load_in_table = 'N'
                stat = 'FATAL ERROR !!! Validation Failed. Section 1 not found for sheet_name : ' +sheet_name
                print(stat)
                log_upd(sql_insert_load_log,e_code_1,sev_err,stat,sid=SI_SPREADSHEET_DETAILS_ID, tab = sheet_name, sec = 'Section 1')                
            
            data_frame = None
                       
    if max_col_in_xl > max_col_in_db:
        data_load_in_table = 'N'
        stat = 'ERROR !!! columns in Excel are greater than that of the DB table. Num of columns in DB Table : '+str(max_col_in_db) + ' and Num of columns in Excel : ' +str(max_col_in_xl)
        print(stat)
        print('columns in DB : ',max_col_in_db)
        print('columns in excel : ',max_col_in_xl)
        log_upd(sql_insert_load_log,e_code_1,sev_err,stat,sid=SI_SPREADSHEET_DETAILS_ID, tab = sheet_name, sec = 'Section 1')                
    else:
        stat = 'columns in Excel are less than that of the DB table. Good to go !'
        print(stat)
        print('columns in DB : ',max_col_in_db)
        print('columns in excel : ',max_col_in_xl)
        log_upd(sql_insert_load_log,e_code_0,sev_info,stat,sid=SI_SPREADSHEET_DETAILS_ID)                
     
###############################################################################
##---Loop through the sheet to load into DB
###############################################################################
        
if data_load_in_table == 'N':
    stat = 'Exiting the process as one of the basic checks failed.'
    print(stat)
    log_upd(sql_insert_load_log,e_code_1,sev_err,stat,sid=SI_SPREADSHEET_DETAILS_ID) 
    sys.exit()
    
else:
    with open_xlsb(xlsb_file) as wb:
        print("----------------------------------------------------------------")
        print(FI)
        print("----------------------------------------------------------------")
        
        try:
            df_fi_sec_1, df_2, fi_ret_status = firm_info(wb,SI_SPREADSHEET_DETAILS_ID)
        except Exception as e:
            stat = FI+' : sheet could not be processed. Check the sheet and log table for errors.error_type - '+str(type(e))+'Err_desc - '+str(e.desc)
            print(stat)
            log_upd(sql_insert_load_log,e_code_1,sev_err,stat,sid=SI_SPREADSHEET_DETAILS_ID, tab = FI, sec = 'Section 1 and Section 2')
            sys.exit()
        
        err_num = to_DB(df_fi_sec_1,tab_fi,dtype_tab_1, tab_name = FI, sec_name = 'Section 1' )
        
        
        if fi_ret_status == 2:
            try :
                df_NA, df_fi_sec_2,SI_OBND_IBND_INTERFACES_ID,ret_status= insert_df(df_2,SI_OBND_IBND_INTERFACES_ID,FI, FIS = 'Y')
            except Exception as e:
                stat = FI+' : Section 2 could not be processed from excel. Check the sheet and log table for errors.error_type - '+str(type(e))+'Err_desc - '+str(e.args)
                print(stat)
                log_upd(sql_insert_load_log,e_code_1,sev_err,stat,sid=SI_SPREADSHEET_DETAILS_ID, tab = FI, sec = 'Section 1 and Section 2')
                roll_back_DB_changes(tab = 1,tab_id=SI_SPREADSHEET_DETAILS_ID)
                sys.exit()
                
                
            err_num = to_DB(df_fi_sec_2,tab_other,dtype_tab_2,tab_name =  FI, sec_name = 'Section 2')
            if err_num != 0:
                roll_back_DB_changes(tab = 1,tab_id=SI_SPREADSHEET_DETAILS_ID)
                sys.exit()
                            
        for sheet_name in wb.sheets:
            if sheet_name not in exclude_list:
                print("----------------------------------------------------------------")
                print(sheet_name)
                print("----------------------------------------------------------------")
                with wb.get_sheet(sheet_name) as sheet:
                    list_df = []
                    for row in sheet.rows(sparse=True):
                        list_df.append([item.v for item in row])
                    data_frame = pd.DataFrame(list_df, columns=list_df[0])
                    
                    try:
                        df_sec_1, df_sec_2,SI_OBND_IBND_INTERFACES_ID,ot_ret_status = insert_df(data_frame,SI_OBND_IBND_INTERFACES_ID, sheet_name)
                    except Exception as e:
                        stat = sheet_name+' could not be processed from excel. Check the sheet and log table for errors.error_type - '+str(type(e))+'Err_desc - '+str(e.desc)
                        print(stat)
                        log_upd(sql_insert_load_log,e_code_1,sev_err,stat,sid=SI_SPREADSHEET_DETAILS_ID, tab = FI, sec = 'Section 1 and Section 2')
                        roll_back_DB_changes(tab = 2,tab_id=SI_SPREADSHEET_DETAILS_ID)
                        roll_back_DB_changes(tab = 1,tab_id=SI_SPREADSHEET_DETAILS_ID)
                        sys.exit()
                    
                    if ot_ret_status == 2:# both section present
                        err_num = to_DB(df_sec_1,tab_other,dtype_tab_2,tab_name =  sheet_name, sec_name = 'Section 1')
                        if err_num != 0 :
                            roll_back_DB_changes(tab = 2,tab_id=SI_SPREADSHEET_DETAILS_ID)
                            roll_back_DB_changes(tab = 1,tab_id=SI_SPREADSHEET_DETAILS_ID)
                            sys.exit()
                        
                        err_num = to_DB(df_sec_2,tab_other,dtype_tab_2,tab_name =  sheet_name, sec_name = 'Section 2')
                        
                        if err_num != 0 :
                            roll_back_DB_changes(tab = 2,tab_id=SI_SPREADSHEET_DETAILS_ID)
                            roll_back_DB_changes(tab = 1,tab_id= SI_SPREADSHEET_DETAILS_ID)
                            sys.exit()
                        
                    elif ot_ret_status == 1: # section 2 not present
                        err_num = to_DB(df_sec_1,tab_other,dtype_tab_2,tab_name =  sheet_name, sec_name = 'Section 1')
                        if err_num != 0 :
                            roll_back_DB_changes(tab = 2,tab_id=SI_SPREADSHEET_DETAILS_ID)
                            roll_back_DB_changes(tab = 1,tab_id=SI_SPREADSHEET_DETAILS_ID)
                            sys.exit()
                      
                    data_frame = None
     
    try:
        to_update(sql_upd_spread_loaded,load_completed,SI_SPREADSHEET_DETAILS_ID)
        stat = 'The load has been completed successfully. Processing Status updated'
        print(stat)
        log_upd(sql_insert_load_log,e_code_0,sev_info,stat,sid=SI_SPREADSHEET_DETAILS_ID)
    except Exception as e:
        stat = 'The load has been completed successfully. However, Processing Status could not be updated. error_type - '+str(type(e))+'Err_desc - '+str(e.desc)
        print(stat)
        log_upd(sql_insert_load_log,e_code_1,sev_err,stat,sid=SI_SPREADSHEET_DETAILS_ID)
        
        
        
