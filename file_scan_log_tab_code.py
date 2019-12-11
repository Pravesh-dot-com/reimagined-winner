# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 12:34:51 2019

@author: pjoshi
"""

import os
os.environ['NLS_LANG'] = ".AL32UTF8" #Allow non-ascii characters LIKE "…"
from os import scandir
import pandas as pd
import datetime as dt
import dateutil.relativedelta as dutl
from file_scan_prop import mon_wkdy, mon_wknd, home_path_2 as loc, log_file_path, platform as plat, col_list_mandatory, col_list_optional, num_col
from file_scan_prop import db, df_file_col_list, df_file_stat_col_list, master_tab, child_tab, log_tab, dtype_master, dtype_child, col_to_check_inval_data
import sys
import traceback
import numpy as np
from sqlalchemy import types, create_engine



#filepath = '\\\\atlas/gipp/GWMP Directory/Performance Engineering/BigData/Data/SPTC/TestFolder/2019/11-Nov'

#file = '518-1st Source.xlsx'


##############################################################################
#                               Get folder path(s)
##############################################################################

def get_folder_path(loc, months):
    
    """
    It identifies the months and year folders to prepare the path of the files to be fetched.
    months parameter is inclusive of the current month. For eg. months = 1 means current month, whereas months = 2 means current and previous month.
    """
    fold = {}    
    
    for i in range(0,months):
        m_delta = dutl.relativedelta(months=i)
        new_date = today - m_delta
        fold[new_date.strftime('/%Y/%m-%b')] = new_date.strftime('%Y_%m_') # create a dict of folder and c_key as key : value pair
          
    return fold
            

##############################################################################
#                               Get total no of files in Month folder
##############################################################################
    
def get_num_files_month_fold(folder_path):
    """
    count no of files in month folder. If zero, then break out. No further processing required.
    """
    global body_p 
    numf = 0
    try :
        os.scandir(folder_path)

        for root , direc ,files in os.walk(folder_path):        
            for file in files:
                if not file.startswith('~$'):
                    numf +=1   
        return numf
    
    except FileNotFoundError:
        stat ='Failure ! Month folder not found -'+str(folder_path)
        print(stat)
        log_upd('NA', 'NA', 'NA', e_sev_err, stat)
        return numf



##############################################################################
#                           File processing in DF
##############################################################################   

def to_df_file(df, ck, file_name, root_dir):
    """
    To process the DF and remove any unwanted rows or columns
    """
    global body_p
    skip_this_file = 'N'
    
    
    try:
        dff = df.copy().dropna(how= 'all', axis = 1)
        dff = dff.dropna(how='all', axis = 0)
        dff = dff.reset_index(drop=True)
        
        f_row = dff.iloc[0].isnull()
        f_col = dff.iloc[:,0].isnull()
        
        df_head = dff.iloc[0,0]
        
        invalid_f_row = 0
        invalid_f_col = 0
        
        df_head = dff.iloc[0,0]
        
        if df_head is np.NaN :
            
         #Delete first row if empty till tenth column
            for ix in f_row[:10].index.values:
                if f_row[ix] == False and ix<11: # not null value found 
                    invalid_f_row = 0
                    break
                else:
                    invalid_f_row += 1 
                            
            if invalid_f_row > 0:
                dff = dff.drop(axis=0, labels=[0])
                dff = dff.reset_index(drop=True)
                
    
            #Delete first column if empty till tenth row
            
            for ix in f_col[:10].index.values:
                if f_col[ix] == False: # not null value found 
                    invalid_f_col = 0
                    break
                else:
                    invalid_f_col += 1 
                            
                    
            if invalid_f_col > 0:
                dff = dff.drop(axis=1,labels=[0])
                
            df_head = dff.iloc[0,0]
            
            
        
        df_head = df_head.replace('  ','').strip()
        dff = dff.drop(axis=0, labels=[0])
        dff = dff.reset_index(drop=True) 
        
    
        invalid_col = []
        for ix in dff.iloc[0].isnull().index.values:
            if dff.iloc[0].isnull()[ix] == True:
                invalid_col.append(ix)
                #stat ='Null columns found before the header. Invalid Column number - '+str(ix+1)
        
        dff = dff.drop(axis=1, labels=invalid_col)

        
        """
        dff = dff.drop([0])
        dff = dff.dropna(how= 'all', axis = 1)
        dff = dff.dropna(how='all', axis = 0)
        
        dff = dff.reset_index(drop=True) 
        
        
        df_chk_null = dff.iloc[0]
        ind_null = df_chk_null[df_chk_null.isnull()].index.values
        
        
        if len(ind_null) > 0:
            for ix in ind_null:
                dff = dff.drop(axis=1,columns=ix)
            stat ='Null columns removed - '+str(len(ind_null))
        """   
        #dff = dff.drop(axis=0,labels = [0])#
        
        
        #dff = dff.drop([0])#remove the headings from the df
    except Exception as e:
        stat = 'Something wrong in the file - ' + str(file_name)+' ;  Error desc - '+str(e.args)
        stat =stat  
        
    df_col_old = dff.iloc[0,:].tolist()
    df_col = []
    optional_df_col = []
    
    
    for item in df_col_old:
        entry = item.upper()
        entry = entry.replace(' ','')
        df_col.append(entry)
    
    # Validation 1 -  mandatory columns
    
    for item in col_list_mandatory:
        if item not in df_col:
            stat ='One of the mandatory columns not found - '+str(item)
            print(stat)
            log_upd(ck, file_name, root_dir, e_sev_err, stat)
            skip_this_file = 'Y'
            return None,None, skip_this_file
        
            
    # Validation 2 - Check for optional columns; if not present add an empty column representing the optional column           
            
    for key in col_list_optional:
        if key not in df_col:
            optional_df_col.append(col_list_optional[key])
            lc = col_list_optional[key]
            dff.insert(lc,key,np.NaN)
         
    # Validation 3 -  check no of columns
       
    extra_cols = [] 
    extra_col_name = []    
    if len(dff.columns) > num_col:
        for lc in range(num_col,len(dff.columns)):
            extra_cols.append(dff.columns[lc])
            extra_col_name.append(dff.iloc[0,lc])
        dff = dff.drop(axis=1,labels = extra_cols)
        stat ='More than 9 columns found. Extra columns will be ignored. Column names  - '+ str(extra_col_name)
        print(stat)
        log_upd(ck, file_name, root_dir, e_sev_warn, stat)
    
    #drop Task and other header - row
    dff = dff.drop(axis=0, labels=[0])
    dff = dff.reset_index(drop=True)             
   
    
    # Convert Business Day rows into Columns
    
    dff['BUSINESS_DAY'] = np.NaN
    
    bd = dff[dff.iloc[:, 1:].isnull().all(1)] ## find all rows where except for first row all columns are null
        #bd = bd.reset_index(drop=True) 
        
    if len(bd) == 0 :
        stat ='Business Day row/data not found. Null value will be given by default'
        print(stat)
        log_upd(ck, file_name, root_dir, e_sev_info, stat)
    
    else:
        
        for i in range(0,len(bd.index)):
            cur = bd.index[i]
            if cur != bd.index[-1]:
                dff.at[cur:(bd.index[i+1]),'BUSINESS_DAY'] = bd.iloc[i,0]
                
            else:
                dff.at[cur:dff.shape[0],'BUSINESS_DAY'] = bd.iloc[i,0]
                
        dff = dff.drop(bd.index) 
        stat ='Business day rows converted to columns - '
        print(stat)
        log_upd(ck, file_name, root_dir, e_sev_info, stat)
    
   
    
    #stat ='Business day rows converted to columns - '
    
    # insert other DB columns    
    dff.insert(0,'FILE_STAT_ID',ck)
    dff = dff.astype(str)
    dff = dff.replace(to_replace= 'nan', value=np.NaN)
    dff['DS_CREATE_DT'] = sysdate
    dff['ORDER_NUM'] = dff.index
    dff.columns = df_file_col_list
    dff = dff.drop_duplicates(keep='first',subset=['TASK', 'DUE', 'OWNER', 'QC', 'BUSINESS_DAY']) # delete dups 
    
    return df_head, dff, skip_this_file


 
##############################################################################
#                           Encapsulate duplicate code
##############################################################################       

def check_files(root_dir, file_entry, platform, fi, ck):
    """
    This is to encapsulate the code that's being used at multiple locations in the program.
    """
    global body_p
    
    if fi == 1:
        stat ='File is currently getting accessed. File name - ' + str(file_entry.name)
        print(stat)
        log_upd(ck, file_entry.name, root_dir, e_sev_warn, stat)
                
    elif fi == 2:
        stat ='File is found. File name - ' + str(file_entry.name)
        print(stat)
        log_upd(ck, file_entry.name, root_dir, e_sev_info, stat)
        
        file_list = []
        file_list.append(ck)
        file_list.append(file_entry.name)
        file_list.append(file_entry.stat().st_size)
        file_list.append(dt.datetime.fromtimestamp(file_entry.stat().st_ctime))
        file_list.append(dt.datetime.fromtimestamp(file_entry.stat().st_mtime))
        file_list.append(dt.datetime.fromtimestamp(file_entry.stat().st_atime))
        file_list.append(None)#left empty for FILE_HEADER
        file_list.append(root_dir)
        file_list.append(platform) 
        file_list.append(sysdate)#column for DS_CREATE_DT
        #File_stat_list.append(file_list)
        df = pd.DataFrame(file_list)
        df = df.T                   # Transpose the rows into Columns since its made up of lists 
        df.columns = df_file_stat_col_list
        
        return df 
        
        
        
    elif fi == 3:
        stat ='This folder is unrecognisable : ' + str(file_entry.name) 
        log_upd(ck, file_entry.name, root_dir, e_sev_err, stat)
        print(stat)
    else:
        stat ="Something wrong with the folder or the files"
        print(stat)
        log_upd(ck, file_entry.name, root_dir, e_sev_warn, stat)
        
  
 
##############################################################################
#                               log_table update
##############################################################################

def log_upd(file_stat_id, file_name, folder, e_sev, e_desc):
    sql_insert_log = 'insert into '+ str(log_tab)+ ' values ( :col_file_stat_id, :col_load_dt, :col_file_name, :col_folder_name, :col_err_sev, :col_err_desc, :col_h_key)'
	
    if can_log == 'Y':
        try:
            cursor = conn.execute(sql_insert_log,{'col_file_stat_id' : file_stat_id, 'col_load_dt' : sysdate, 'col_file_name' : file_name , 'col_folder_name' : folder, 'col_err_sev' : e_sev, 'col_err_desc' : e_desc, 'col_h_key' : h_key })
            cursor.close()
            
        except Exception as e:
            stat = 'Cant load log details into LOG Table for File - '+str(file_name)+' at location - '+str(folder)+'. Error Desc - '+str(e.args)
            print(stat)
        
    #else:
        #print('Log cannot be written to DB table since the DB is down or not accessible.')

##############################################################################
#                               INSERT Into DB
##############################################################################
        
def to_DB (df, table_name, col_dtype, ck,idx = False):
    
    global body_p
    if data_load_in_table == 'Y':
        
        df.to_sql( table_name, conn,  if_exists='append', chunksize=ck,index=idx,dtype = col_dtype)    
        stat ='Function TO_DB invoked.'
        print(stat)
        

##############################################################################
#                               ROLLBACK DB Changes
##############################################################################      
 
    
def roll_back_DB_changes( stat_id ):
    sql_rb = 'delete from '+master_tab+' where FILE_STAT_ID = :id'
    global body_p
    stat ='roll_back invoked'
    
    try:
        cursor = conn.execute(sql_rb,{'id' : stat_id})
        stat = 'DB changes have been rolled back for Table 1'
        stat =stat
        cursor.close()
    except Exception as e:
        stat = 'DB changes were not rolled back for Table 1. Error type : '+str(type(e))+' and err_desc : '+str(e.args)
        stat =stat
        
            
    
##############################################################################
#                               MAIN
##############################################################################   


stat ='Start Time - '+str(dt.datetime.now().strftime('%d_%b_%Y_%I_%M_%S_%p'))
print(stat)

process_id = 193
env = 'Global'
run_stat = 'S'


db_usr, db_pwd, db_host, db_port, db_service = db['usr'], db['pwd'], db['host'], db['port'], db['svc']

conn =  create_engine('oracle+cx_oracle://'+str(db_usr)+':'+str(db_pwd)+'@'+str(db_host)+':'+str(db_port)+'/?service_name='+str(db_service))


sysdate = dt.datetime.now()
h_key = sysdate.strftime('%Y_%m_%d_%H_%M')
today = dt.date(2019,11,24)#(2020,8,10)
dt_tag = today.strftime('%d-%b-%Y')
c_year = today.year
c_month = today.month
c_day_of_week = today.weekday()


e_sev_info = 'INFO' # FYI
e_sev_err = 'ERROR' # File not processed
e_sev_warn = 'WARNING' # File processed but user needs to check 


SQL_DEL_STG_HDR = 'delete from '+str(master_tab)


dt_tm_tag_log = dt.datetime.now().strftime('%d_%b_%Y_%I_%M_%p')
    

    ##############################################################################
    #                               Validation - Check the system 
    ##############################################################################   

    
file_log = 'Y'
process_run_log = 'Y'
try: 
    fcon = open(log_file_path['home']+'_'+str(dt_tm_tag_log)+'.log','w'); saveout=sys.stdout;  sys.stdout=fcon
    print('Log file successfully created')
    
except Exception as e:
    print('error : logging in file could not be initiated. error_type - '+str(type(e))+'Err_desc - '+str(e.args))
    #fm.f_email(to_add, from_add, Subject, body_p, mail_type, attachment)
    file_log = 'N'
    
    
try:   
    conn =  create_engine('oracle+cx_oracle://'+str(db_usr)+':'+str(db_pwd)+'@'+str(db_host)+':'+str(db_port)+'/?service_name='+str(db_service))

    conn_proc = conn.raw_connection()
    curs = conn_proc.cursor()
    run_id = curs.var(int)
    curs.callproc('bmr_log.insert_process_run_dtl',[dt_tag,process_id,env,run_stat,run_id])
    stat ='DB Connection successful. Good to proceed.'
    print(stat)
    data_load_in_table = 'Y'
    can_log = 'Y'
    
    try:
        cursor = conn.execute(SQL_DEL_STG_HDR)
        stat = 'Header Staging table deleted'
        print(stat)
        log_upd('Basic Validation', 'Delete Staging', 'NA', e_sev_info, stat)
        cursor.close() 
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 1 -- Prepare(Delete) staging table for loading new data','Completed',''])
    except Exception as e:
        stat = 'Header Staging table deletetion failed. Error Desc - '+str(e.args)
        print(stat)
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 1 -- Prepare(Delete) staging table for loading new data','Failed',''])
        log_upd('Basic Validation', 'Delete Staging', 'NA', e_sev_err, stat)
        data_load_in_table = 'N'
    
except Exception as e:
    process_run_log = 'N'
    if file_log == 'Y':
        print('FATAL Error ! DB Connection failed ! Data will not be loaded. Continue logging errors and Warnings in Log file . Error desc - ',e.args)
        data_load_in_table = 'N'
        can_log = 'N'
    else:
        print('FATAL Error ! DB Connection failed ! Data will not be loaded. No logging possible. Exiting . Error desc - ',e.args)
        sys.exit()
        
        
  
    
    ############################################################################## 
    
try:
    
    if process_run_log == 'Y':
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 2 -- Basic Validation','Start',''])
    ## check for day of the week, if weekday(Monday(0) - Friday(4)) then fetch 1 month's data else (Saturday(5) and Sunday(6)) fetch 4 month's data
    ## 0 - Monday 
    
    if c_day_of_week in [5,6]:
        mon = mon_wknd
        stat ='Its a weekend. Therefore, '+str(mon)+ ' month(s) data will be fetched'
        print(stat)
        log_upd('Basic Validation', 'Weekend vs Weekday', 'NA', e_sev_info, stat)
    else:
        mon = mon_wkdy
        stat ='Its a weekday. Therefore, '+str(mon)+ ' month(s) data will be fetched'
        print(stat)
        log_upd('Basic Validation', 'Weekend vs Weekday', 'NA', e_sev_info, stat)
    
    ### call get_folder function
    
    try:
        folder = get_folder_path(loc,mon)
        stat ='Folder path created'
        print(stat)
        log_upd('Basic Validation', 'Folder identification', 'NA', e_sev_info, stat)
    except Exception as e:
        stat ='Folder path creation failed. Error desc - '+str(e.args)
        print(stat)
        log_upd('Basic Validation', 'Folder identification', 'NA', e_sev_err, stat)
       
    if process_run_log == 'Y':        
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 2 -- Basic Validation','END',''])    
    
    #----------
    
    if process_run_log == 'Y':
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 3 -- Navigate through files and folders','START','']) 
    
    File_stat_list = []
    
    for key in folder:
        f_path = (loc+str(key))  
        
        #Validation -  check number of files , if zero then exit
        try:
            num_of_files = get_num_files_month_fold(f_path)
            
            if num_of_files > 0:
                stat ='Total no of files to be processed : '+str(num_of_files)+' ; for folder : '+str(f_path)
                print(stat)
                log_upd('Basic Validation', 'NA', f_path, e_sev_info, stat)
            else:
                stat ='No SWP or T3K Files for the month : '+str(f_path) +' Skipping this folder.'
                print(stat)
                log_upd('Basic Validation', 'NA', f_path, e_sev_warn, stat)
                continue
             
        except Exception as e:
            stat ='get_num_files_month_fold function failed. error desc -'+str(e.args)
            print(stat)
            log_upd('Basic Validation', 'Code_error', f_path, e_sev_err, stat)
            continue
        
        #f_path = 'E:/Pravesh_v1/Work/files/file_scan/2019/12-Dec' #
        File_stat_list = [] #
        stat ="Folder path is - " + str(f_path)
        print(stat)
        
        try:
            entries = os.listdir(f_path)
            
            #Validation 
            if plat['S'] not in entries or len(entries) == 0:
                stat ="SWP Folder not found. moving out of this folder"
                print(stat)
                log_upd('Basic Validation', 'SWP', f_path, e_sev_err, stat)
                continue
                
            else:
                
                for entry in os.scandir(f_path): 
                
                    c_key = str(folder[key])+str(entry.name.replace('.xlsx',''))
                    c_key = c_key.replace(' ','_')
                                   
                    # file getting accessed - ignore
                    if entry.name.startswith('~$'):  
                        check_files(root_dir = f_path, file_entry = entry, platform = plat['T'], fi=1, ck = None )
                    
                    # valid file to be further processed
                    elif os.path.isfile(f_path+'/'+str(entry.name)):   
                        df_stat = check_files(root_dir = f_path, file_entry = entry, platform = plat['T'], fi = 2, ck = c_key)
                        stat ='Dataframe for Header file has been processed'
                        print(stat)
                        log_upd(c_key, entry.name, f_path, e_sev_info, stat)
                        
                        
                        
                        
                        try:
                            df = pd.read_excel(io=os.path.join(f_path,entry.name), header = None)
                            if len(df) <=2:
                                stat ="File doesn't have any data. File name : "+str(entry.name)
                                print(stat)
                                log_upd(c_key, entry.name, f_path, e_sev_warn, stat)
                                continue
                            
                            else:
                                stat ='File has data. Good to proceed.'
                                print(stat)
                                log_upd(c_key, entry.name, f_path, e_sev_info, stat)
                            
                                try:
                                    header, df_file, skip_file = to_df_file(df, c_key, entry.name, f_path) # file_df 
                                    if skip_file == 'Y':
                                        stat ='File - '+str(entry.name)+ ' has been skipped due to previous error'
                                        log_upd(c_key, entry.name, f_path, e_sev_warn, stat)
                                        print(stat)
                                        continue
                                    
                                    
                                    df_stat = df_stat.assign(FILE_HEADER = header) # df_stat updated with header
                                    stat ='Header Dataframe(df_stat) has been updated with Header column value and both Dataframes are ready for load'
                                    print(stat)
                                    log_upd(c_key, entry.name, f_path, e_sev_info, stat)
                                    
                                    
                                    # since both the DF got processed - Start loading into DB
                                    try:
                                        #insert FILE_STAT Data - Master
                                        to_DB(df_stat, master_tab, dtype_master ,1) # chunksize = 1
                                        stat ='Data insertion for Header Dataframe complete(df_stat)'
                                        print(stat)
                                        log_upd(c_key, entry.name, f_path, e_sev_info, stat)
                                        
                                        # insert FILE data - child
                                        try:
                                            to_DB(df_file, child_tab, dtype_child,500 ) # chunksize = 500
                                            stat ='Data insertion for Detail Dataframe(df_file) complete'
                                            print(stat)
                                            log_upd(c_key, entry.name, f_path, e_sev_info, stat)
                                                
                                        except Exception as e:
                                                stat ='error while inserting into DB for Detail Dataframe(df_file) -'+str(e.args)
                                                print(stat)
                                                log_upd(c_key, entry.name, f_path, e_sev_warn, stat)
                                                roll_back_DB_changes(c_key)
                                        
                                    except Exception as e:
                                        stat ='error while inserting into DB for Header Dataframe(df_stat)'+str(e.args)
                                        print(stat)
                                        log_upd(c_key, entry.name, f_path, e_sev_err, stat)
                                        
                                    
                                except Exception as e:
                                    stat ='error while calling DF function(to_df_file)'+str(e.args)
                                    print(stat)
                                    log_upd(c_key, entry.name, f_path, e_sev_err, stat)
                            
                            
                        except Exception as e:
                            stat ='error while creating main DF(pd.read_excel)'+str(e.args)
                            print(stat)
                            log_upd(c_key, entry.name, f_path, e_sev_err, stat)
                            
                            
    
                        #SWP folder - Repeat the previous steps
                    elif entry.name == plat['S']:
                        stat ='Its an SWP folder. Moving ahead.'
                        print(stat)
                        log_upd(c_key, entry.name, f_path, e_sev_info, stat)
                    
                        
                        
                            
                        f_path = f_path+'/'+plat['S']
                        
                        for entry in os.scandir(f_path):  
                            c_key = str(folder[key])+str(entry.name.replace('.xlsx',''))
                            c_key = c_key.replace(' ','_')
                            
                            
                            # file getting accessed - ignore
                            
                            if entry.name.startswith('~$'):  
                                check_files(root_dir = f_path, file_entry = entry, platform = plat['S'], fi=1, ck = None)
                        
                            # valid file to be further processed
                            elif os.path.isfile(f_path+'/'+str(entry.name)):   
                                df_stat = check_files(root_dir = f_path, file_entry = entry, platform = plat['S'], fi=2, ck = c_key)  
                                stat ='Dataframe for Header file has been processed'
                                print(stat)
                                log_upd(c_key, entry.name, f_path, e_sev_info, stat)
                                # call to_db function --> send DF to DB
                               # to_DB(df_stat, master_tab, dtype_master,1 ) # chunksize = 1
                               
                               
                               
                                try:
                                    df = pd.read_excel(io=os.path.join(f_path,entry.name), header = None)
                                    if len(df) <=2:
                                        stat ="File doesn't have any data. File name : "+str(entry.name)
                                        print(stat)
                                        log_upd(c_key, entry.name, f_path, e_sev_warn, stat)
                                        continue
                            
                                    else:
                                        
                                        stat ='File has data. Good to proceed.'
                                        print(stat)
                                        try:
                                            header, df_file, skip_file = to_df_file(df, c_key, entry.name, f_path) # file_df 
                                            if skip_file == 'Y':
                                                stat ='File - '+str(entry.name)+ ' has been skipped due to previous error'
                                                print(stat)
                                                log_upd(c_key, entry.name, f_path, e_sev_err, stat)
                                                continue
                                            df_stat = df_stat.assign(FILE_HEADER = header) # df_stat updated with header
                                            stat ='Header Dataframe(df_stat) has been updated with Header column value and both Dataframes are ready for load'
                                            print(stat)
                                            log_upd(c_key, entry.name, f_path, e_sev_info, stat)
                                            # since both the DF got processed - Start loading into DB
                                            try:
                                                #insert FILE_STAT Data - Master
                                                to_DB(df_stat, master_tab, dtype_master,1 ) # chunksize = 1
                                                stat ='Data insertion for Header Dataframe complete(df_stat)'
                                                print(stat)
                                                log_upd(c_key, entry.name, f_path, e_sev_info, stat)
                                                # insert FILE data - child
                                                
                                                try:
                                                    to_DB(df_file, child_tab, dtype_child,500 ) # chunksize = 500
                                                    stat ='Data insertion for Detail Dataframe complete(df_file)'
                                                    print(stat)
                                                    log_upd(c_key, entry.name, f_path, e_sev_info, stat)
                                                    
                                                except Exception as e:
                                                        stat ='error while inserting into DB for Detail Dataframe(df_file) -'+str(e.args)
                                                        print(stat)
                                                        log_upd(c_key, entry.name, f_path, e_sev_err, stat)
                                                        roll_back_DB_changes(c_key)
                                                  
                                                
                                            except Exception as e:
                                                stat ='error while inserting into DB for Header Dataframe(df_stat) -'+str(e.args)
                                                print(stat)
                                                log_upd(c_key, entry.name, f_path, e_sev_err, stat)
                                                
                                            
                                        except Exception as e:
                                            stat ='error while calling DF function(to_df_file)'+str(e.args)
                                            print(stat)
                                            log_upd(c_key, entry.name, f_path, e_sev_err, stat)
                                    
                                    
                                except Exception as e:
                                    stat ='error while creating main DF(pd.read_excel)'+str(e.args)
                                    print(stat)
                                    log_upd(c_key, entry.name, f_path, e_sev_err, stat)
                                
                        
                                    
                                    
                                    
                                    
                                    
                            else:
                                stat ='This folder is unrecognisable : ' + str(entry.name)
                                print(stat)
                                log_upd(c_key, entry.name, f_path, e_sev_warn, stat)
                        
                        stat ='SWP folder access completed. Moving out.'
                        print(stat)
                        log_upd('Basic Validation', entry.name, f_path, e_sev_info, stat)
                       
                    # For T3K Folder
                    else:
                        stat ='This folder is unrecognisable : ' + str(entry.name)                    
                        print(stat)
                        log_upd('Basic Validation', entry.name, f_path, e_sev_warn, stat)
                            
        except FileNotFoundError :
            
            stat ='Month folder not found. folder name - '+str(f_path)+' Skipping this folder'
            print(stat)  
            log_upd('Basic Validation', 'NA', f_path, e_sev_err, stat)
            continue
        
    if process_run_log == 'Y':
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 3 -- Navigate through files and folders','END','']) 
        curs.callproc('bmr_log.update_process_run_dtl',[run_id,'C'])

except Exception as e:
    stat ='error in code outer wrap. Error Desc - '+str(e.args)
    print(stat)
    log_upd('Basic Validation', 'NA', 'NA', e_sev_err, stat)
    
    if process_run_log == 'Y':
        curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
        curs.close()
            

                             


stat ='End Time - '+str(dt.datetime.now().strftime('%d_%b_%Y_%I_%M_%S_%p'))      
print(stat)    
    
    

        