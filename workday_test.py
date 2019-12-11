# -*- coding: utf-8 -*-
"""
Created on Tue Nov  5 17:56:17 2019

@author: pjoshi
"""

 
    
import pandas as pd
import requests as req
import json
from sqlalchemy import types, create_engine
from workday_prop import db, workday, workday_url, log_file_path, to_add, from_add
import mailing_func as fm
import datetime as dt
import numpy as np
import sys


today = dt.date.today()
dt_tag = today.strftime('%d-%b-%Y')
dt_tm_tag_log = today.strftime('%d_%b_%Y_%I_%p')
rpt_time = today.strftime('%Y-%m-%d')
sysdate = dt.datetime.now()
process_id = 192
env = 'WD'
run_stat = 'S'


usr = db['usr']
pwd = db['pwd']
host = db['host']
port = db['port']
svc = db['svc']

wd_usr = workday['usr']
wd_pwd = workday['pwd']
body_p = ''
NL = '\n'

mail_type = 'html'
attachment = ''
Subject = 'Failure Alert - Workday DEV Job'


try: 
    fcon = open(log_file_path['home']+'_'+str(dt_tm_tag_log)+'.log','w'); saveout=sys.stdout;  sys.stdout=fcon
    body_p = body_p +NL+'Log file successfully created'
    
except Exception as e:
    body_p = body_p +NL+'error : logging in file could not be initiated. error_type - '+str(type(e))+'Err_desc - '+str(e.args)
    fm.f_email(to_add, from_add, Subject, body_p, mail_type, attachment)
    


engine = 'oracle+cx_oracle://'+str(usr)+':'+str(pwd)+'@'+str(host)+':'+str(port)+'/?service_name='+str(svc)


#------------Establish DB Connection---------------------#

try :
    con_ddl = create_engine(engine);conn = con_ddl.raw_connection()
    body_p = body_p +NL+'DB Connection successfully established'
    
except Exception as e:
    body_p = body_p +NL+'error : DB Connection failed. error_type - '+str(type(e))+'Err_desc - '+str(e.args)
    print(body_p)
    fm.f_email(to_add, from_add, Subject, body_p, mail_type, attachment) 
    sys.exit()

curs = conn.cursor()

run_id = curs.var(int)
curs.callproc('bmr_log.insert_process_run_dtl',[dt_tag,process_id,env,run_stat,run_id])




#---- STEP 1 : Establish connection with URL and fetch response---######

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 1 -- Fetch response','START',''])

rheaders = {"Content-Type":"application/json","Accept":"application/json"}

try:
    response = req.get(workday_url, auth= (wd_usr,wd_pwd), headers=rheaders)
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'Got some response','','response :' + str(response.status_code)])
except Exception as e:
    body_p = body_p +NL+'Got no response from URL - Failed - Come out. Error desc - '+str(e.args) 
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'Got no response from URL','Failed','Error desc - '+str(e.args)])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
    fm.f_email(to_add, from_add, Subject, body_p, mail_type, attachment) 
    print(body_p)
    sys.exit()

if response.status_code != 200:
    body_p = body_p +NL+'Error while fetching response from Workday URL' + str(response.status_code)   
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 1 -- Fetch response','Failed','response :' + str(response.status_code)])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
    fm.f_email(to_add, from_add, Subject, body_p, mail_type, attachment) 
    print(body_p)
    sys.exit()
    
else:
    rdata = json.loads(response.content)
    body_p = body_p +NL+'Success while fetching response from Workday URL. Response status - ' + str(response.status_code) 
    
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 1 -- Fetch response','START','response :' + str(response.status_code)])




#------ STEP 2 : Create DataFrame--- ######

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 2 -- Create DataFrame','START',''])

reordered_col = ['Employee_ID','firstName','lastName','Business_Unit','Cost_Center','Email','Worker_Type','location','Contingent_Worker_Supplier','Job_Title','businessTitle','Worker_s_Manager','Job_Profile','Time_Type','fte','Length_of_Service_in_Months']


try:
    df = pd.DataFrame.from_dict(rdata['Report_Entry'], orient='columns')
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 2 -- Create DataFrame','END',''])
    body_p = body_p +NL+'DataFrame creation completed'
except Exception as e:
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 2 -- Create DataFrame','FAILED','Error desc -'+str(e.args)])
    body_p = body_p +NL+'DataFrame creation failed'   
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
    fm.f_email(to_add, from_add, Subject, body_p, mail_type, attachment) 
    print(body_p)
    sys.exit()



#Validation #1 ------utf-8 validation---------check for long dash and convert to small dash 

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 3 -- Data Validation','START','utf-8 validation'])


for col in df.columns:
    df[col]=df[col].str.replace('–','-')
    
body_p = body_p +NL+'DataFrame validation completed'  


curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 3 -- Data Validation','END',''])


# -- Step 5 -- Data Processing ---------------###############

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 4 -- Data Processing','START',''])

#--- reorder columns ----#

df = df[reordered_col] 

#------Add creat_dt column----------#

df['DS_CREATE_DT'] = sysdate

df = df.astype({'DS_CREATE_DT' : np.datetime64})


#--- rename  columns as per DB ---##

renamed_col = ['EMPLOYEE_ID','FIRSTNAME','LASTNAME','BUSINESS_UNIT','COST_CENTER','EMAIL','WORKER_TYPE','LOCATION','CONTINGENT_WORKER_SUPPLIER','JOB_TITLE','BUSINESSTITLE','WORKER_S_MANAGER','JOB_PROFILE','TIME_TYPE','FTE','LENGTH_OF_SERVICE_IN_MONTHS','DS_CREATE_DT']
df.columns = renamed_col
body_p = body_p +NL+'DataFrame processing completed'  

#----Define Datatype for all the columns in DF----#

dtp = {'EMPLOYEE_ID' : types.VARCHAR ,'FIRSTNAME' : types.VARCHAR,'LASTNAME' : types.VARCHAR,'BUSINESS_UNIT' : types.VARCHAR,'COST_CENTER' : types.VARCHAR,'EMAIL' : types.VARCHAR,'WORKER_TYPE' : types.VARCHAR,'LOCATION' : types.VARCHAR,'CONTINGENT_WORKER_SUPPLIER' : types.VARCHAR,'JOB_TITLE' : types.VARCHAR,'BUSINESSTITLE' : types.VARCHAR,'WORKER_S_MANAGER' : types.VARCHAR,'JOB_PROFILE' : types.VARCHAR,'TIME_TYPE' : types.VARCHAR,'FTE' : types.Float ,'LENGTH_OF_SERVICE_IN_MONTHS' : types.Integer, 'DS_CREATE_DT' : types.DateTime}    

 
curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 4 -- Data Processing','END',''])



#------ STEP 2 : TRUNCATE Table before inserting new data --- ######

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5 -- TRUNCATE Table','START',''])

try:
    cursor = con_ddl.execute("TRUNCATE TABLE TMP_WORKDAY_SEI_SWP")
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5 -- TRUNCATE Table','END',''])
    body_p = body_p +NL+'Table truncated'
except Exception as e:
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5 -- TRUNCATE Table','FAILED','Error Desc - '+str(e.args)])
    body_p = body_p +NL+'Table truncate failed'
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
    fm.f_email(to_add, from_add, Subject, body_p, mail_type, attachment) 
    print(body_p)
    sys.exit()
           
cursor.close()


# --- Insert into DB ---#


curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 6 -- Data Insert','START',''])

try:
    df.to_sql(name='tmp_workday_sei_swp', con = con_ddl,  if_exists='append', index=False,dtype = dtp)   
    body_p = body_p +NL+'Data insertion completed'  
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 6 -- Data Insert','END',''])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'C'])
except Exception as ex:
    err_type = type(ex)
    err_desc = ex.args
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 6 -- Data Insert','Failed',err_desc[0]])
    body_p = body_p +NL+'Data insertion failed with error desc - ' +str(err_desc)
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
    fm.f_email(to_add, from_add, Subject, body_p, mail_type, attachment) 
    print(body_p)
    sys.exit()


print(body_p)

curs.close()

    