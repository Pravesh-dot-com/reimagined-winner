# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 13:54:07 2019

@author: pjoshi
"""


import datetime as dt
import cx_Oracle as ora
import tabulate as tb
import mailing_func as fm #this is common mailing function
import sys
from BMR_AVL_DBD_prop import db, log_file_path, fromaddr , toaddr, toaddr_p 


######---invoke inner mailing function---#####

def mail_invoke(mail_par):
    
    mail_type = 'html'
    attachment = ''
    global body_p
    
    if mail_par == 'A':
        try:
            fm.f_email(toaddr, fromaddr, Subject, body, mail_type, attachment)
            curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5--Inner mailing function invoked','END',''])
            curs.callproc('bmr_log.update_process_run_dtl',[run_id,'C'])
            body_p = body_p +NL+'Inner Mailing function invoked'
            
        except Exception as e:
            curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5','ERROR','Inner Mailing function failed'])
            curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
            body_p = body_p +NL+'Inner Mailing function failed with error desc - '+str(e.args)
            mail_invoke(mail_run_p)
            
    elif mail_par == 'P':
        body_p = body_p +NL+'Error found. Check the logs carefully'
        fm.f_email(toaddr_p, fromaddr_p, 'Error in alert mail', body_p, mail_type, attachment)
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5--Email sent to self','job failed',''])
        curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
        
 
#--------------------------------------------------------------
#---Function for creation of mail body dynamically
#--------------------------------------------------------------

def get_mail_body(sql_result,type_e, header):
    
    global body_p
    global body
        
    try:
        body_tbl  = tb.tabulate(sql_result,header, tablefmt = 'html', stralign = 'center')
    except:
        body_p = body_p +NL+ 'get_mail_body function failure - body_table can not be generated for : '+str(type_e)
        mail_invoke(mail_run_p)
    
    if type_e == 'ns':
        try:
            body = body+body_ns+body_div
            
        except:
            body_p =  body_p +NL+'mail body can not be generated for : '+str(type_e)
            mail_invoke(mail_run_p)            
        
    elif type_e == 'f':
        try:
            body = body+body_f+body_div
            
        except:
            body_p = body_p +NL+'mail body can not be generated for : '+str(type_e)
            mail_invoke(mail_run_p)
        
    elif type_e == 's':
        try:
            body = body+body_s+body_div
            
        except:
            body_p =body_p +NL+ 'mail body can not be generated for : '+str(type_e)
            mail_invoke(mail_run_p)
        
    elif type_e == 'w':
        try:
            body = body+body_w+body_div
            
        except:
            body_p = body_p +NL+ 'mail body can not be generated for : '+str(type_e)
            mail_invoke(mail_run_p)
            
    elif type_e == 'lr':
        try:
            body = body+body_lr+body_div
            
        except:
            body_p = body_p +NL+ 'mail body can not be generated for : '+str(type_e)
            mail_invoke(mail_run_p)
            
    elif type_e == 'rest_15':
        try:
            body = body+body_rest_15+body_div
            
        except:
            body_p = body_p +NL+ 'mail body can not be generated for : '+str(type_e)
            mail_invoke(mail_run_p)            
        
    body = body.format(table = body_tbl)
    return body


#######################################################################################
###-----------------------------------------MAIN BODY--------------------------########
#######################################################################################


today = dt.datetime.today()
dt_tag = today.strftime('%d-%b-%Y')
dt_tag_log = today.strftime('%d_%b_%Y')
dt_tm_tag = today.strftime('%d-%b-%Y %I %p')
mail_run_bus = 'A'
mail_run_p = 'P'
send_mail_bus = 'N'
NL = '\n'
body_p = ''
fromaddr_p = "process_monitor_summary_mail_alert_@seic.com"

header_rest_15 = ['PROCESS_ID','PROCESS_NAME','ENV_REF','JOB_NAME','FREQUENCY','SCHEDULED_TIME','LAST_SUCCESS_RUN_DATE','LAST_RUN_STATUS','FINAL STATUS' ,'COMMENTS' ,'START_TM','END_TM','TIME TAKEN (In Min)','FAILED_RUNS' ,'COMPLETED_RUNS']
sql_rest_15 = "select mon.PROCESS_ID,mon.PROCESS_NAME, mon.ENV_REF, mon.JOB_NAME,job.frequency, job.scheduled_time,mon.LAST_SUCCESS_RUN_DATE, mon.LAST_RUN_STATUS,  mon.STATUS, mon.COMMENTS,  mon.START_TM, mon.END_TM, mon.time_taken_in_min , mon.FAILED_RUNS ,mon.COMPLETED_RUNS from T_APP_MONITOR mon, t_job_priority_map t , t_job_details job where substr(mon.status,1,1) = t.stat and mon.process_id <>201 and job.process_id = mon.process_id and job.job_name = mon.job_name and job.active_flag = 'Y' and mon.status <> 'Failure' and mon.time_taken_in_min<15 and trunc(mon.as_of_date)= trunc(sysdate) order by t.pri desc, mon.time_taken_in_min desc"

header_lr = ['PROCESS_ID','PROCESS_NAME','ENV_REF','JOB_NAME','FREQUENCY','SCHEDULED_TIME','LAST_SUCCESS_RUN_DATE','LAST_RUN_STATUS','FINAL STATUS' ,'COMMENTS' ,'START_TM','END_TM','TIME TAKEN (In Min)','FAILED_RUNS' ,'COMPLETED_RUNS']
sql_long_run = " select mon.PROCESS_ID,mon.PROCESS_NAME, mon.ENV_REF, mon.JOB_NAME, job.frequency, job.scheduled_time, mon.LAST_SUCCESS_RUN_DATE, mon.LAST_RUN_STATUS,  mon.STATUS, mon.COMMENTS,  mon.START_TM, mon.END_TM,mon.time_taken_in_min , mon.FAILED_RUNS ,mon.COMPLETED_RUNS from T_APP_MONITOR mon, t_job_details job where  mon.process_id <>201 and job.process_id = mon.process_id and job.job_name = mon.job_name and job.active_flag = 'Y' and trunc(mon.as_of_date)= trunc(sysdate) and  mon.time_taken_in_min >=15 order by mon.time_taken_in_min  desc"

header_ns= ['JOB_NAME','PROCESS_ID', 'PROCESS_NAME', 'FREQUENCY', 'SCHEDULED_TIME', 'SCHEDULED_DAYS_OF_WEEK','SCHEDULED_DAYS_OF_MONTH']
sql_no_show = "select job_name, process_id,process_name, frequency, scheduled_time, SCHEDULED_DAYS_OF_WEEK ,SCHEDULED_DAYS_OF_MONTH from ( select j.job_name, j.process_id, p.process_name, j.frequency, j.scheduled_time, j.SCHEDULED_DAYS_OF_WEEK ,j.SCHEDULED_DAYS_OF_MONTH, case     when to_char(sysdate,'DAY') = upper(j.SCHEDULED_DAYS_OF_WEEK) and j.SCHEDULED_DAYS_OF_MONTH is null     then 'Y'     when j.SCHEDULED_DAYS_OF_WEEK is null     then 'Y'     when to_char(sysdate,'DAY') = upper(j.SCHEDULED_DAYS_OF_WEEK) and              (select trunc(TRUNC(sysdate, 'Month'),'Day')+(7*(j.SCHEDULED_DAYS_OF_MONTH-1))+(select val from TEMP_map_days where day =j.SCHEDULED_DAYS_OF_WEEK) from dual)=trunc(sysdate)     then 'Y'     end no_show from  t_job_details j, t_process p where j.active_flag ='Y' and j.process_id=p.process_id and j.frequency <>'Hourly' and length(j.scheduled_time) <= 12 and to_char(sysdate,'hh24mi') >  to_char(to_date(substr(j.scheduled_time,1,8),'hh:mi am'),'hh24mi') and (select to_char(sysdate,'hh24')|| to_char(to_date(substr(scheduled_time,1,8),'hh:mi am'), 'mi') from t_job_details where job_id=501 ) > to_char(to_date(substr(j.scheduled_time,1,8),'hh:mi am'),'hh24mi') and not exists (select 'q' from stg_job_run_dtl s where j.job_id=s.job_id) and j.process_id <> 201 ) where no_show='Y'"

header_f = ['PROCESS_ID','PROCESS_NAME','ENV_REF','JOB_NAME','FREQUENCY','SCHEDULED_TIME','LAST_SUCCESS_RUN_DATE','LAST_RUN_STATUS','FINAL STATUS' ,'COMMENTS' ,'START_TM','END_TM','FAILED_RUNS' ,'COMPLETED_RUNS']
sql_failed = "select mon.PROCESS_ID,mon.PROCESS_NAME, mon.ENV_REF, mon.JOB_NAME, job.frequency, job.scheduled_time, mon.LAST_SUCCESS_RUN_DATE, mon.LAST_RUN_STATUS,  mon.STATUS, mon.COMMENTS,  mon.START_TM, mon.END_TM, mon.FAILED_RUNS ,mon.COMPLETED_RUNS from T_APP_MONITOR mon, t_job_details job where  mon.process_id <>201 and job.process_id = mon.process_id and job.job_name = mon.job_name and job.active_flag = 'Y' and mon.status='Failure' and trunc(mon.as_of_date)= trunc(sysdate) order by mon.START_TM"

header_s = ['PROCESS_ID','PROCESS_NAME','ENV_REF','JOB_NAME','FREQUENCY','SCHEDULED_TIME','LAST_SUCCESS_RUN_DATE','LAST_RUN_STATUS','FINAL STATUS' ,'COMMENTS' ,'START_TM','END_TM','FAILED_RUNS' ,'COMPLETED_RUNS','STARTED_RUNS']
sql_started = "select process_id, process_name, job_name, env_ref, frequency, job_status, scheduled_time, START_TIME, end_time,diff from (select s.process_id, p.process_name, j.job_name, s.env_ref, s.job_status, j.scheduled_time, s.START_TIME,s.end_time, (to_char(sysdate, 'HH24.MI') - to_char(s.START_TIME, 'HH24.MI'))  diff, s.frequency, s.as_of_date from stg_job_run_dtl s, t_job_details j, t_process p where s.job_status ='S' and s.process_id = p.process_id and s.job_id = j.job_id ) where ( diff>1 and frequency = 'Hourly') or ( diff >3 and frequency not in  ('Hourly'))"

header_w = ['PROCESS_ID','PROCESS_NAME','ENV_REF','JOB_NAME','FREQUENCY','SCHEDULED_TIME','LAST_SUCCESS_RUN_DATE','LAST_RUN_STATUS','FINAL STATUS' ,'COMMENTS' ,'START_TM','END_TM','FAILED_RUNS' ,'COMPLETED_RUNS','STARTED_RUNS', 'EXPECTED_RUNS']
sql_warning = "select mon.PROCESS_ID,mon.PROCESS_NAME, mon.ENV_REF, mon.JOB_NAME, job.frequency, job.scheduled_time, mon.LAST_SUCCESS_RUN_DATE, mon.LAST_RUN_STATUS,  mon.STATUS, mon.COMMENTS,  mon.START_TM, mon.END_TM, mon.FAILED_RUNS ,mon.COMPLETED_RUNS, mon.STARTED_RUNS, mon.expected_runs from T_APP_MONITOR mon, t_job_details job where  mon.process_id <>201 and job.process_id = mon.process_id and job.job_name = mon.job_name and job.active_flag = 'Y' and mon.status='Warning' and trunc(mon.as_of_date)= trunc(sysdate) order by mon.START_TM"

process_id = 201
env = 502
run_stat = 'S'

body_div = """<br>
<br>
<div>-----------------------------------------------------------------------------------------------------------------------------------------------------------------</div>

<br>
<br>
<br>"""

body_start = """
<html><body><p>Hi All,</p>
<p>Qlik BMR process Monitor dashboard link:<p>
<a>https://seigwps01ql01.gwp.seic.com/bmr/sense/app/f04b6f9b-5c94-48de-90cd-fd74326d386d/sheet/b4efa9b5-a956-4466-9c68-41ac340fcdc9/state/analysis</a>
<br>
<br>
<a>http://seigwps01ql01.gwp.seic.com/sense/app/f04b6f9b-5c94-48de-90cd-fd74326d386d/sheet/b4efa9b5-a956-4466-9c68-41ac340fcdc9/state/analysis</a>
<br>
"""

body = body_start

body_close = """
<p>Regards,</p>
<p>PE - Data Team</p>
</body></html>
"""

body_ns = """
<p>The below app loads did not run at the scheduled time today. Please check and take the necessary steps :</p>
{table}
"""

body_f = """
<p>Please find the status of all the failed app loads below :</p>
{table}
"""
body_s = """
<p>Please find all the Started app loads/jobs below :</p>
{table}
"""

body_w="""
<p>Please find all the App loads/jobs with Warnings below :</p>
{table}
"""

body_lr="""
<p>Please find all the app loads/jobs that are taking 15 or more min to complete :</p>
{table}
"""

body_rest_15="""
<p>Please find the status of all the app loads/jobs other than failed and taking less than 15 min:</p>
{table}
"""

try: 
    fcon = open(log_file_path['Daily']+'_'+str(dt_tag_log)+'.log','w'); saveout=sys.stdout;  sys.stdout=fcon
    body_p = body_p +NL+'Log file successfully created'
    
except Exception as e:
    body_p = body_p +NL+'error : logging in file could not be initiated. error_type - '+str(type(e))+'Err_desc - '+str(e.args)
    mail_invoke(mail_run_p)


###-----Establish connection with DB--############ GROUND ZERO

db_usr, db_pwd, db_host, db_port, db_svc = db['usr'], db['pwd'], db['host'], db['port'], db['svc']

try:
    
    conn = ora.connect(db_usr+'/'+str(db_pwd)+'@'+str(db_host)+':'+str(db_port)+'/'+str(db_svc))
    curs = conn.cursor()
    body_p = body_p +NL+'DB connection successful'
        
except:
    body_p = body_p +NL+'DB connection failed'
    mail_invoke(mail_run_p)
    
    

###-----Start logging into T_PROCESS_RUN Table----######    
    
run_id = curs.var(int)
curs.callproc('bmr_log.insert_process_run_dtl',[dt_tag,process_id,env,run_stat,run_id])
body_p = body_p +NL+'PROCESS_RUN_ID - '+ str(run_id)+' has been created'


###-----Call Procedure to populate the Target Table - T_APP_MONITOR--############    

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 2 -- Call P_APP_MONITOR','START',''])
        
try:
    curs.callproc('p_app_monitor')
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 2 -- Call P_APP_MONITOR','END',''])
    body_p = body_p +NL+'P_APP_MONITOR Procedure successfully invoked'
    
except Exception as e:
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 2 -- Call P_APP_MONITOR','ERROR','Check PROCESS_ID - 201'])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
    body_p = body_p +NL+'P_APP_MONITOR Procedure failed with error - '+str(e.args)
    mail_invoke(mail_run_p)


###-----Collect data from Target Table - T_APP_MONITOR--############

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 3 -- Collect data from T_APP_MONITOR','START',''])


#--Failure body---#

try:
    curs.execute(sql_failed); sql_result_failed = curs.fetchall()
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'failed sql ran','START',''])
    if len(sql_result_failed) > 0:
        body = get_mail_body(sql_result_failed,'f', header_f)
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'failed sql ran','END','failed data present'])
        body_p = body_p +NL+'failed data present'
        send_mail_bus = 'Y'
    
    else:
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'failed sql ran','END','failed data doesnt exist'])
        body_p = body_p +NL+'failed data doesnt exist'
      
except Exception as e:
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'failure sql failed','ERROR','Check PROCESS_ID - 201'])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
    body_p = body_p +NL+'failure sql failed - error_desc for failure sql : '+str(e.args)
    mail_invoke(mail_run_p)
  


curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 1 -- No Show Calculations','START',''])

#--NO_SHOW body---#

try:
    curs.execute(sql_no_show); sql_result_nos = curs.fetchall()
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'no_show sql ran','START',''])
    if len(sql_result_nos) > 0:
        body = get_mail_body(sql_result_nos,'ns', header_ns)
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'no_show sql ran','END','no_show data present'])
        body_p = body_p +NL+'no_show data present'
        send_mail_bus = 'Y'
    
    else:
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'no_show sql ran','END','no_show data doesnt exist'])
        body_p = body_p +NL+'no_show data doesnt exist'
      
except Exception as e:
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'no_show sql failed','ERROR','Check PROCESS_ID - 201'])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
    body_p = body_p +NL+'no_show sql failed - error_desc for no_show sql : '+str(e.args)
    mail_invoke(mail_run_p)
  

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 1 -- No Show Calculations','END',''])
   
#--Started body---#

try:
    curs.execute(sql_started); sql_result_started = curs.fetchall()
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'started sql ran','START',''])
    if len(sql_result_started) > 0:
        body = get_mail_body(sql_result_started,'s', header_s)
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'started sql ran','END','started data present'])
        body_p = body_p +NL+'started data present'
        send_mail_bus = 'Y'
    else:
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'started sql ran','END','started data doesnt exist'])
        body_p = body_p +NL+'started data doesnt exist'
        
except Exception as e:
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'started sql failed','ERROR','Check PROCESS_ID - 201'])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
    body_p = body_p +NL+'started sql failed - error_desc for started sql : '+str(e.args)
    mail_invoke(mail_run_p)
   

#--Warning body---#
    
try:
    curs.execute(sql_warning); sql_result_warning = curs.fetchall()
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'warning sql ran','START',''])
    if len(sql_result_warning) > 0:
        body = get_mail_body(sql_result_warning,'w', header_w)
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'warning sql ran','END','warning data present'])
        body_p = body_p +NL+'warning data present'
        send_mail_bus = 'Y'
    else:
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'warning sql ran','END','warning data doesnt exist'])
        body_p = body_p +NL+'warning data doesnt exist'
        
except Exception as e:
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'warning sql failed','ERROR','Check PROCESS_ID - 201'])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
    body_p = body_p +NL+'warning sql failed - error_desc for warning sql : '+str(e.args)
    mail_invoke(mail_run_p)
  
    
#--Long_run body---#
    
try:
    curs.execute(sql_long_run); sql_result_long_run = curs.fetchall()
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'long_run sql ran','START',''])
    if len(sql_result_long_run) > 0:
        body = get_mail_body(sql_result_long_run,'lr', header_lr)
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'long_run sql ran','END','long_run data present'])
        body_p = body_p +NL+'long_run data present'
        send_mail_bus = 'Y'
    else:
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'long_run sql ran','END','long_run data doesnt exist'])
        body_p = body_p +NL+'long_run data doesnt exist'
        
except Exception as e:
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'long_run sql failed','ERROR','Check PROCESS_ID - 201'])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
    body_p = body_p +NL+'long_run sql failed - error_desc for long_run sql : '+str(e.args)
    mail_invoke(mail_run_p)
 

#--rest_15 body---#    

try:
    curs.execute(sql_rest_15); sql_result_15 = curs.fetchall()
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'rest_15 sql ran','START',''])
    if len(sql_result_15) > 0:
        body = get_mail_body(sql_result_15,'rest_15', header_rest_15)
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'rest_15 sql ran','END','rest_15 data present'])
        body_p = body_p +NL+'rest_15 data present'
        send_mail_bus = 'Y'
    else:
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'rest_15 sql ran','END','rest_15 data doesnt exist'])
        body_p = body_p +NL+'rest_15 data doesnt exist'
        
except Exception as e:
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'rest_15 sql failed','ERROR','Check PROCESS_ID - 201'])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
    body_p = body_p +NL+'rest_15 sql failed - error_desc for rest_15 sql : '+str(e.args)
    mail_invoke(mail_run_p)
  
body = body + body_close

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 3 -- Collect data from T_APP_MONITOR','END',''])

 
######----MAIL SUBJECT------############


curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 4--create the Subject for the mail','START',''])

Subject = 'BMR Jobs - Daily Status Report - '+str(dt_tag)
body_p = body_p +NL+'Subject Line created'    

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 4--create the Subject for the mail','END',''])    


##########------MERGE AND SEND----#######


curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5--Call the mailing function and send the mail','START',''])
body_p = body_p +NL+'Preparing to send mail to business.'    

try:
    mail_invoke(mail_run_bus)
    body_p = body_p +NL+'Outer mailing function invoked. Mail sent to business' 
    print(body_p)
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5-- Outer mailing function invoked. Mail sent to Business','END',''])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'C'])  
    
except Exception as e:
    body_p = body_p +NL+'Outer mailing function failed. Could not send Mail to business. Error desc - '+ str(e.args) 
    print(body_p)
    mail_invoke(mail_run_p)
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5-- Outer mailing functino failed. Could not send Mail to Business','ERROR',''])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
            

curs.close()
conn.close()  


#---------------------------------------
