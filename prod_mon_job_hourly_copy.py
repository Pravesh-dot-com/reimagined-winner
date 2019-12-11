# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 17:19:38 2019

@author: pjoshi
"""

import datetime as dt
import cx_Oracle as ora
import tabulate as tb
import mailing_func as fm #this is common mailing function
import sys
from BMR_AVL_DBD_prop import db, log_file_path,  fromaddr , toaddr, toaddr_p 




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
        
     
"""        
    else:
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5','END','No Failed jobs and no NO_SHOW and no Warnings, as such no email for this hour.'])
        curs.callproc('bmr_log.update_process_run_dtl',[run_id,'C'])        
        print('mail not triggered as obvious')   
"""


#--------------------------------------------------------------
#---Function for creation of mail body and subject dynamically
#--------------------------------------------------------------

def get_mail_body(sql_result,type_e, header):
    
    global body_p
    global body
    global subs
        
    try:
        body_tbl  = tb.tabulate(sql_result,header, tablefmt = 'html', stralign = 'center')
    except:
        body_p = body_p +NL+ 'get_mail_body function failure - body_table can not be generated for : '+str(type_e)
        mail_invoke(mail_run_p)
                
    
    if type_e == 'ns':
        try:
            body = body+body_ns+body_div
            subs = subs + ' | No-Show | '
        except:
            body_p =  body_p +NL+'mail body can not be generated for : '+str(type_e)
            mail_invoke(mail_run_p)            
        
    elif type_e == 'f':
        try:
            body = body+body_f+body_div
            subs = subs + ' | Failed | '
        except:
            body_p = body_p +NL+'mail body can not be generated for : '+str(type_e)
            mail_invoke(mail_run_p)
        
    elif type_e == 's':
        try:
            body = body+body_s+body_div
            subs = subs + ' | Started | '
        except:
            body_p =body_p +NL+ 'mail body can not be generated for : '+str(type_e)
            mail_invoke(mail_run_p)
        
    elif type_e == 'w':
        try:
            body = body+body_w+body_div
            subs = subs + ' | Warning | '
        except:
            body_p = body_p +NL+ 'mail body can not be generated for : '+str(type_e)
            mail_invoke(mail_run_p)
        
    body = body.format(table = body_tbl)
    return body
    

####--------------------------------
#---------------MAIN-----------------
###---------------------------------
    

today = dt.datetime.today()
dt_tag = today.strftime('%d-%b-%Y')
dt_tm_tag = today.strftime('%d-%b-%Y %I %p')
dt_tm_tag_log = today.strftime('%d_%b_%Y_%I_%p')
mail_run_bus = 'A'
mail_run_p = 'P'
send_mail_bus = 'N'
NL = '\n'
body_p = ''
subs = ''    

body_div = """<br>
<br>
<div>-----------------------------------------------------------------------------------------------------------------------------------------------------------------</div>

<br>
<br>
<br>"""

body_start = """
<html><body><p>Hi All,</p>
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

fromaddr_p = "process_monitor_hourly_mail_alert_@seic.com"

header_f = ['PROCESS_ID','PROCESS_NAME','JOB_NAME','ENV_REF','FREQUENCY','JOB_STATUS', 'SCHEDULED_TIME','START_TM','END_TM']
sql_app_fail = "select process_id, process_name, job_name, env_ref, frequency, case when job_status = 'F' then 'Failure' end status, scheduled_time, START_TIME, end_time from (select s.process_id, p.process_name, j.job_name, s.env_ref, s.job_status, j.scheduled_time, s.START_TIME,s.end_time, (to_char(sysdate, 'HH24.MI') - to_char(s.START_TIME, 'HH24.MI'))  diff, s.frequency, s.as_of_date from stg_job_run_dtl s, t_job_details j, t_process p where s.job_status ='F' and s.process_id = p.process_id and s.job_id = j.job_id ) where ( diff < 1 and frequency = 'Hourly') or ( diff  < 1 and frequency not in  ('Hourly'))"

sql_no_show = "SELECT    job_name,    process_id,    process_name,    frequency,    scheduled_time,    scheduled_days_of_week,    scheduled_days_of_month FROM     (         SELECT             j.job_name,            j.process_id,            p.process_name,            j.frequency,            j.scheduled_time,            j.scheduled_days_of_week,            j.scheduled_days_of_month,            CASE                   WHEN TO_CHAR(SYSDATE,'DAY') = upper(j.scheduled_days_of_week) AND j.scheduled_days_of_month IS NULL THEN 'Y'                     WHEN j.scheduled_days_of_week IS NULL THEN 'Y'                    WHEN j.scheduled_days_of_month IS NOT NULL and ((select F_DAY_MON(j.scheduled_days_of_week,scheduled_days_of_month) from dual) = 'Y')                        THEN 'Y'                 END             no_show         FROM             t_job_details j,             t_process p         WHERE             j.active_flag = 'Y'             AND j.process_id = p.process_id             AND j.frequency <> 'Hourly'             AND length(j.scheduled_time) <= 12             AND TO_CHAR(SYSDATE,'hh24mi') > TO_CHAR(TO_DATE(substr(j.scheduled_time,1,8),'hh:mi am'),'hh24mi')             AND (                 SELECT                     TO_CHAR(SYSDATE,'hh24')                     || TO_CHAR(TO_DATE(substr(scheduled_time,1,8),'hh:mi am'),'mi')                 FROM                     t_job_details                 WHERE                     job_id = 501             ) > TO_CHAR(TO_DATE(substr(j.scheduled_time,1,8),'hh:mi am'),'hh24mi')             AND NOT EXISTS (                 SELECT                     'q'                 FROM                     stg_job_run_dtl s                 WHERE                     j.job_id = s.job_id             )             AND j.process_id <> 201     ) WHERE     no_show = 'Y'" 
header_ns= ['JOB_NAME','PROCESS_ID', 'PROCESS_NAME', 'FREQUENCY', 'SCHEDULED_TIME', 'SCHEDULED_DAYS_OF_WEEK','SCHEDULED_DAYS_OF_MONTH']

header_s = ['PROCESS_ID','PROCESS_NAME','JOB_NAME','ENV_REF','FREQUENCY','JOB_STATUS', 'SCHEDULED_TIME','START_TM','END_TM','ELAPSED_TIME(IN HOURS)']
sql_started = """select process_id, process_name, job_name, env_ref, frequency, job_status, scheduled_time, START_TIME, end_time,diff from (select s.process_id, p.process_name, j.job_name, s.env_ref, s.job_status, j.scheduled_time, s.START_TIME,s.end_time, (to_char(sysdate, 'HH24.MI') - to_char(s.START_TIME, 'HH24.MI'))  diff, s.frequency, s.as_of_date from stg_job_run_dtl s, t_job_details j, t_process p where s.job_status ='S' and s.process_id = p.process_id and s.job_id = j.job_id ) where ( diff between 3 and 4 and frequency = 'Hourly') or ( diff  between 1 and 2 and frequency not in  ('Hourly')) """


header_w = ['PROCESS_ID','PROCESS_NAME','ENV_REF','JOB_NAME','FREQUENCY','SCHEDULED_TIME','LAST_SUCCESS_RUN_DATE','LAST_RUN_STATUS','FINAL STATUS' ,'COMMENTS' ,'START_TM','END_TM','FAILED_RUNS' ,'COMPLETED_RUNS','STARTED_RUNS', 'EXPECTED_RUNS']
sql_warning = "select mon.PROCESS_ID,mon.PROCESS_NAME, mon.ENV_REF, mon.JOB_NAME, job.frequency, job.scheduled_time, mon.LAST_SUCCESS_RUN_DATE, mon.LAST_RUN_STATUS,  mon.STATUS, mon.COMMENTS,  mon.START_TM, mon.END_TM, mon.FAILED_RUNS ,mon.COMPLETED_RUNS, mon.STARTED_RUNS, mon.expected_runs from T_APP_MONITOR mon, t_job_details job where  mon.process_id <>201 and job.process_id = mon.process_id and job.job_name = mon.job_name and job.active_flag = 'Y' and mon.status='Warning' and trunc(mon.as_of_date)= trunc(sysdate) order by mon.START_TM"

process_id = 201
env = 501
run_stat = 'S'


try: 
    fcon = open(log_file_path['Hourly']+'_'+str(dt_tm_tag_log)+'.log','w'); saveout=sys.stdout;  sys.stdout=fcon
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

  
try:
    curs.execute(sql_app_fail);sql_result_failed = curs.fetchall()
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'failed sql ran','START',''])
    if len(sql_result_failed) > 0:
        body = get_mail_body(sql_result_failed,'f', header_f)
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'failed sql ran','END','Failed data present'])
        body_p = body_p +NL+'failed data present'
        send_mail_bus = 'Y'
    else:
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'failed sql ran','END','Failed data doesnt exist'])
        body_p = body_p +NL+'failed data doesnt exist'
      
except Exception as e:
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'failed sql failed','ERROR','Check PROCESS_ID - 201'])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
    body_p = body_p +NL+'failure sql failed - error_desc for failure sql : '+str(e.args)
    mail_invoke(mail_run_p)
    

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 1 -- No Show calculations','START',''])

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

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 1 -- No Show calculations','END',''])
     
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
    

body = body + body_close
    

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 3 -- Collect data from T_APP_MONITOR','END',''])

######----MAIL SUBJECT------############

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 4--create the Subject for the mail','START',''])


Subject = 'BMR Jobs - '+str(subs)+ ' Alert – ' + dt_tm_tag   
body_p = body_p +NL+'Subject Line created'    

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 4--create the Subject for the mail','END',''])    

##########------MERGE AND SEND----#######

curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5--Call the mailing function and send the mail','START',''])

if send_mail_bus == 'Y':
    
    body_p = body_p +NL+'Mail body data found. Preparing to send mail to business.'    
    try:
        mail_invoke(mail_run_bus)
        body_p = body_p +NL+'Outer mailing function invoked. Mail sent to business' 
        print(body_p)
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5-- Outer mailing function invoked. Mail sent to Business','END',''])
        curs.callproc('bmr_log.update_process_run_dtl',[run_id,'C'])  
        
    except Exception as e:
        body_p = body_p +NL+'Outer mailing functino failed. Could not send Mail to business. Error desc - '+ str(e.args) 
        print(body_p)
        mail_invoke(mail_run_p)
        curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5-- Outer mailing functino failed. Could not send Mail to Business','ERROR',''])
        curs.callproc('bmr_log.update_process_run_dtl',[run_id,'F'])
                
    
else:
    body_p = body_p +NL+'Mail body data not found. No Action required.'    
    curs.callproc('bmr_log.insert_process_run_log_dtl',[run_id,'STEP 5','END','No Failed jobs and no NO_SHOW and no Warnings, as such no email for this hour.'])
    curs.callproc('bmr_log.update_process_run_dtl',[run_id,'C'])        
    print(body_p)  
 

curs.close()
conn.close()  
    