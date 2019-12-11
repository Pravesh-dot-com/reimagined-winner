# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 05:04:24 2019

@author: svcpe
"""

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib


def f_email ( to_addr, from_addr, subject, body, mail_type, attachment):
    msg = MIMEMultipart()    
    msg.attach(MIMEText(body, mail_type))
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = ', '.join(to_addr)
    #print(msg)
    s = smtplib.SMTP('smtp2.corp.seic.com')
    s.sendmail(from_addr,to_addr, msg.as_string())

    