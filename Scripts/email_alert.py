#!/usr/bin/python
# -*- coding: utf-8 -*-


import json
import os,sys
from utils.utils import log_handler
from json import load
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import *
import sendgrid
from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition)
import base64


path=os.getcwd()
failure_file_path=path+"/Logs/Failure_logs.txt"


logger = log_handler(__name__)

def file_creation():
    try:
        if os.path.exists(failure_file_path):
            os.remove(failure_file_path)
        file = open(failure_file_path,"a+")
        print("file creation done")
        logger.info("file creation done")
        return file
            
    except OSError:
        print ("Creation of the directory %s failed" % path)
        logger.info("Creation of the directory %s failed" % path)

def notification():
    try:
        logger.info('Email notification method intilization started')
        with open(path+'/resources/Sendgrid_Conf.json', 'r') as f:
            eamil_cred = load(f)
            API_KEY = eamil_cred['API']
            from_email = eamil_cred['from_email']
            to_email = eamil_cred['to_email']
        logger.info('Sendgrid creds have been loaded'+" \n")
        message = Mail(
                        from_email=from_email,
                        to_emails=to_email,
                        subject='Facebook Ads Python Code Failure',
                        html_content="""\
                    <html>
                      <head></head>
                      <body>
                        Hi Team,
                        <br><br>
                        Please find the attached log file.
                        <br><br>
                        <br>
                        Regards,<br>
                        
                        <br>
                      </body>
                    </html>
                    """
                    )
        with open(failure_file_path,"rb") as f:
            data = f.read()
            f.close()
        encoded_file = base64.b64encode(data).decode()
        attachedFile = Attachment(
        FileContent(encoded_file),
        FileName('Failure_logs.txt'),
        FileType('application/txt'),
        Disposition('attachment')
                                )
        message.attachment = attachedFile
        if len(data) > 0:
            sg = SendGridAPIClient(api_key=API_KEY)
            response = sg.send(message)
            logger.info('Email has been sent successfully in case of any failure occured while running Facebokk ads code today'+"\n")
            print(response.status_code)
            return sg
        else:
            print('passing in case of no failure')
            pass      
        
    except Exception as e:
            logger.exception("Error in loading sendgrid creds or sending email, Please check access")
