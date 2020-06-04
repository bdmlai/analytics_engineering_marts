from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import mysql.connector
from datetime import datetime
from airflow.operators import BashOperator, SubDagOperator
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
import os
import sys
import slack
sys.path.append('/opt/config')
import  config
sc = slack.WebClient(token=config.token)


def send_success_email():
    sc.chat_postMessage(channel='#dbt-dev-comm',text="DBT_4B_REPORT_TABLE_INCR_PROCESS job executed successfully and data transfer is successfull ")
	
def send_validation_success_email(subject):
    sc.chat_postMessage(channel='#dbt-dev-comm',text="DBT_4B_REPORT_TABLE_INCR_PROCESS validation is successfull ")
	
def send_fail_email(subject):
    sc.chat_postMessage(channel='#dbt-dev-comm',text="DBT_4B_REPORT_TABLE_INCR_PROCESS job failed ")

def send_validation_fail_email(subject):
    sc.chat_postMessage(channel='#dbt-dev-comm',text="DBT_4B_REPORT_TABLE_INCR_PROCESS validation failed ")
	 
with DAG('DBT_4B_REPORT_TABLE_INCR_PROCESS',catchup=True, description='Python DAG', schedule_interval='0 13 * * *', start_date=datetime.strptime('2020-02-28T19:00:00Z', '%Y-%m-%dT%H:%M:%SZ')) as dag:
    DBT_4B_REPORT_TABLE_INCR_PROCESS = BashOperator(task_id='DBT_4B_REPORT_TABLE_INCR_PROCESS',bash_command='''cd ~/workspace/analytics_engineering_marts && dbt run --m rpt_nl_daily_wwe_program_ratings  rpt_nl_daily_wwe_live_quarterhour_ratings rpt_nl_daily_wwe_live_commercial_ratings --target uat_fds_nl''',on_success_callback=send_success_email,on_failure_callback=send_fail_email,dag=dag)
    DBT_4B_REPORT_TABLE_INCR_PROCESS