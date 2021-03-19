import psycopg2
import csv
import glob
import os
import sys
sys.path.append('/opt/config')
import config


con=psycopg2.connect(dbname=config.DB_NAME, host=config.DB_HOST,port=5439,user=config.DB_USER, password=config.DB_PASS)
cur = con.cursor()

dirs_in_dir = []
location='/home/ec2-user/workspace/analytics_engineering_marts/ddl/deploy'
print(location)

fileset = [file for file in glob.glob(location + "**/*.sql", recursive=True)]

for file in fileset:
    f1 = open(file, "r")
    query=f1.read()
    sqlCommands = query.split(';')
    for i in sqlCommands:
        print(i)
        cur.execute(i)
        con.commit()
    f1.close()
cur.close()
