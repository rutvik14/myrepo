
#!/usr/bin/python

import logging 
import csv 
import sys
from cassandra.cluster import Cluster
from collections import deque

csv_filename="../data/bmkfundmeas.txt"
keyspacename="lipperkeyspace"
tablename="bmkfundmeas"

logging.basicConfig(filename='missed_entries.log', level=logging.DEBUG)
cluster=Cluster(['localhost'])
session=cluster.connect()

### take sys args

def get_last_row(csv_filename):
 with open(csv_filename,'rb') as f:
  return deque(csv.reader(f,delimiter='='),1)[0]


def schema_header(header):
 header1=[]
 for word in header:
  header1.append(word.replace(" ","_"))
  
 return header1 

def cassandra_ingest(row,header1):
 ### try catch
 #row1=[]
 #for i in range(len(row)):
 #  if row[i]!=None
 #  row1[i]=row[i]
 session.execute("INSERT INTO "+tablename_complete+" ("+header1[0]+","+header1[1]+","+header1[2]+","+header1[3]+","+header1[4]+") VALUES ('"+row[0]+"', '"+row[1]+"', '"+row[2]+"', '"+row[3]+"', '"+row[4]+"');")

 

with open (csv_filename, 'rb') as csvfile: 
 spamreader=csv.reader(csvfile,delimiter='\t')
 timestamp=spamreader.next()[0].replace(" ","")
 timestamp=timestamp.replace(":","n")
 header=spamreader.next()
 header1=schema_header(header)
 tablename_complete=tablename+"_"+timestamp

 print ",".join(header1) 
 print timestamp 
 ##### try catch exception  
 session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspacename +" WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': 1}; ")
 session.execute("use "+keyspacename+";")
 session.execute("DROP table IF EXISTS "+tablename_complete+";")
 session.execute("CREATE table IF NOT EXISTS "+tablename_complete+ "("+header1[0]+" text PRIMARY KEY, "+header1[1]+" text, "+header1[2]+" text, "+ header1[3]+" text, "+header1[4]+" text);")



 validator=', '.join(get_last_row(csv_filename))
 print validator
 for row in spamreader:
  if len(row) >= 5:
    cassandra_ingest(row,header1)
  else:
    logging.debug(row)
    

 

 cluster.shutdown()   
 

#if __name__=='__main__':
# main()
 
