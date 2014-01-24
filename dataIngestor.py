#!/usr/bin/python 

#author: Rutvik Sheth

## imports 

import sys
import csv
import logging
import glob
from cassandra.cluster import Cluster
from collections import deque



def get_metadata(filename,filepath):
  try:
   with open (filepath,'rb') as csvfile:
    metareader=csv.reader(csvfile,delimiter='\t')
    for m in metareader:
     if m[0]==filename:
      print m[0]
      return m[1].split(",")
    return "NF" 
  except IOError:
   print "Could not open metadatafile"
   sys.exit(-1)


def get_last_row(csv_filename):
 with open(csv_filename,'rb') as f:
  valrowdequeobject=deque(csv.reader(f,delimiter='\t'),1)
  valrowstring=valrowdequeobject.popleft()[0]
  print valrowstring
  if "RECORD" in valrowstring:
   print "validator found", valrowstring
   return valrowstring
  else:
   return "NF"

def get_header(headerrow):
 header1=[]
 for word in headerrow:
  header1.append(word.replace(" ","_"))
 return header1      
 
def get_filename(filepath):
  return filepath.split("/")[-1]

def get_timestamp(timerow):
 timestamp=timerow[0].replace(" ","")
 timestamp=timestamp.replace(":","n")
 return timestamp 


def create_keyspace(session,keyspace):
 session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspace +" WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': 1}; ")
 

def use_keyspace(session,keyspace):
 session.execute("USE "+ keyspace+ ";")

def drop_table(session,tablename_complete):
 session.execute("DROP table IF EXISTS "+tablename_complete+";")  

def create_table(session,tablename_complete,header):
 session.execute("CREATE table IF NOT EXISTS "+tablename_complete+ "("+header[0]+" text PRIMARY KEY, "+header[1]+" text, "+header[2]+" text, "+ header[3]+" text, "+header[4]+" text);")




def cassandra_ingest(session,row,header,tablename_complete):
   ### try catch
   row1=[]
 
 
   for index,item in enumerate(row):
    if row[index]=="":
     row1.append("0.00")
    else:
     row1.append(item.replace("'",""))
#   print row1  
   session.execute("INSERT INTO "+tablename_complete+" ("+header[0]+","+header[1]+","+header[2]+","+header[3]+","+header[4]+") VALUES ('"+row1[0]+"', '"+row1[1]+"', '"+row1[2]+"', '"+row1[3]+"', '"+row1[4]+"');")


## main 

def main():

### declaration : use arguments later 


 dirpath="/home/shethru/cassandra/data/*.txt"

 keyspace="lipperkeyspace"
# tablename="bmkfundmeas"
 metadatafile="../metadata/metadata.txt" 
 
## countes the no of files processed and also used for making sure the use keyspace statement is not executed twice
 cql_counter=0
### connect to the cassandra cluster 

 logging.basicConfig(filename='missed_entries.log', level=logging.DEBUG)
 cluster=Cluster(['localhost'])
 session=cluster.connect()
 
### get list of files in cassandra data directory to upload 

 filenamelist=glob.glob(dirpath)
 print filenamelist
## for each file inthe directory ##
 for filepath in filenamelist:
  cql_counter=cql_counter+1
  filename=get_filename(filepath)
  print filename
  
# tablename needs to be created and csv_filename with path needs to be created
  tablename=filename.split(".")[0]
  csv_filename="../data/"+filename
  print csv_filename
 ## got the file we want to work with 
  

  try:
   with open (filepath,'rb') as csvfile:
    filereader=csv.reader(csvfile,delimiter='\t',quotechar='"')
    print filereader
    timerow=filereader.next()
    print timerow
  # convert to datetime
  # time=get_realtime()
    timestamp=get_timestamp(timerow)
    print timestamp
    tablename_complete=tablename+"_"+timestamp
    rowcounter=0
## add functionality for metadata spreadsheet to get header metadata
    header=get_metadata(filename,metadatafile)
    headerfromfile=get_header(filereader.next())
    
## if header from metadata is NF then take default as headerfromfile
    if header=="NF":
     header=headerfromfile

     
    print header 
    print headerfromfile


## compare header from metadata file with data file to make sure they match else kill 
    if set(header)!=set(headerfromfile):
     print "Wrong header: you probably have a mistake in the schmea stored in metadata file or are uploading the wrong file"
     sys.exit(-1)
    
## get the number of fields in the file
    headerlength=len(header)
     
## get the validator if it exists
  
    vcountrowstring=get_last_row(csv_filename)
    if vcountrowstring!="NF":
     vcount=int(vcountrowstring.rsplit("=")[1])
     print vcount
    else: 
      print "Warning: Expected validator row.Validator not found, will not be able to check data integrity."
     
# Execute session commands on cassandra to initialize keyspace and tables;
    create_keyspace(session,keyspace)
    print "created keyspace "
    if cql_counter==1:
     use_keyspace(session,keyspace)
     print "use keyspace"
    drop_table(session,tablename_complete)
    print "drop table"
    create_table(session,tablename_complete,header)
    print "create new table"
# now start the main file iteration 
  
    for row in filereader: 
     if len(row)==headerlength:
      rowcounter=rowcounter+1

      
      cassandra_ingest(session,row,header,tablename_complete)
     else:
      logging.debug(row)

##  print message if the validator matches/no match    
    if vcount and vcount!=rowcounter:
     print "Mismatch in file validator count and rows added to cassandra, rows missed:",int(vcount)-rowcounter
    else:
     print "Perfect validation!!! Job done for file ",filename 
     
  except IOError, AttributeError:
   print "Sorry there was an error in reading the file "+filename+", might want to check the path."
   sys.exit(-1) 


if __name__=='__main__':
 main()
