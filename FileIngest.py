
#!/usr/bin/python 

#author: Rutvik Sheth

## imports 

import sys
import csv
import logging
import glob
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from collections import deque



## changed method for get_metadata()

def get_metadata(filename,filepath):
  try:
   with open (filepath,'rb') as csvfile:
    metareader=csv.reader(csvfile,delimiter='\t')
    for m in metareader:
     if m[0]==filename:
      print "filename: ", m
      return m
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

## creating composite primary key needs to be changed
def create_primarykey(row_clean,header,composite_keylist):
 pk=""
 for i,r in enumerate(row_clean):
  for c in composite_keylist:
   if header[i].lower()==c.lower():
    pk=pk+r.replace(" ","")
    pk=pk.replace(":","")
    pk=pk.replace(";","")
    pk=pk.replace("/","")
  i=i+1 
     
 return pk


def get_schemaheader(filemetadata):
 return filemetadata[1].split(",")

def get_flag(filemetadata):
 return filemetadata[2]

def get_compositekeyschema(filemetadata):
 print filemetadata[3]
 return filemetadata[3].split(",")
 
def create_keyspace(session,keyspace):
 session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspace +" WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': 1}; ")
 

def use_keyspace(session,keyspace):
 session.execute("USE "+ keyspace+ ";")

def drop_table(session,tablename_complete):
 session.execute("DROP table IF EXISTS "+tablename_complete+";")  

def create_table(session,tablename_complete,header,composite_flag,composite_keylist):
 
## dynamic creation of the header ..
 print "entering create table"
# primary_key=create_primarykey(header)
 schema1="CREATE table IF NOT EXISTS "+tablename_complete+" ("
 schema2=" , "
 schema3=" text PRIMARY KEY "
 schema4=" text "
 schema5=" );"
 
 composite_key=""

# chk if PK is composite or not and act accordingly
 if composite_flag==0:

  primary_keyhead=composite_keylist[0]
  schema=schema1+primary_keyhead+schema3
  for i,h in enumerate(header):
   if h==primary_keyhead:
    i=i+1
    continue
   elif i==len(header)-1:
     schema=schema+schema2+h+schema4+schema5
   else:
    schema=schema+schema2+h+schema4
 elif composite_flag==1:

  primary_keyhead="composite_PK"
  schema=schema1+primary_keyhead+schema3
  for i,h in enumerate(header):
   if i==len(header)-1:
    schema=schema+schema2+h+schema4+schema5
    i=i+1
   else:
    schema=schema+schema2+h+schema4 
 else:
   print "somethingwrong in composite flag"

 session.execute(schema)  




def clean_row(row):
 row1=[]
 for column in row:
  if column=="":
   row1.append("NF")
  else:
   row1.append(column.replace("'",""))
 return row1 

def cassandra_ingest(session,row,header,tablename_complete,composite_flag,composite_keylist ):

 schema1="INSERT INTO "+tablename_complete+" ("
 schema2=" , "
 schema3=" ) "
 schema4="'"
 schema5=");"
 
 schemah=schema1
 schemar=" ('"
 schemarr=""
 i=0

## clean the row for adding NF and removing ' in data

 row_clean=clean_row(row)
 if composite_flag==0:
  for i,r in enumerate(row_clean):
   if len(header)==1:
    schemah=schemah+header[i]+schema3
    schemar=schemar+r+schema4+schema5
    i=i+1
   elif i==len(header)-1: 
    schemah=schemah+schema2+header[i]+schema3
    schemar=schemar+schema2+schema4+r+schema4+schema5
    i=i+1
   
   elif i==0:
    schemah=schemah+header[i]
    schemar=schemar+r+schema4
   else:
    schemah=schemah+schema2+header[i]
    schemar=schemar+schema2+schema4+r+schema4
    i=i+1
  
  
  schemacfO=schemah+" VALUES "+schemar 
  session.execute(schemacfO)
 
## case where CF=1 ** this is the case where a composite pk  needs to be created 

 elif composite_flag==1:
  schemah=schemah+"composite_pk"+schema2
  
  for i,r in enumerate(row_clean):
   if len(header)==1:
    print "Something went wrong here: getting composite key for a single columnar file!! not possible"
    sys.exit(-1) 
   elif i==len(header)-1:
    schemah=schemah+schema2+header[i]+schema3
    schemar=schemar+schema2+schema4+r+schema4+schema5
    i=i+1
   elif i==0:
    schemah=schemah+header[i]
    schemar=schemar+create_primarykey(row_clean,header,composite_keylist)+schema4+schema2+schema4+r+schema4
    i=i+1
   else:
    schemah=schemah+schema2+header[i]
    schemar=schemar+schema2+schema4+r+schema4
    i=i+1
  schemacf1=schemah+" VALUES "+schemar
  session.execute(schemacf1)
  
 else:
  print "error with composite flag"
  sys.exit(-1)   

###   session.execute("INSERT INTO "+tablename_complete+" ("+header[0]+","+header[1]+","+header[2]+","+header[3]+","+header[4]+") VALUES ('"+row[0]+"', '"+row[1]+"', '"+row[2]+"', '"+row[3]+"', '"+row[4]+"');")
   

## main 

def main():

### declaration : use arguments later 


 dirpath="/home/shethru/cassandra/data/*.txt"

 keyspace="lipperkeyspace"
# tablename="bmkfundmeas"
 metadatafile="../metadata/metadata1.txt" 

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
  header=""
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
    filemetadata=get_metadata(filename,metadatafile)
    headerfromfile=get_header(filereader.next())
    print filemetadata
## if header from metadata is NF then take default as headerfromfile
    if filemetadata=="NF":
     header=headerfromfile

## adding extra code:  
    else:
     header=get_schemaheader(filemetadata)
     composite_keylist=get_compositekeyschema(filemetadata)
     composite_flag=int(get_flag(filemetadata))
    print "header from metadata"
    print header 
    print "header from file itself"
    print headerfromfile
    
    print "composite keylist: ", composite_keylist
    print "File has primary key or need to create one? ", composite_flag

    
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
    print session, tablename_complete, header, composite_flag,composite_keylist
    create_table(session,tablename_complete,header,composite_flag,composite_keylist)
    print "create new table"
# now start the main file iteration 
    i=0
    for row in filereader: 
     if len(row)==headerlength:
      rowcounter=rowcounter+1

      
      cassandra_ingest(session,row,header,tablename_complete, composite_flag, composite_keylist)
      i=i+1
      print i
     else:
      logging.debug(row)

##  print message if the validator matches/no match    
    if vcount and vcount!=rowcounter:
     print "Mismatch in file validator count and rows added to cassandra, rows missed:",int(vcount)-rowcounter
    else:
     print "Perfect validation!!! Job done for file ",filename 
   csvfile.close()
  
  except Exception,e:
   print "Sorry there was an error in reading the file "+filename+", might want to check the path."
   print e
   print "current value %s" % socket
   raise
   sys.exit(-1) 
  
 cluster.shutdown()
  
if __name__=='__main__':
 main()
