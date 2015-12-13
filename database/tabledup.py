#!/usr/bin/env python
#_*_ coding:utf-8 _*_
'''
Created on 2015-12-13

@author: dannl
'''
import db
dbconn = db.pgdb()
import dbconfig

def _insertItem(tablename, newsid1,newsid2): 
    query = """INSERT INTO """ + tablename + """(
               newsid1,newsid2,fbcnt)
               values(%s, %s, %s)"""
    dbconn.Insert(query, (newsid1,newsid2,1))

def _insertItemDict(tablename, data):
    query = "INSERT INTO " + tablename + """(
             newsid1,newsid2,fbcnt) 
             values(%(newsid1)s,%(newsid2)s, %(fbcnt)s )"""
    dbconn.Insert(query, data)
    return 0

def getAllCount(tablename):
    query="select count(id) from "+tablename
    count=dbconn.Select(query,())[0][0]
    return count

def getAllRecords(tablename):
    query = "SELECT * FROM " + tablename
    rows = dbconn.Select(query, ())
    return rows

def getMaxId(tablename):
    query='select max(id) from '+tablename
    rows=dbconn.Select(query,())
    return rows[0][0]

def getRecordsById(tablename,webid):
    query = "SELECT * FROM " + tablename+' where id = %s'
    rows = dbconn.Select(query, (webid,))
    return rows

def _increaseFbcnt(tablename,newsid1,newsid2):        
    query = 'select fbcnt from '+tablename+""" WHERE newsid1 = %s and newsid2 = %s """
    rows=dbconn.Select(query,(newsid1,newsid2))
    if rows !=-1 and len(rows)>0:
        fbcnt=rows[0][0]
        fbcnt+=1
        query = "UPDATE " + tablename + """ SET fbcnt = %s WHERE newsid1 = %s and newsid2 = %s """
        dbconn.Update(query, (fbcnt,newsid1,newsid2))

def increaseFbcnt(tablename,newsid1,newsid2):  
    if newsid1==newsid2:
        return
    if newsid1 > newsid2:  # store the smaller one in newsid1
        tmp=newsid1
        newsid1=newsid2
        newsid2=tmp
    if ChkExistRow(tablename,newsid1,newsid2):
        _increaseFbcnt(tablename,newsid1,newsid2)
    else:
        _insertItem(tablename, newsid1,newsid2)
        
def ChkExistRow(tablename, newsid1,newsid2):
    query = "SELECT COUNT(id) FROM " + tablename + " WHERE newsid1 = %s and newsid2= %s "
    row = dbconn.Select(query, (newsid1,newsid2))[0][0]
    if row == 0:
        return False
    return True
                
def CreateNewsTable(tablename):
    # for mysql text has a length 2^16 bytes limit,for postgresql text is unlimited
    # and for postgresql there is no performance effect among char(n),varchar(n),text
    query = """CREATE TABLE """ + tablename + """(
               id serial primary key, 
               newsid1 varchar(255),         
               newsid2 varchar(255),                              
               fbcnt int)"""
    dbconn.CreateTable(query, tablename)
    dbconn.CreateIndex('create index on %s (newsid1,newsid2)'%tablename)

def ReIndex(tablename):
    if dbconn.ReIndex(tablename)!=-1:
        print '%s reindex successfully!'%tablename
    else:
        print 'Failure:reindex %s'%tablename
    
if __name__ == "__main__":
    CreateNewsTable(dbconfig.duptable)