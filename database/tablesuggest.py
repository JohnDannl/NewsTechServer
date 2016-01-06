#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
Created on 2016-1-6

@author: dannl
'''
import db
dbconn = db.pgdb()
import dbconfig

def insertItem(tablename, data): 
    query = """INSERT INTO """ + tablename + """(
               userid,email,suggestion,submitime)
               values(%s, %s, %s, %s)"""
    dbconn.Insert(query, (data))

def insertItemDict(tablename, data):
    query = "INSERT INTO " + tablename + """(
             userid,email,suggestion,submitime) 
             values(%(userid)s, %(email)s, %(suggestion)s, %(submitime)s)"""
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
    if rows!=-1:
        return rows[0][0]

def getTopRecords(tablename,topnum=10):
    query = 'Select * from '+ tablename +' order by submitime desc limit %s'
    rows = dbconn.Select(query,(topnum,))
    return rows
    
def updateUserInfoByUserId(tablename,newname,newemail,userid):
    query ='update '+tablename+' set name =  %s , email = %s where userid = %s '
    rows=dbconn.Update(query, (newname,newemail,userid))
    return rows

def ChkExistRow(tablename, userid):
    query = "SELECT COUNT(id) FROM " + tablename + " WHERE userid = %s "
    row = dbconn.Select(query, (userid,))[0][0]
    if row == 0:
        return False
    return True
                
def CreateTable(tablename):
    # for mysql text has a length 2^16 bytes limit,for postgresql text is unlimited
    # and for postgresql there is no performance effect among char(n),varchar(n),text
    query = """CREATE TABLE """ + tablename + """(
               id serial primary key, 
               userid varchar(255),
               email varchar(255),
               suggestion varchar(10240),
               submitime bigint)"""
    dbconn.CreateTable(query, tablename)
    dbconn.CreateIndex('create index on %s (userid)'%tablename)

def ReIndex(tablename):
    if dbconn.ReIndex(tablename)!=-1:
        print '%s reindex successfully!'%tablename
    else:
        print 'Failure:reindex %s'%tablename
    
if __name__ == "__main__":
    CreateTable(dbconfig.suggestable)