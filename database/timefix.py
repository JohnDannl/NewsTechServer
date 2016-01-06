#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
Created on 2016-1-6

@author: dannl
'''
import db
from database.dbconfig import mergetable
dbconn = db.pgdb()
import dbconfig
import time
def getRecordsBugTime(tablename,ctime):
    query = "SELECT id,source FROM " + tablename+' where ctime > %s'
    rows = dbconn.Select(query, (ctime,))
    return rows

def updateCTimeById(tablename,iid,ctime):
    query ='update '+tablename+' set ctime =  %s where id = %s '
    rows=dbconn.Update(query, (ctime,iid))
    return rows

def getCTimeById(tablename,iid):
    query = 'select ctime from '+ tablename+' where id = %s '
    rows=dbconn.Select(query,(iid,))
    if rows!=-1:
        return rows[0][0]

def fixBugTime(tablename):
    threshold=long(time.time()+1000)
    rows=getRecordsBugTime(tablename, threshold)
    if rows==-1:
        return    
    for row in rows:
        oldtime=getCTimeById(tablename, row[0])
        newtime=oldtime-365*24*3600
        updateCTimeById(tablename, row[0], newtime)
            
if __name__=='__main__':    
#     oldtime=getCTimeById(dbconfig.tableName['sina'], 2)
#     print oldtime
#     print updateCTimeById(dbconfig.tableName['sina'], 2, oldtime+3600*24*365)
#     newtime=getCTimeById(dbconfig.tableName['sina'], 2)
#     print newtime
#     print getRecordsBugTime(dbconfig.tableName['sina'],long(time.time()+1000))
#     fixBugTime(dbconfig.tableName['sina'])
    print getRecordsBugTime(dbconfig.mergetable,long(time.time()+1000))

    