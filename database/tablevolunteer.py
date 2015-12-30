#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
Created on 2015-12-21

@author: dannl
'''
import db
dbconn = db.pgdb()
import dbconfig

def insertItem(tablename, data): 
    if ChkExistRow(tablename, data[0]):
        return
    query = """INSERT INTO """ + tablename + """(
               userid,name,email,password,registertime,auth)
               values(%s, %s, %s, %s, %s, %s)"""
    dbconn.Insert(query, (data))

def insertItemDict(tablename, data):
    query = "INSERT INTO " + tablename + """(
             userid,name,email,password,registertime,auth) 
             values(%(userid)s, %(name)s, %(email)s, %(password)s, %(registertime)s, %(auth)s )"""
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

def getPasswordByUserId(tablename,userid):
    query = 'Select password from '+ tablename +' where userid = %s '
    rows = dbconn.Select(query,(userid,))
    if rows!=-1:
        return rows[0][0]
    
def setAuthorizationByUserId(tablename,userid,auth):
    query = 'Update '+tablename+' set auth = %s where userid = %s '
    rows=dbconn.Update(query, (auth,userid))
    return rows 

def getAuthorizationByUserId(tablename,userid):
    query = 'Select auth from '+ tablename +' where userid = %s '
    rows = dbconn.Select(query,(userid,))
    if rows!=-1:
        return rows[0][0]
    
def getNameByUserId(tablename,userid):
    query = 'Select name from '+ tablename +' where userid = %s '
    rows = dbconn.Select(query,(userid,))
    if rows!=-1:
        return rows[0][0]
    
def updatePasswordByUserId(tablename,newpassword,userid):
    query ='update '+tablename+' set password =  %s where userid = %s '
    rows=dbconn.Update(query, (newpassword,userid))
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
                
def CreateNewsTable(tablename):
    # for mysql text has a length 2^16 bytes limit,for postgresql text is unlimited
    # and for postgresql there is no performance effect among char(n),varchar(n),text
    query = """CREATE TABLE """ + tablename + """(
               id serial primary key, 
               userid varchar(255),         
               name varchar(255), 
               email varchar(255),
               password varchar(255),
               registertime bigint,
               auth boolean)"""
    dbconn.CreateTable(query, tablename)
    dbconn.CreateIndex('create index on %s (userid)'%tablename)

def ReIndex(tablename):
    if dbconn.ReIndex(tablename)!=-1:
        print '%s reindex successfully!'%tablename
    else:
        print 'Failure:reindex %s'%tablename
    
if __name__ == "__main__":
    CreateNewsTable(dbconfig.volunteertable)