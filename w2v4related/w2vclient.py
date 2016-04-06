#!/usr/bin/env python
#_*_ coding:utf-8 _*_
'''
Created on 2015-1-23

@author: JohnDannl
'''
import sys
sys.path.append(r'..')
sys.path.append(r'../database')
import socket
from config import host,port
from database import tablemerge2,dbconfig
import time

def _getRelatedMids(title):    
    rec_buffer=[]    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, port))
        s.sendall(title)
        while True:
            data = s.recv(1024)
            if not data:
                break
            rec_buffer.append(data)
        s.close()
    except:
        print 'client socket error'
    finally:        
        s.close()
    #print 'Received', repr(''.join(rec_buffer))
    return ''.join(rec_buffer).split(',')

def getRelatedRecords(title):
    # default return 10 most related news if has enough
    mids=_getRelatedMids(title)
    records=[]
    for mid in mids:
        rows=tablemerge2.getRecordsById(dbconfig.mergetable,mid)
        if rows !=-1 and len(rows)>0:
            records.append(rows[0])
    return records

def getRelatedRecords2(title):  
    # This method is much faster than the above one
    # default return 10 most related news if has enough
    # but not keep the order of mid in the mids
    mids=_getRelatedMids(title)
    if len(mids)<=1:
        return []
    mids =['\''+mid+'\'' for mid in mids]
    mids='('+','.join(mids)+')'
    rows=tablemerge2.getRecordsByIds(dbconfig.mergetable,mids)  
    if rows==-1:  
        rows=[]
    return rows

if __name__=='__main__':
    oldtime=time.time()
    title='微信生态中企业应用的创新与创业机会'
    records=getRelatedRecords(title)
    for record in records:
        print record[3]
    print 'time cost:%s'%(str(time.time()-oldtime))