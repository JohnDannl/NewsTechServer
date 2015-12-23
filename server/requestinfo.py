#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
Created on 2015-12-13

@author: dannl
'''
from database import tablemerge,tablemerge2,tablerequest
from database import dbconfig
from newsinfo import NewsInfo
import time,logging

class RequestInfo(object):
    def __init__(self,newsid,title,url,thumb,brief,source,ctime,author,description,mtype,related,click,requestime):
        self.newsid=newsid
        self.title=title.decode('utf-8')
        self.url=url.decode('utf-8')
        self.thumb=thumb.decode('utf-8')
        self.brief=brief.decode('utf-8')    #jinja2.escape(brief)#.replace(r'&nbsp','')
        self.source=source.decode('utf-8')
        self.ctime=ctime   
        self.author=author.decode('utf-8')     
        self.descripiton=description.decode('utf-8')
        self.mtype=mtype.decode('utf-8')
        self.related=related    # a list of related news info
        self.click=click
        self.requestime=requestime
        
def trackUser(newsid,userid,userip):
    # newsid,userid,userip,requesttime
    requesttime=long(time.time())   
    data=(newsid,userid,userip,requesttime)
    tablerequest.InsertItem(dbconfig.requestable, data)

def _getRequestInfosfromMerge2Record(record,requestime):
    #0id,1webid/mtid,2url,3title,4newsid,5thumb,6summary,7keywords,8ctime,9source,
    #10author,11description,12mtype,13click/related        
    #4newsid,3title,2url,5thumb,6brief,9source,8ctime,10author,11description,12mtype,13click/related
    related=[]
    relnewsids=record[13].split(',')
    for relnewsid in relnewsids:
        if not relnewsid:
            continue
        relrecord=tablemerge.getRecordsByNewsid(dbconfig.mergetable,relnewsid)
        if not relrecord:
            continue
        relrecord=relrecord[0] 
        related.append(NewsInfo(relrecord[4],relrecord[3],relrecord[2],relrecord[5],relrecord[6],relrecord[9],
                                relrecord[8],relrecord[10],relrecord[11],relrecord[12],[],relrecord[13]))                                
    return RequestInfo(record[4],record[3],record[2],record[5],record[6],record[9],record[8],
                            record[10],record[11],record[12],related,0,requestime)

def getHistoryInfo(userid,rqtime,topnum):
    rows=tablerequest.getUserTopNewsids(dbconfig.requestable, userid, topnum, rqtime)
    records=[]
    for newsid,requestime in rows:
        record=tablemerge2.getRecordsByNewsid(dbconfig.mergetable2, newsid)
        if not record:
            continue
        record=record[0]
        record=_getRequestInfosfromMerge2Record(record, requestime)
        records.append(record)
    return records