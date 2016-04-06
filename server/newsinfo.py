#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
Created on 2014-9-2

@author: JohnDannl
'''
import sys
sys.path.append(r'..')
sys.path.append(r'../database')

from database import tablemerge2,tablemerge
from database.dbconfig import mergetable2,mergetable
from search import search
from w2v4related import w2vclient

type_new,type_hot='newest','hot'
class NewsInfo(object):
    def __init__(self,newsid,title,url,thumb,brief,source,ctime,author,description,mtype,related,click):
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

def _getInfosfromMergeRecords(records):
    nInfos=[]     
    for item in records:
    #0id,1webid/mtid,2url,3title,4newsid,5thumb,6summary,7keywords,8ctime,9source,
    #10author,11description,12mtype,13click/related        
    #4newsid,3title,2url,5thumb,6brief,9source,8ctime,10author,11description,12mtype,13click/related
        nInfos.append(NewsInfo(item[4],item[3],item[2],item[5],item[6],item[9],item[8],
                                item[10],item[11],item[12],[],item[13]))
    return nInfos

def _getInfosfromMerge2Records(records):
    nInfos=[]
    for item in records:
    #0id,1webid/mtid,2url,3title,4newsid,5thumb,6summary,7keywords,8ctime,9source,
    #10author,11description,12mtype,13click/related        
    #4newsid,3title,2url,5thumb,6brief,9source,8ctime,10author,11description,12mtype,13click/related
        related=[]
        relnewsids=item[13].split(',')
        for relnewsid in relnewsids:
            if not relnewsid:
                continue
            relitem=tablemerge.getRecordsByNewsid(mergetable,relnewsid)
            if not relitem:
                continue
            relitem=relitem[0] 
            related.append(NewsInfo(relitem[4],relitem[3],relitem[2],relitem[5],relitem[6],relitem[9],
                                    relitem[8],relitem[10],relitem[11],relitem[12],[],relitem[13]))                                
        nInfos.append(NewsInfo(item[4],item[3],item[2],item[5],item[6],item[9],item[8],
                                item[10],item[11],item[12],related,0)) 
    return nInfos
            
def getTopRecords(newsid,ctime='0',topnum=10,mtype=None,click=0):
    nInfos=[]      
    if mtype == type_hot:
        records=tablemerge.getTopClickRecords(mergetable, topnum)
        if records!=-1 and len(records)>0:
            nInfos+=_getInfosfromMergeRecords(records)
    else:        
        if mtype == type_new:
            records=tablemerge2.getTopRecords(mergetable2, topnum)    
        else:        
            records=tablemerge2.getTopRecords(mergetable2, topnum, mtype)
        if records!=-1 and len(records)>0:
            nInfos+=_getInfosfromMerge2Records(records)            
    return nInfos

def getRefreshRecords(newsid,ctime='0',topnum=10,mtype=None,click=0):
    nInfos=[] 
    if mtype == type_new:        
        records=tablemerge2.getBottomETBNRecords(mergetable2, ctime,newsid,topnum)
    elif mtype == type_hot:
#         records=tablemerge2.getBottomECBVRecords(mergetable2, click, newsid, topnum)
        records=[]
    else:        
        records=tablemerge2.getBottomETBNRecords(mergetable2, ctime,newsid,topnum,mtype)
    if records!=-1 and len(records)>0:
        nInfos+=_getInfosfromMerge2Records(records)
    if len(nInfos)<int(topnum):
        if mtype == type_new:
            records=tablemerge2.getBottomBTRecords(mergetable2, ctime,topnum-len(nInfos))
        elif mtype == type_hot:
            records=[]
        else:            
            records=tablemerge2.getBottomBTRecords(mergetable2, ctime,topnum-len(nInfos),mtype)
        if records!=-1 and len(records)>0:
            nInfos+=_getInfosfromMerge2Records(records)
    return nInfos

def getMoreRecords(newsid,ctime='0',topnum=10,mtype=None,click=0):
    nInfos=[]         
    if mtype == type_hot:
        records=tablemerge.getTopECSNRecords(mergetable, click, newsid, topnum)
        nInfos+=_getInfosfromMergeRecords(records)
    else:        
        if mtype == type_new:
            records=tablemerge2.getTopETSNRecords(mergetable2, ctime,newsid,topnum)
        else:        
            records=tablemerge2.getTopETSNRecords(mergetable2, ctime,newsid,topnum,mtype)
        if records!=-1 and len(records)>0:
            nInfos+=_getInfosfromMerge2Records(records)
    if len(nInfos)<int(topnum):
        if mtype ==type_hot:
            records=tablemerge.getTopSCRecords(mergetable, click, topnum-len(nInfos))
            nInfos+=_getInfosfromMergeRecords(records)
        else:
            if mtype == type_new:     
                records=tablemerge2.getTopSTRecords(mergetable2, ctime,topnum-len(nInfos))
            else:
                records=tablemerge2.getTopSTRecords(mergetable2, ctime,topnum-len(nInfos),mtype)
            if records!=-1 and len(records)>0:
                nInfos+=_getInfosfromMerge2Records(records)
    return nInfos

def getRelatedRecords(newsid,ctime='0',topnum=10,mtype=None,click=0):
    vnInfos=getW2vRelated(newsid,ctime,topnum,mtype,click)
    if not vnInfos:  # if esa server is broken,then use searched related
        vnInfos=getSearchedRelated(newsid,ctime,topnum,mtype,click)  
        print 'using searched related'  
    return vnInfos

def getW2vRelated(newsid,ctime='0',topnum=10,mtype=None,click=0):
    nInfos=[] 
    rows=tablemerge2.getRecordsByNewsid(mergetable2,newsid)
    if rows==-1 or len(rows)<1:
        return nInfos
    title=rows[0][3]      
    print title  
    records=w2vclient.getRelatedRecords2(title)
    if records!=None and len(records)>0:
        for item in records:
        #0id,1webid/mtid,2url,3title,4newsid,5thumb,6summary,7keywords,8ctime,9source,
        #10author,11description,12mtype,13click/related        
        #4newsid,3title,2url,5thumb,6brief,9source,8ctime,10author,11description,12mtype,13click/related
            # pass the same news
            if item[4]==newsid:
                continue
            nInfos.append(NewsInfo(item[4],item[3],item[2],item[5],item[6],item[9],item[8],
                                item[10],item[11],item[12],[],0))
    if len(nInfos)>topnum:
        return nInfos[0:topnum]    
    return nInfos

def getSearchedRelated(newsid,ctime='0',topnum=10,mtype=None,click=0):
    nInfos=[] 
    rows=tablemerge2.getRecordsByNewsid(mergetable2,newsid)
    if rows==-1 or len(rows)<1:
        return nInfos
    title=rows[0][3]      
    print title  
    records=search.searchWithLimit(title,limit=topnum+1)
    if records!=None and len(records)>0:
        for item in records:
            if item['newsid']==newsid:
                continue
            #4newsid,3title,2url,5thumb,6brief,9source,8ctime,10author,11description,12mtype,13click/related
            nInfos.append(NewsInfo(item['newsid'],item['title'],item['url'],item['thumb'],item['summary'],
                                    item['source'],item['ctime'],item['author'],item['description'],item['mtype'],
                                    [],0))
    if len(nInfos)>topnum:
        return nInfos[0:topnum]
    return nInfos

def getSearchedPage(keywords,pagenum=1):
    nInfos=[]     
#     print keywords
    records=search.searchWithPage(keywords,page=pagenum)
    if records and len(records)>0:
        for item in records:
            #4newsid,3title,2url,5thumb,6brief,9source,8ctime,10author,11description,12mtype,13click/related
            nInfos.append(NewsInfo(item['newsid'],item['title'],item['url'],item['thumb'],item['summary'],
                                    item['source'],item['ctime'],item['author'],item['description'],item['mtype'],
                                    [],0))
    return nInfos