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

type_new,type_hot='newest','hot'
class NewsInfo(object):
    def __init__(self,mid,title,url,thumb,brief,source,ctime,wapurl,img,mtype,related,click):
        self.mid=mid
        self.title=title.decode('utf-8')
        self.url=url.decode('utf-8')
        self.thumb=thumb.decode('utf-8')
        self.brief=brief.decode('utf-8')    #jinja2.escape(brief)#.replace(r'&nbsp','')
        self.source=source.decode('utf-8')
        self.ctime=ctime   
        self.wapurl=wapurl.decode('utf-8')     
        self.img=img.decode('utf-8')
        self.mtype=mtype.decode('utf-8')
        self.related=related    # a list of related news info
        self.click=click

def _getClickInfosfromRecords(records):
    nInfos=[]     
    for item in records:
    #0id,1mtid,2url,3title,4newsid,5thumb,6summary,7keywords,8ctime,9commentid,
    #10type,11source,12wapurl,13img,14mid,15mtype,16related/16click        
    #14mid,3title,2url,5thumb,6brief,11source,8ctime,12wapurl,13img,15mtype,16related,16click            
        nInfos.append(NewsInfo(item[14],item[3],item[2],item[5],item[6],item[11],item[8],
                                item[12],item[13],item[15],[],item[16]))
    return nInfos
def _getInfosfromRecords(records):
    nInfos=[]
    for item in records:
    #0id,1mtid,2url,3title,4newsid,5thumb,6summary,7keywords,8ctime,9commentid,
    #10type,11source,12wapurl,13img,14mid,15mtype,16related/16click        
    #14mid,3title,2url,5thumb,6brief,11source,8ctime,12wapurl,13img,15mtype,16related,16click
        related=[]
        relmids=item[16].split(';;')
        for relmid in relmids:
            if not relmid:
                continue
            relitem=tablemerge.getRecordsByMid(mergetable,relmid)
            if not relitem:
                continue
            relitem=relitem[0] 
            related.append(NewsInfo(relitem[14],relitem[3],relitem[2],relitem[5],relitem[6],relitem[11],
                                    relitem[8],relitem[12],relitem[13],relitem[15],[],relitem[16]))                                
        nInfos.append(NewsInfo(item[14],item[3],item[2],item[5],item[6],item[11],item[8],
                                item[12],item[13],item[15],related,0)) 
    return nInfos
            
def getTopRecords(mid,ctime='0',topnum=10,mtype=None,click=0):
    nInfos=[]      
    if mtype == type_hot:
        records=tablemerge.getTopClickRecords(mergetable, topnum)
        if records!=-1 and len(records)>0:
            nInfos+=_getClickInfosfromRecords(records)
    else:        
        if mtype == type_new:
            records=tablemerge2.getTopRecords(mergetable2, topnum)    
        else:        
            records=tablemerge2.getTopRecords(mergetable2, topnum, mtype)
        if records!=-1 and len(records)>0:
            nInfos+=_getInfosfromRecords(records)            
    return nInfos

def getRefreshRecords(mid,ctime='0',topnum=10,mtype=None,click=0):
    nInfos=[] 
    if mtype == type_new:        
        records=tablemerge2.getBottomETBMRecords(mergetable2, ctime,mid,topnum)
    elif mtype == type_hot:
#         records=tablemerge2.getBottomECBVRecords(mergetable2, click, mid, topnum)
        records=[]
    else:        
        records=tablemerge2.getBottomETBMRecords(mergetable2, ctime,mid,topnum,mtype)
    if records!=-1 and len(records)>0:
        nInfos+=_getInfosfromRecords(records)
    if len(nInfos)<int(topnum):
        if mtype == type_new:
            records=tablemerge2.getBottomBTRecords(mergetable2, ctime,topnum-len(nInfos))
        elif mtype == type_hot:
            records=[]
        else:            
            records=tablemerge2.getBottomBTRecords(mergetable2, ctime,topnum-len(nInfos),mtype)
        if records!=-1 and len(records)>0:
            nInfos+=_getInfosfromRecords(records)
    return nInfos

def getMoreRecords(mid,ctime='0',topnum=10,mtype=None,click=0):
    nInfos=[]         
    if mtype == type_hot:
        records=tablemerge.getTopECSMRecords(mergetable, click, mid, topnum)
        nInfos+=_getClickInfosfromRecords(records)
    else:        
        if mtype == type_new:
            records=tablemerge2.getTopETSMRecords(mergetable2, ctime,mid,topnum)
        else:        
            records=tablemerge2.getTopETSMRecords(mergetable2, ctime,mid,topnum,mtype)
        if records!=-1 and len(records)>0:
            nInfos+=_getInfosfromRecords(records)
    if len(nInfos)<int(topnum):
        if mtype ==type_hot:
            records=tablemerge.getTopSCRecords(mergetable, click, topnum-len(nInfos))
            nInfos+=_getClickInfosfromRecords(records)
        else:
            if mtype == type_new:     
                records=tablemerge2.getTopSTRecords(mergetable2, ctime,topnum-len(nInfos))
            else:
                records=tablemerge2.getTopSTRecords(mergetable2, ctime,topnum-len(nInfos),mtype)
            if records!=-1 and len(records)>0:
                nInfos+=_getInfosfromRecords(records)
    return nInfos

# def getRelatedRecords(mid,ctime='0',topnum=10,mtype=None,click=0):
#     nInfos=[] 
#     rows=tablemerge2.getRecordsBySourceNewsid(mergetable2,mid)
#     if rows==-1 or len(rows)<1:
#         return nInfos
#     title=rows[0][0]   
#     records=related.getRelatedNewsList(title, 30,None,topnum+1)  # topnum+1 to exclude the news itself
#     if records!=None and len(records)>0:
#         for item in records:
#         #0id,1webid,2url,3title,4newsid,5thumb,6summary,7keywords,8ctime,9commentid,10type,
#         #11source,12wapurl,13img,14mid,15mtype,16click
#             # pass the same news
#             if item[4]==mid and item[11]==web:
#                 continue
#             nInfos.append(NewsInfo(item[4],item[3],item[2],item[5],item[6],item[11],item[8],
#                                     item[12],item[13],item[15],item[16]))
#     if len(nInfos)>topnum:
#         return nInfos[0:topnum]
#     return nInfos

def getSearchedRelated(mid,ctime='0',topnum=10,mtype=None,click=0):
    nInfos=[] 
    rows=tablemerge2.getRecordsByMid(mergetable2,mid)
    if rows==-1 or len(rows)<1:
        return nInfos
    title=rows[0][3]      
    print title  
    records=search.searchWithLimit(title,limit=topnum+1)
    if records!=None and len(records)>0:
        for item in records:
            if item['mid']==mid:
                continue
            #14mid,3title,2url,5thumb,6brief,11source,8ctime,12wapurl,13img,15mtype,16related,16click
            nInfos.append(NewsInfo(item['mid'],item['title'],item['url'],item['thumb'],item['summary'],
                                    item['source'],item['ctime'],item['wapurl'],item['img'],item['mtype'],
                                    item['related'],item['click']))
    if len(nInfos)>topnum:
        return nInfos[0:topnum]
    return nInfos

def getSearchedPage(keywords,pagenum=1):
    nInfos=[]     
#     print keywords
    records=search.searchWithPage(keywords,page=pagenum)
    if records!=None and len(records)>0:
        for item in records:
            #14mid,3title,2url,5thumb,6brief,11source,8ctime,12wapurl,13img,15mtype,16related,16click
            nInfos.append(NewsInfo(item['mid'],item['title'],item['url'],item['thumb'],item['summary'],
                                    item['source'],item['ctime'],item['wapurl'],item['img'],item['mtype'],
                                    item['related'],item['click']))
    return nInfos