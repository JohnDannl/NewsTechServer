#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
Created on 2014-11-4

@author: JohnDannl
'''
import search

def printRelatedNewsList(title,topnum=10):
    searchList=search.searchWithLimit(title,limit=topnum)
    if searchList==None or len(searchList)<1:
        return
    count=0
    for info in searchList:
        count+=1
        msg='%d. title=%s, ctime=%s，summary=%s'% (count,info['title'],info['ctime'],info['summary'])
        print msg

def printSearchNewsList(title,pagenum=1):
    searchList=search.searchWithPage(title,page=pagenum)
    if searchList==None or len(searchList)<1:
        return
    count=0
    for info in searchList:
        count+=1
        msg='%d. title=%s, ctime=%s，summary=%s'% (count,info['title'],info['ctime'],info['summary'])
        print msg

if __name__=='__main__':
    title='足球'
    resList=printRelatedNewsList(title)