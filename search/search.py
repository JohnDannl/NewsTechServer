#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
Created on 2014-10-29

@author: JohnDannl
'''
from coreseekapi import *
# from sphinxapi import *
import sys, time
import math


q = ''
# or SPH_MATCH_ALL or SPH_MATCH_ANY or SPH_MATCH_BOOLEAN or SPH_MATCH_EXTENDED
# mode = SPH_MATCH_ALL
mode= SPH_MATCH_ANY
# mode=SPH_MATCH_EXTENDED
host = 'localhost'
port = 9313
# index = '*'
index='newstech_merge2_index'
filtercol = 'group_id'
filtervals = []
# sortby=''
# sortby = '@weight DESC, @id desc'
sortby = '@weight DESC, ctime desc'
groupby = ''
groupsort = '@group desc'
limit = 0 # limit is default set to 20 unless 0<limit<16777216
weights = {"title":2, "summary":1}

client = SphinxClient()
client.SetServer ( host, port )
client.SetMatchMode ( mode )
client.SetFieldWeights(weights)
if filtervals:
    client.SetFilter ( filtercol, filtervals )
if groupby:
    client.SetGroupBy ( groupby, SPH_GROUPBY_ATTR, groupsort )
if sortby:    
    client.SetSortMode ( SPH_SORT_EXTENDED, sortby )
#     client.SetSortMode ( SPH_SORT_ATTR_DESC, sortby )

# SetFilterRange (attribute, min_, max_, exclude=0 )
def search(keys,limit=0):
    # limit is default to 20 when <=0 or >=16777216
    if limit:
        client.SetLimits ( 0, limit, max(limit,1000) )
#     client.SetFilterRange ('@id', 800, 1000)
#     client.SetFilterRange ('click', 0, 1000)
    res = client.Query ( keys, index )
        
    if not res:
        print 'query failed: %s' % client.GetLastError()
        sys.exit(1)
    
    if client.GetLastWarning():
        print 'WARNING: %s\n' % client.GetLastWarning()
    
    print 'Query \'%s\' retrieved %d of %d matches in %s sec' % (q, res['total'], res['total_found'], res['time'])
    print 'Query stats:'
    
    if res.has_key('words'):
        for info in res['words']:
            print '\t\'%s\' found %d times in %d documents' % (info['word'], info['hits'], info['docs'])
    
    if res.has_key('matches'):
#         match = { 'id':doc, 'weight':weight, 'attrs':{} }
        n = 1
        print '\nMatches:'
        for field in res['fields']:
            print field
        for match in res['matches']:
            attrsdump = ''
            for attr in res['attrs']:
                attrname = attr[0]
                attrtype = attr[1]
                value = match['attrs'][attrname]
                if attrtype==SPH_ATTR_TIMESTAMP:
                    value = value #time.strftime ( '%Y-%m-%d %H:%M:%S', time.localtime(value) )
                attrsdump = '%s, %s=%s' % ( attrsdump, attrname, value)
    
            print '%d. doc_id=%s, weight=%d%s' % (n, match['id'], match['weight'], attrsdump)
            n += 1
            
def searchWithLimit(keys,limit=0):
    # limit is default to 20 when <=0 or >=16777216
    if limit:
        client.SetLimits ( 0, limit, max(limit,1000) )
#     client.SetFilterRange ('@id', 800, 1000)
#     client.SetFilterRange ('click', 0, 1000)
    res = client.Query ( keys, index )
        
    if not res:
        print 'query failed: %s' % client.GetLastError()
        return   
    if res.has_key('matches'):
        resList=[]
        for match in res['matches']:
            infoDict ={}
#             infoDict['id']=match['id']
#             infoDict['weight']=match['weight']
            for attr in res['attrs']:
                attrname = attr[0]
                value = match['attrs'][attrname]
                infoDict[attrname]=value
            resList.append(infoDict)
        return resList
    
def searchWithPage(keys,page=1):
    assert isinstance(page, int),'page is not a integer'
    resList=searchWithLimit(keys,limit=500)
    total_found = len(resList)
    total_pages = math.ceil(total_found / 15.0)
    if page>total_pages:
        return None
    elif page == total_pages:
        return resList[(page-1)*15:]
    else:
        return resList[(page-1)*15:page*15]
     
if __name__=='__main__':
    keys='国民'
#     keys='2014'
#     keys='apple'
#     search(keys)
#     resList=searchWithLimit(keys,limit=20)
    resList=searchWithPage(keys,page=1)
    if not resList:
        print 'no search result'
    else:        
        n=0
        for res in resList:   
            n += 1     
            print '%d. newsid=%s,title=%s' % (n, res['newsid'],res['title'])
#     for res in resList:   
#         n += 1     
#         print '%d. doc_id=%s, weight=%d, title=%s' % (n, res['id'], res['weight'], res['title'])
        
            