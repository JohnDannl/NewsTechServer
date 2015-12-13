#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
Created on 2015-12-13

@author: dannl
'''
from database import tablemerge,tablerequest
from database.dbconfig import mergetable,requesttable
import time,logging

click_mod={'brief':'brief','detail':'detail'}

def trackUser(newsid,userid,userip,mode):
    # newsid,userid,userip,requesttime,clickmode
#     try:
    if mode in click_mod.values():
        tablemerge.increaseClick(mergetable,newsid)     
    requesttime=long(time.time())   
    data=(newsid,userid,userip,requesttime,mode)
    tablerequest.InsertItem(requesttable, data)
#     except:
#         logging.error('trackUser database visit error')