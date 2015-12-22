#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
Created on 2015-12-13

@author: dannl
'''
from database import tablemerge,tablerequest
from database.dbconfig import mergetable,requesttable
import time,logging

def trackUser(newsid,userid,userip):
    # newsid,userid,userip,requesttime
    requesttime=long(time.time())   
    data=(newsid,userid,userip,requesttime)
    tablerequest.InsertItem(requesttable, data)