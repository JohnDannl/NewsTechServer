#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
Created on 2015-12-22

@author: dannl
'''
import time
import tornado.httpserver
import tornado.ioloop
from tornado.web import RequestHandler
import tornado.web
from database import tabledup, dbconfig
from database import tablevolunteer

class UserHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        userid=self.get_secure_cookie("userid", max_age_days=31)
        if userid:
            return tornado.escape.xhtml_escape(userid)

class WelcomeHandler(UserHandler):
    def get(self):
        if not self.current_user:
            self.redirect("/login/login")
            return
#        self.set_secure_cookie("userid", self.current_user)
        self.write('Hello,'+self.current_user)
        
class SigninHandler(UserHandler):
    '''The cookie data should contain 4 fields:
    <1> userid,<2> name,<3> email,<4> password '''
    def get(self):
        self.write('<html><body><form action="/signin" method="post">'
                   '<p>Name: <input type="text" name="name"></p>'
                   '<p>E-mail: <input type="text" name="email"></p>'
                   '<p>Password: <input type="text" name="password"></p>'
                   '<p><input type="submit" value="Sign in"></p>'
                   '</form></body></html>')
 
    def post(self):
        #name=self.get_argument('name')
        userid=self.get_argument("userid")
        name=self.get_argument('name', 'anonymous')
        email=self.get_argument('email')
        password=self.get_argument('password')
        if self.checkUserId(userid):                
            self.write('This device has been registered')           
        else:
            self.signin(userid,name,email,password)
            self.redirect("/login/login")
        
    def checkUserId(self,userid):
        return tablevolunteer.ChkExistRow(dbconfig.volunteertable, userid)
          
    def signin(self,userid,name,email,password):
        '''Note: default the authorization is true'''
        data=(userid,name,email,password,long(time.time()),True)
        tablevolunteer.insertItem(dbconfig.volunteertable, data)   
        
class LoginHandler(UserHandler):
    def get(self,call):
        if call =='changepassword':
            self.write('<html><body><form action="/login/changepassword" method="post">'
                   '<p>Name: <input type="text" name="name"></p>'
                   '<p>Password: <input type="text" name="password"></p>'
                   '<p>New password: <input type="text" name="newpassword"></p>'
                   '<p><input type="submit" value="Log in"></p>'
                   '</form></body></html>')
        else:
            self.write('<html><body><form action="/login/login" method="post">'
                       '<p>Name: <input type="text" name="name"></p>'
                       '<p>Password: <input type="text" name="password"></p>'
                       '<p><input type="submit" value="Log in"></p>'
                       '</form></body></html>') 
    def post(self,call):
        userid=self.get_argument('userid')
        name= self.get_argument('name')
        password=self.get_argument('password')        
        if self.checkUserId(userid):
            if self.checkUserPassword(userid, name, password):
                if not self.authorizeUser(userid):
                    self.write("No authorization")
                    return
                self.set_secure_cookie("userid", self.get_argument("userid"))
                #name=self.get_argument('name')
                if call =='changepassword':
                    newpassword=self.get_argument('newpassword')
                    self.changePassword(userid, newpassword)
                    self.write('Change password successfully')
                elif call =='changeinfo':
                    newname=self.get_argument('newname')
                    newemail=self.get_argument('newemail')
                    self.changUserInfo(userid,newname,newemail)
                    self.write('Change info successfully')
                else:
                    self.redirect("/welcome")
            else:
                self.write('User authorization failure')
        else:
            self.redirect('/signin')
    
    def checkUserId(self,userid):
        return tablevolunteer.ChkExistRow(dbconfig.volunteertable, userid)
    
    def checkUserPassword(self,userid,name,password):
        if tablevolunteer.getPasswordByUserId(dbconfig.volunteertable, userid) == password and \
        tablevolunteer.getNameByUserId(dbconfig.volunteertable, userid) == name:
            return True
        else:
            return False
        
    def authorizeUser(self,userid):
        return tablevolunteer.getAuthorizationByUserId(dbconfig.volunteertable, userid)            
        
    def changePassword(self,userid,newpassword):
        tablevolunteer.updatePasswordByUserId(dbconfig.volunteertable, newpassword, userid)   
             
    def changUserInfo(self,userid,newname,newemail):
        tablevolunteer.updateUserInfoByUserId(dbconfig.volunteertable, newname,newemail,userid)   
            
class DuplicateHandler(UserHandler):
    def get(self,call):
        if not self.current_user:
            self.redirect("/login/login")
            return
        newsid1=self.get_argument('newsid1')
        newsid2=self.get_argument('newsid2')
        userid=self.current_user
        data=(newsid1,newsid2,userid,long(time.time()))
        tabledup.insertItem(dbconfig.duptable, data)
        self.write('Submit duplication successfully')

