#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
Created on 2014-8-28

@author: JohnDannl
'''
import sys
sys.path.append(r'..')
# print sys.getdefaultencoding()
# reload(sys)
# sys.setdefaultencoding('utf-8')
# print sys.getdefaultencoding()
import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.options import define, options
from tornado.web import RequestHandler
import jinja2
from jinja2 import Environment, FileSystemLoader, TemplateNotFound
import os,logging,time
import newsinfo,requestinfo
from database import dbconfig
from volunteer import SigninHandler,LoginHandler,WelcomeHandler,DuplicateHandler

# port from 8889 ~ 9999
define ("port", default=8897, help="run on the given port", type=int)

class TemplateRendering:
    """
    A simple class to hold methods for rendering templates.
    """
    def render_template(self, template_name, **kwargs):
        template_dirs = [r'../templates']
        if self.settings.get('template_path', ''):
            template_dirs.append(
                self.settings["template_path"]
            )
        env = Environment(loader=FileSystemLoader(template_dirs),autoescape=True)

        try:
            template = env.get_template(template_name)
        except TemplateNotFound:
            raise TemplateNotFound(template_name)
        content = template.render(kwargs)
        return content

class BaseHandler(RequestHandler, TemplateRendering):
    """
    RequestHandler already has a `render()` method. I'm writing another
    method `render2()` and keeping the API almost same.
    """
    def render2(self, template_name, **kwargs):
        """
        This is for making some extra context variables available to
        the template
        """
        kwargs.update({
            'settings': self.settings,
            'STATIC_URL': self.settings.get('static_url_prefix', '../static'),
            'request': self.request,
            'xsrf_token': self.xsrf_token,
            'xsrf_form_html': self.xsrf_form_html,
        })
        content = self.render_template(template_name, **kwargs)
        self.write(content)

class NewsHandler(BaseHandler):
    """The handler to return the recent news records"""
    xtype_calls={r'top':newsinfo.getTopRecords,
           r'refresh':newsinfo.getRefreshRecords,
           r'more':newsinfo.getMoreRecords,
           r'related':newsinfo.getSearchedRelated }
#            r'related':newsinfo.getRelatedRecords
    def get(self, call):
#         print call
        topnum = str(self.get_argument('num', '10'))     
        newsid=str(self.get_argument('newsid', '0'))
        ctime=str(self.get_argument('ctime', '0'))
        mtype=str(self.get_argument('mtype', 'newest'))
        click=str(self.get_argument('click', '0'))
        print 'num:%s,newsid:%s,ctime:%s,mtype:%s,click:%s'%(topnum,newsid,ctime,mtype,click)
        records = self.getRecords(call,newsid,ctime,topnum,mtype,click)     
        #get thte user's ip addr
        self.set_header('Content-Type', 'application/xml')
        #print self.render_string('template.xml',source=source)
        print self.request.remote_ip
        self.render2('news.xml',records=records)

    def getRecords(self,call,newsid,ctime,topnum,mtype,click):
        records=[]
        try:
            # params check
            # top_num=int(unicode.decode(topnum).encode('utf-8'))
            top_num=int(topnum)
            click_num=int(click)
            if self.xtype_calls.has_key(call):
                resp=self.xtype_calls.get(call)
                records=resp(newsid,ctime,top_num,mtype,click_num)
        except:
            logging.info('NewsHandler getRecords error!')
        if not records:
            records=[]
        return records
#         return [VNInfo('movie1','This is movie1'),VNInfo('movie2','This is movie2')]

class SearchHandler(BaseHandler):
    """The handler to return the recent news records"""
    def get(self, call):
#         print call
        keywords=str(self.get_argument('keywords').encode('utf-8'))        
        pagenum = str(self.get_argument('page', '1'))  
        records = self.getRecords(keywords,pagenum)
        #get thte user's ip addr
        self.set_header('Content-Type', 'application/xml')
        #print self.render_string('template.xml',source=source)
        print self.request.remote_ip
        self.render2('news.xml',records=records)

    def getRecords(self,keywords,pagenum):
        records=[]        
        try:
            # params check
#                 top_num=int(unicode.decode(topnum).encode('utf-8'))
            page_num=int(pagenum)
            records=newsinfo.getSearchedPage(keywords, page_num)
        except:
            logging.info('SearchHandler getRecords error!')
        if not records:
            records=[]
        return records
        
class ClickHandler(RequestHandler):
    def get(self,call):
        newsid=self.get_argument('newsid')
        userid=str(self.get_argument('userid', 'anonymous'))        
        userip=str(self.request.remote_ip)
        requestinfo.trackUser(newsid, userid, userip)
        self.write('Success')
        
class UserHistoryHandler(BaseHandler):
    def get(self,call):
        userid=str(self.get_argument('userid', 'anonymous')) 
        rqtime=str(self.get_argument('requestime',long(time.time())))
        topnum = str(self.get_argument('num', '30'))    
        records=requestinfo.getHistoryInfo(userid, rqtime, topnum)
        self.render2('userhistory.xml',records=records)
        
if __name__ == "__main__":
    tornado.options.parse_command_line()
    settings = {
    "static_path": os.path.join(os.path.dirname(os.getcwd()), "static"),
    "cookie_secret": "61oETzKXQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
    "xsrf_cookies": False,# True will set an _xsrf cookie and include the same value 
                         # as a non-cookie field with all POST requests
    }
    app = tornado.web.Application(handlers=[(r"/news/(\w+)", NewsHandler), \
                                            (r"/search/(\w+)", SearchHandler), \
                                            (r"/click/(\w+)",ClickHandler),\
                                            (r"/signin", SigninHandler), \
                                            (r"/login/(\w+)", LoginHandler), \
                                            (r"/welcome", WelcomeHandler), \
                                            (r"/duplicate/(\w+)",DuplicateHandler),\
                                            (r"/history/(\w+)",UserHistoryHandler),\
                                            (r"/(favicon\.ico|\w+\.py)", tornado.web.StaticFileHandler, dict(path=settings['static_path']))], **settings)
    http_server = tornado.httpserver.HTTPServer(app, xheaders=True)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
