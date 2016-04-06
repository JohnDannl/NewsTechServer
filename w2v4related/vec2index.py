#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
Created on 2015-1-23

@author: JohnDannl
'''
from database import tablew2v,dbconfig
import doc2vec
import logging
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',level=logging.INFO)
from gensim import corpora,models,similarities
from config import ids_file,news_mm_file,index_file,index_prefix

class MyCorpus(object):
    def __init__(self,topnum):
        self.__topnum=topnum
        self.ids=[]
    def __iter__(self):
        rows=tablew2v.getTopRecords(dbconfig.w2vtable, self.__topnum)
        for row in rows:    # mid,vec
            self.ids.append(str(row[0]))
            yield doc2vec.parse_vec_str(row[1])
            
    def save_ids(self):
        fout=open(ids_file,'w')
        fout.write('\n'.join(self.ids))
        fout.close()
            
def index_latest(topnum=10000):
    vec_corpus=MyCorpus(topnum)
    corpora.MmCorpus.serialize(news_mm_file,vec_corpus ) # store to disk, for later use
    vec_corpus.save_ids()
    corpus=corpora.MmCorpus(news_mm_file) # now corpus has random access
    index=similarities.Similarity(index_prefix,corpus,num_features=doc2vec.model.layer1_size,num_best=10)
    index.save(index_file)