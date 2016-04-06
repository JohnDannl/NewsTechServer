#!/usr/bin/env python
# -*- coding:utf-8 -*-
'''
Created on 2015-1-23

@author: JohnDannl
'''
from config import w2v_md_file
import jieba
import logging
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',level=logging.INFO)
from gensim import corpora,models,similarities
from gensim.models.word2vec import Word2Vec
import time
import scipy
from common.punckit import delpunc

model = Word2Vec.load(w2v_md_file)
model.init_sims(replace=True)

def _get_concept_vec(wordList):
    # wordList is a list of word:[word1,word2,...,wordn]
    total_vec=scipy.zeros(model.layer1_size)
    wordStr=' '.join(wordList)
    if isinstance(wordStr,unicode): # make sure word is utf-8 str type
        wordList=wordStr.encode('utf-8').split()
    for word in wordList:
        # make sure the word2vec model contain key 'word'        
        if model.vocab.has_key(word):
            total_vec+=model[word]
    return [(i,total_vec[i]) for i in xrange(model.layer1_size) if total_vec[i] !=0]

def get_vec(doc):
    wordList=delpunc(' '.join(jieba.cut(doc.lower()))).split()# make sure is a utf-8 str     
    return _get_concept_vec(wordList)

def get_vec_str(doc):
    items=[]
    for concept,weight in get_vec(doc):
        items.append(str(concept)+' '+str(weight))
    return ','.join(items)

def parse_vec_str(vec_str):    
    items=vec_str.split(',')
    vec=[]    
    try:
        for item in items:
            concept,weight=item.split()
            vec.append((int(concept),float(weight)))
    except:
        print 'empty vec_str or bad format'
    return vec

class MyCorpus(object):
    def __init__(self,file_name):
        self.__file_name=file_name
    def __iter__(self):
        for line in open(self.__file_name):
            line=line.split()     
#             yield _get_concept_vec_slope(line[1:])
            yield _get_concept_vec(line[1:])
            
if __name__=='__main__':
    a=get_vec_str('a')    
    print a
    print len(a)
    print parse_vec_str(a)
