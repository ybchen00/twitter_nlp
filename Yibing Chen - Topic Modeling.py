# -*- coding: utf-8 -*-
"""
@author: Yibing Chen

This program takes in collected tweets, tokenizes them into words, and applies Latent Dirichlet Allocation model to extract topics of the corpus.
"""

import mysql
import mysql.connector
from gensim import models, corpora
import nltk
from nltk import pos_tag
from nltk.tokenize import RegexpTokenizer
import re


def nouns_adj_verb(text, tokenizer):
    ### Tokenize the text and pull out only the nouns, adjectives, and verbs ###
    is_noun_adj_verb = lambda pos: pos[:2] == 'JJ' or pos[:2] == 'JJR' or pos[:2] == 'JJS' or pos[:2] == 'NN' or pos[:2] == 'NNP' or pos[:2] == 'NNS' or pos[:2] == 'RB' or pos[:2] == 'RBR' or pos[:2] == 'RBS' or pos[:2] == 'VB' or pos[:2] == 'VBD' or pos[:2] == 'VBG' or pos[:2] == 'VBN' or pos[:2] == 'VBP' or pos[:2] == 'VBZ'
    tokenized = tokenizer.tokenize(text)
    noun_adj_verb = [word for (word, pos) in pos_tag(tokenized) if is_noun_adj_verb(pos)]
    return ' '.join(noun_adj_verb)

def nouns_adj(text, tokenizer):
    ### Tokenize the text and pull out only the nouns and adjectives ###
    is_noun_adj = lambda pos: pos[:2] == 'JJ' or pos[:2] == 'JJR' or pos[:2] == 'JJS' or pos[:2] == 'NN' or pos[:2] == 'NNP' or pos[:2] == 'NNS' or pos[:2] == 'RB' or pos[:2] == 'RBR' or pos[:2] == 'RBS'
    tokenized = tokenizer.tokenize(text)
    noun_adj_verb = [word for (word, pos) in pos_tag(tokenized) if is_noun_adj(pos)]
    return ' '.join(noun_adj_verb)

mydb = mysql.connector.connect(
  host="name",
  user="name",
  passwd="password"
)
mycursor = mydb.cursor()

mycursor.execute("SELECT `text`, `tweetid` FROM `nycha`.`tweets`;")
tweets = mycursor.fetchall()
tokenizer = RegexpTokenizer(r"\w+|\w+\'\w")  # Create a regular expression tokenizer that takes in words and words with apostrophes
data = []
for tup in tweets:
    ### Remove noise words ###
    text = tup[0]
    text = text.replace("RT ","")    # RT = Retweet. All retweeted tweets start with "RT" and should be removed.
    text = text.lower()
    text = text.replace("nycha", "")
    text = text.replace("#", "")
    text = text.replace("@","")
    text = text.replace(" is "," ")
    text = text.replace(" are "," ")
    text = text.replace(" have "," ")
    text = text.replace(" am "," ")
    text = text.replace("new york city" ,"")
    text = text.replace("new york" ,"")
    text = text.replace("nyc" ,"")
    text = text.replace("housing authority" ,"")
    text = text.replace("housing" ,"")
    
    url = re.findall('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+[/[\w]*]?', text)  # Remove urls 
    for i in url:
       if text.find(i) != -1:
          text= text.replace(i, "")
          
    ### Tokenize ###
    # Tokenizer only keeps adjectives, nouns, and verbs
    text = nouns_adj_verb(text, tokenizer)

    tokens = tokenizer.tokenize(text)
    data.append(tokens)
    

### Running tokenized data through LDA Model ###
for d in data:
    nltk.pos_tag(d)
num_topics = 6
dictionary = corpora.Dictionary(data)
corpus = []
for doc in data:
    corpus.append(dictionary.doc2bow(doc))

lda = models.LdaModel(corpus=corpus, num_topics=num_topics, id2word=dictionary, passes=80)
    

for idx in range(num_topics):
     print("Topic #%s:" % (idx+1), lda.print_topic(idx, 15))


            
        
