# -*- coding: utf-8 -*-
"""
@author: Yibing Chen

This program uses Twython to do a standard search of tweets related to NYCHA and store the Tweet text, ID, and user ID into SQL database.
"""

from twython import Twython, TwythonError
import mysql
import mysql.connector

class TwitterUser:
    def __init__(self,username,url,id):
        self.username=username
        self.url=url
        if self.url is None:
            self.url=""
        self.id=id

class TweetItem:
    def __init__(self, id, hashtags,bld,text, user, url, geo=None,coordinates=None,place=None):
        self.id=id
        self.hashtags=hashtags
        self.bld=bld
        self.text=text
        self.user=user
        self.url=url
        self.geo=geo
        self.coordinates = coordinates
        self.place=place
        
if __name__ == '__main__':
    ### Initialize Twython search tool ###
    mydb = mysql.connector.connect(
      host="name",
      user="name",
      passwd="password"
    )
    mycursor = mydb.cursor()
    t = Twython(app_key="", app_secret="")
 
    ### Query keywords ###
    q_list = ["NYCHA", "#NYCHA", "New York City Housing Authority", "New York public housing"]
    for i in range(len(q_list)):
        q=q_list[i]
        results = t.search(q=q,count=100,tweet_mode='extended')
        all_tweets = results["statuses"]
        #nextToken = results["next"]

        ### FILTERING ###
        
        for tweet in all_tweets:
            ### Collect Tweet ID, text, and user ID ###
            id=tweet['id_str']
            mycursor.execute("SELECT COUNT(*) FROM `nycha`.`tweets` WHERE `tweetid`="+id+";")
            count = mycursor.fetchone()
            if count==1:
                continue
            text = tweet['full_text']
            user = TwitterUser(tweet['user']['screen_name'],tweet['user']['url'],tweet['user']['id_str'])
            url='twitter.com/'+user.username+'/status/'+id
            tItem= TweetItem(id,[],"",text,user,url)   # Store data in SQL (The empty string is for NYCHA buildings in the next steps of the project)
            mycursor.execute("SELECT COUNT(*) FROM `nycha`.`users` WHERE `twitterid`="+user.id+";")
            countuser = mycursor.fetchone()
            
            ### Keep count of unique users. Only collect tweets from unique users to prevent data biases ###
            if countuser[0]==0:
                mycursor.execute('INSERT INTO `nycha`.`users` (`username`,`url`,`twitterid`) VALUES("'+user.username+'","'+user.url+'",'+user.id+');')
                mydb.commit()
            mycursor.execute("SELECT COUNT(*) FROM `nycha`.`tweets`;")
            
            count = 1+int(mycursor.fetchone()[0])
            mycursor.execute("SELECT COUNT(*) FROM `nycha`.`tweets` WHERE `tweetid`="+tItem.id+";")
            if(mycursor.fetchone()[0]==1):
                continue;
            mycursor.execute('INSERT INTO `nycha`.`tweets` (`text`,`tweetid`,`user`) VALUES('+'\''+tItem.text.replace("'",'"')+'\','+tItem.id+','+user.id+');')
            mydb.commit()
    
    mycursor.close()
    mydb.close()