# -*- encoding: utf-8 -*-
from __future__ import unicode_literals
from datetime import date as dt, timedelta
from typing import List
from google.cloud import firestore
from functools import reduce
from newsapi import NewsApiClient
import time
import base64
import sys
import math 
import hashlib
import config

DB = firestore.Client(project="covid-news-291320")

KEYWORDS = [ "corona", "covid", "pandemia", "SARS"]

class DateController:
	DATE = dt.today()

	@staticmethod
	def setDate(newDate):
		DATE = newDate
	

class Article():
	def __init__(self, articleId, author, category, description, publishedAt, title, url, urlToImage):
		self.articleId = articleId
		self.author = author
		self.category = category
		self.description = description
		self.publishedAt = publishedAt
		self.title = title
		self.url = url
		self.urlToImage = urlToImage

	def to_dict(self):
		return {
			u'articleId': self.articleId,
			u'author': self.author,
			u'category': self.category,
			u'description': self.description,
			u'publishedAt': self.publishedAt,
			u'title': self.title,
			u'url': self.url,
			u'urlToImage': self.urlToImage
		}	
	
	def __repr__(self):
		return(u'Article(articleId={}, author={}, category={}, description={}, publishedAt={},title={},url={},urlToImage={})'
            .format(self.articleId, self.author, self.category, self.description,
                    self.publishedAt, self.title, self.url, self.urlToImage))


def setupNewsApi():
	return NewsApiClient(api_key=config.NEWS_API_KEY)	

def getArticles(query, page = None):		
	api = setupNewsApi()	
	responsedict = api.get_top_headlines(country="br", page_size=100, q=query)
	if "articles" in responsedict:
		return responsedict["articles"]
	return []
		
def saveArticles(articles):
	collection = DB.collection('articles')
	for article in articles:
		# logging.info(f'Trying to create document {article.articleId}')
		if collection.document(str(article.articleId)).get().exists == False:		
			# logging.info(f'Does not exist, creating...')
			collection.add(article.to_dict(), str(article.articleId))	

def filterArticles(articles):
	filteredArticles = []
	for article in articles:
		if articles.title != None:
			if any(word in articles.title for word in KEYWORDS):
				filteredArticles.append(article)
	return filteredArticles	

def pollArticles():
	articlesDict = []
	for word in KEYWORDS:
		articlesDict.extend(getArticles(word))
	articles = map(lambda article: 
	Article(articleId=getsha256(article["url"]),
	 author=article["author"],
	 category=None, 
	 description=article["description"], 
	 publishedAt=article["publishedAt"],
	 title=article["title"],
	 url=article["url"],
	 urlToImage=article["urlToImage"]), articlesDict)

	# TODO: classify articles

	saveArticles(articles)

def getsha256(text):
	return hashlib.sha256(text.encode('utf-8')).hexdigest()

def pubsub_receiver(event, context):
	pollArticles()

if __name__ == '__main__':
	pollArticles()