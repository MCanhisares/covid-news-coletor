# -*- encoding: utf-8 -*-
from __future__ import unicode_literals
from datetime import date as dt, timedelta
from typing import List
from google.cloud import firestore
from functools import reduce
from newsapi import NewsApiClient
from textblob.classifiers import NaiveBayesClassifier
import nltk
import pickle5 as pickle
import time
import base64
import sys
import math 
import hashlib
import config
import gcsfs

DB = firestore.Client(project="covid-news-291320")

KEYWORDS = ["corona", "covid", "pandemia", "SARS", "covid-19"]

STOP_WORDS = [ 'a', 'à', 'agora', 'ainda', 'alguém', 'algum', 'alguma', 'algumas', 'alguns', 'ampla', 'amplas', 'amplo', 'amplos', 'ante', 'antes', 'ao', 'aos', 'após', 'aquela', 'aquelas', 'aquele', 'aqueles', 'aquilo', 'as', 'às', 'até', 'através', 'cada', 'coisa', 'coisas', 'com', 'como', 'contra', 'contudo', 'da', 'daquele', 'daqueles', 'das', 'de', 'dela', 'delas', 'dele', 'deles', 'depois', 'dessa', 'dessas', 'desse', 'desses', 'desta', 'destas', 'deste', 'destes', 'deve', 'devem', 'devendo', 'dever', 'deverá', 'deverão', 'deveria', 'deveriam', 'devia', 'deviam', 'disse', 'disso', 'disto', 'dito', 'diz', 'dizem', 'do', 'dos','e','é','ela','elas','ele','eles','em','entre','era','eram','éramos','essa','essas','esse','esses','esta','está','estamos','estão','estas','estava','estavam','estávamos','este','esteja','estejam','estejamos','estes','esteve','estive','estivemos','estiver','estivera','estiveram','estivéramos','estiverem','estivermos','estivesse','estivessem','estivéssemos','estou','eu','foi','fomos','for','fora','foram','fôramos','forem','formos','fosse','fossem','fôssemos','fui','há','haja','hajam','hajamos','hão','havemos','havia','hei','houve','houvemos','houver','houvera','houverá','houveram','houvéramos','houverão','houverei','houverem','houveremos','houveria','houveriam','houveríamos','houvermos','houvesse','houvessem','houvéssemos','isso','isto','já','lhe','lhes','mais','mas','me','mesmo','meu','meus','minha','minhas','muito','na','não','nas','nem','no','nos','nós','nossa','nossas','nosso','nossos','num','numa','o','os','ou','para','pela','pelas','pelo','pelos','por','qual','quando','que','quem','são','se','seja','sejam','sejamos','sem','ser','será','serão','serei','seremos','seria','seriam','seríamos','seu','seus','só','sobre','somos','sou','sua','suas','também','te','tem','têm','temos','tenha','tenham','tenhamos','tenho','ter','terá','terão','terei','teremos','teria','teriam','teríamos','teu','teus','teve','tinha','tinham','tínhamos','tive','tivemos','tiver','tivera','tiveram','tivéramos','tiverem','tivermos','tivesse','tivessem','tivéssemos','tu','tua','tuas','um','uma','você','vocês','vos']

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

def removeArticleSource(text:str):
	index = text.rfind('-')	
	return text[:index - 1] if index > 1 else text

def removeStopWords(text: str):
	splitedResults = []	
	splited = text.split()
	for word in splited:
		if word not in STOP_WORDS and word not in KEYWORDS:
			splitedResults.append(word)
	return splitedResults

def downloadNltkPackages():
	nltk.download('rslp')
	nltk.download('punkt')

def stemWords(words):	
	stemmedWords = []
	stemmer = nltk.stem.RSLPStemmer()
	for word in words:
		stemmedWords.append(stemmer.stem(word))
	return stemmedWords

def classifyCategory(classfier, text):
	words = removeStopWords(text)
	stemmedWords = stemWords(words)
	textToClassify = " ".join(stemmedWords)	
	return classfier.classify(textToClassify)

def loadClassifier():
	downloadNltkPackages()
	fs = gcsfs.GCSFileSystem(project='covid-news-291320')
	filename = "covid-news-291320.appspot.com/Classificador.pickle"
	with fs.open(filename, 'rb') as handle:
		return pickle.load(handle)

def setupNewsApi():
	return NewsApiClient(api_key=config.NEWS_API_KEY)	

def getArticles(query, page = None):		
	api = setupNewsApi()	
	responsedict = api.get_top_headlines(country="br", page_size=1, q=query)
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
	classifier = loadClassifier()
	articlesList = []	
	for word in KEYWORDS:
		articlesList.extend(getArticles(word))	
	for article in articlesList:		
		article["title"] = removeArticleSource(article["title"])			
		article["category"] = classifyCategory(classifier, article["title"])
	articles = map(lambda article: 
	Article(articleId=getsha256(article["url"]),
	 author=article["author"],
	 category=article["category"], 
	 description=article["description"], 
	 publishedAt=article["publishedAt"],
	 title=article["title"],
	 url=article["url"],
	 urlToImage=article["urlToImage"]), articlesList)	
	saveArticles(articles)

def getsha256(text):
	return hashlib.sha256(text.encode('utf-8')).hexdigest()

def pubsub_receiver(event, context):
	pollArticles()

def http_receiver(request):
	pollArticles()

if __name__ == '__main__':
	pollArticles()