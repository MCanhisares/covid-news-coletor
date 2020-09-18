# -*- encoding: utf-8 -*-
from __future__ import unicode_literals
from datetime import date as dt, timedelta
from typing import List
from google.cloud import firestore
from google.cloud import logging as gcplog
from functools import reduce
import logging
import tweepy
import time
import base64
import sys
import math

CONSUMER_KEY = ""
CONSUMER_SECRET = ""

OAUTH_TOKEN = ""
OAUTH_TOKEN_SECRET = ""

REQUEST_TOKEN_URL = "https://api.twitter.com/oauth/request_token"
AUTHORIZE_URL = "https://api.twitter.com/oauth/authorize?oauth_token="
ACCESS_TOKEN_URL = "https://api.twitter.com/oauth/access_token"

STOCK_CODES = ["ABCB4", \
"AZUL4", \
"ABEV3", \
"AGRO3", \
"ALPA3", \
"ALPA4", \
"ALUP11", \
"AMAR3", \
"ARZZ3", \
"ATOM3", \
"BAHI3", \
"BAZA3", \
"BBAS3", \
"BBSE3", \
"BBDC4", \
"BBRK3", \
"BIDI4", \
"ANIM3", \
"BEEF3", \
"BMEB3", \
"BMEB4", \
"BMIN4", \
"BOBR4", \
"BOVA11", \
"BRAP3", \
"BRAP4", \
"BRFS3", \
"BRGE11", \
"BRGE3", \
"BRGE8", \
"BRIV4", \
"BRKM3", \
"BRKM5", \
"BRML3", \
"BRPR3", \
"BRSR6", \
"BSLI3", \
"BTOW3", \
"BTTL3", \
"CALI4", \
"CARD3", \
"CCRO3", \
"CESP6", \
"CGAS5", \
"CIEL3", \
"CLSC4", \
"CMIG3", \
"CMIG4", \
"COCE5", \
"CPFE3", \
"CPLE3", \
"CPLE6", \
"CRDE3", \
"CSAN3", \
"CSMG3", \
"CSNA3", \
"CSRN5", \
"CTKA4", \
"CVCB3", \
"CYRE3", \
"DASA3", \
"DIRR3", \
"DIVO11", \
"DTEX3", \
"ECOR3", \
"EEEL3", \
"ELET3", \
"ELET6", \
"EMAE4", \
"EMBR3", \
"ENBR3", \
"ENGI11", \
"EQTL3", \
"ESTR4", \
"ETER3", \
"EUCA4", \
"EVEN3", \
"EZTC3", \
"FESA4", \
"FHER3", \
"FLRY3", \
"FRAS3", \
"FRIO3", \
"GFSA3", \
"GGBR3", \
"GGBR4", \
"GOAU3", \
"GOAU4", \
"GOLL4", \
"GOVE11", \
"GPCP3", \
"GRND3", \
"GSHP3", \
"GUAR3", \
"HAGA4", \
"HBOR3", \
"HGTX3", \
"HYPE3", \
"IDNT3", \
"IGTA3", \
"INEP3", \
"INEP4", \
"ISUS11", \
"ITSA3", \
"ITSA4", \
"ITUB4", \
"JBSS3", \
"JHSF3", \
"JSLG3", \
"KEPL3", \
"KLBN11", \
"LAME3", \
"LAME4", \
"LCAM3", \
"LEVE3", \
"LIGT3", \
"LLIS3", \
"LOGN3", \
"LPSB3", \
"LREN3", \
"LUPA3", \
"LUXM3", \
"LUXM4", \
"MDIA3", \
"MGEL4", \
"MGLU3", \
"MILS3", \
"MNDL3", \
"MOAR3", \
"MRFG3", \
"MRVE3", \
"MULT3", \
"MYPK3", \
"NAFG4", \
"ODPV3", \
"OIBR3", \
"OIBR4", \
"PATI3", \
"PCAR4", \
"PDGR3", \
"PEAB3", \
"PETR3", \
"PETR4", \
"PFRM3", \
"PIBB11", \
"PINE4", \
"PMAM3", \
"POMO3", \
"POMO4", \
"POSI3", \
"PSSA3", \
"PTBL3", \
"PTNT3", \
"ENAT3", \
"QUAL3", \
"RADL3", \
"RAPT3", \
"RAPT4", \
"RCSL3", \
"RDNI3", \
"RENT3", \
"RNEW11", \
"ROMI3", \
"RPMG3", \
"RAIL3", \
"SANB11", \
"SANB3", \
"SANB4", \
"SAPR4", \
"SBSP3", \
"SCAR3", \
"SEER3", \
"SMLS3", \
"SGPS3", \
"SHOW3", \
"SHUL4", \
"SLCE3", \
"SLED4", \
"SMTO3", \
"SULA11", \
"TAEE11", \
"TCNO4", \
"TCSA3", \
"TECN3", \
"TELB4", \
"TGMA3", \
"TIMP3", \
"TOTS3", \
"TRIS3", \
"TRPL3", \
"TRPL4", \
"TUPY3", \
"UCAS3", \
"UGPA3", \
"UNIP5", \
"UNIP6", \
"USIM3", \
"USIM5", \
"TIET11", \
"VALE3", \
"VIVR3", \
"VIVT3", \
"VIVT4", \
"VLID3", \
"VULC3", \
"WEGE3", \
"WHRL3", \
"WSON33"]

LIMIT = 10
COUNT = 100

DB = firestore.Client(project="mc-tcc1")

class DateController:
	DATE = dt.today()

	@staticmethod
	def setDate(newDate):
		DATE = newDate

class DownloadTweetsError(Exception):
    """DT exception"""

    def __init__(self, message):
        self.message = message
        super(DownloadTweetsError, self).__init__(message)

    def __str__(self):
        return self.message

class Analytics():
	def __init__(self):
		self.collisions = dict()
		self.searchResults = dict()
		self.date = DateController.DATE.strftime('%Y-%m-%d')
		client = gcplog.Client()		
		logging.getLogger().setLevel(logging.INFO)
		client.setup_logging()

	def setDate(self, date):
		self.date = date.strftime('%Y-%m-%d')

	def addSearchInfo(self, stock, qtdResults):
		self.searchResults[stock] = qtdResults

	def addClash(self, stock):
		self.collisions[stock] = 1 if stock not in self.collisions else self.collisions[stock] + 1

	def to_dict(self):
		return {
			u'date': self.date,
			u'totalResults': sum(self.searchResults.values()),
			u'totalCollisions': sum(self.collisions.values()),
			u'results': self.searchResults,
			u'collisions': self.collisions
		}
	
	def saveResults(self):		
		collection = DB.collection('analytics')
		if collection.document(self.date).get().exists == True:
			updatedResult = self.mergeResults(collection.document(self.date).get().to_dict())
			collection.document(self.date).update(updatedResult)
		else:
			collection.add(self.to_dict(), self.date)

	def mergeResults(self, oldResults):
		newResults = self.to_dict()		
		updatedDict = dict()
		updatedDict.update(oldResults)
		updatedDict['totalResults'] = oldResults['totalResults'] + newResults['totalResults']
		updatedDict['totalCollisions'] = oldResults['totalCollisions'] + newResults['totalCollisions']
		oldResults['results'].update(newResults['results'])
		updatedDict['results'] = oldResults['results']
		oldResults['collisions'].update(newResults['collisions'])
		updatedDict['collisions'] = oldResults['collisions']		
		return updatedDict

	def logResults(self):
		logging.info(f'Date: {self.date}')
		logging.info(f'Total results: {sum(self.searchResults.values())}')
		logging.info(f'Total collisions: {sum(self.collisions.values())}')
		resultsStr = ''
		for stock, result in self.searchResults.items():
			resultsStr = resultsStr + f'Stock: {stock} - Results: {result} \n'
		logging.info(resultsStr)
		resultsStr = ''
		for stock, collisions in self.collisions.items():
			resultsStr = resultsStr + f'Stock: {stock} - Collisions: {collisions} \n'		

class Tweet():
	def __init__(self, date, stock, full_text, tweet_id, content):
		self.date = date
		self.content = content
		self.full_text = full_text
		self.tweet_id = tweet_id
		self.stock = stock

	def to_dict(self):
		return {
			u'date': self.date,
			u'content': self.content,
			u'full_text': self.full_text,
			u'tweet_id': self.tweet_id,
			u'stock': self.stock
		}

	@staticmethod
	def from_dict(source):
		return Tweet(date=source[u'date'], stock=source[u'stock'], full_text=source[u'full_text'], tweet_id=source[u'tweet_id'], content=source[u'content'])
	
	def __repr__(self):
		return(u'Tweet(date={}, stock={}, full_text={}, tweet_id={}, content={})'
            .format(self.date, self.stock, self.full_text, self.tweet_id,
                    self.content))

analytics = Analytics()

def getStocksForSection(section):	
	return STOCK_CODES[section*LIMIT: min((section+1)* LIMIT, len(STOCK_CODES))]

def setupTwitterApi():
	auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
	auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
	return tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

def saveSectionProgress(section):
	collection = DB.collection('progress')
	progress = { 
		u'lastSection': section
	}
	if collection.document(DateController.DATE.strftime('%Y-%m-%d')).get().exists == True:
		collection.document(DateController.DATE.strftime('%Y-%m-%d')).update(progress)
	else:
		collection.add(progress, DateController.DATE.strftime('%Y-%m-%d'))	

#If last section = 2, it means it failed on section 2
def getSectionProgress():
	document = DB.collection('progress').document(DateController.DATE.strftime('%Y-%m-%d')).get()
	if document.exists == False:
		return 0
	if 'lastSection' in document.to_dict():
		return document.to_dict()['lastSection']
	return 0

def getTweets(section):
	logging.info(f'Getting tweets for section: {section}')
	if section < 0:
		return
	if section * LIMIT > len(STOCK_CODES):
		return
	
	stocks = getStocksForSection(section)
	yesterday= DateController.DATE - timedelta(days=1)
	
	api = setupTwitterApi()
	for stock in stocks:
		tweets = []			
		tweetCursor = tweepy.Cursor(api.search,
								q=stock,
								since=yesterday.strftime('%Y-%m-%d'),								
								until=DateController.DATE.strftime('%Y-%m-%d'),																
								include_entities=True,
								count=COUNT,
								tweet_mode='extended',
								lang="pt").items()						
		try:
			counter = 0
			for item in tweetCursor:								
				counter = counter + 1
				tweet = Tweet(date=item.created_at, full_text=item.full_text, content=str(item), tweet_id=item.id, stock=stock)		
				tweets.append(tweet)		
			analytics.addSearchInfo(stock, counter)
			saveTweets(tweets)	
		except tweepy.TweepError as err:		
			logging.exception(err)
			if err.response.status_code == 429 or err.response.status_code == 420:	
				raise DownloadTweetsError('Twitter query limit reached')
		finally:
			saveSectionProgress(section)
	
def saveTweets(tweets):
	collection = DB.collection('tweets')
	for tweet in tweets:
		logging.info(f'Trying to create document {tweet.tweet_id}')
		if collection.document(str(tweet.tweet_id)).get().exists == False:		
			logging.info(f'Does not exist, creating...')
			collection.add(tweet.to_dict(), str(tweet.tweet_id))			
		else:
			analytics.addClash(tweet.stock)
			logging.info(f'Exists!')
	

def getTweetsForDate(date: dt):
	DateController.DATE = date			
	analytics.setDate(date)

	initialSection = getSectionProgress()
	lastSection = math.ceil(len(STOCK_CODES) / LIMIT)

	if (initialSection == lastSection):
		logging.info(f'Not getting tweets - initialSection: {initialSection} / lastSection: {lastSection}')
		return
	try: 
		for section in range(initialSection, lastSection):
			getTweets(section)
		saveSectionProgress(lastSection)
	except DownloadTweetsError as err:
		logging.exception(err.message)
	finally:
		analytics.logResults()
		analytics.saveResults()

def queryTweets(event, context):
	logging.info(f'This Function was triggered by messageId {context.event_id} published at {context.timestamp}')
	logging.info(f'{event}')
	if 'data' in event:
		getTweetsForDate(dt.today())

if __name__ == '__main__':
	base = dt.today() - timedelta(days=1)	
	date_list = [base - timedelta(days=x) for x in range(7)]
	for date in sorted(date_list):
		getTweetsForDate(date)