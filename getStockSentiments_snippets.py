# importing required libraries
import feedparser
import time
import re
from bs4 import BeautifulSoup
import nltk
import warnings
warnings.filterwarnings('ignore')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import csv
import pandas as pd

sia = SentimentIntensityAnalyzer()



# stock market lexicon

..

stock_lex['sentiment'] = (stock_lex['Aff_Score'] + stock_lex['Neg_Score'])/2
stock_lex = dict(zip(stock_lex.Item, stock_lex.sentiment))


# scaling sentiment values

stock_lex_scaled = {}
for k, v in stock_lex.items():
    if v > 0:
        stock_lex_scaled[k] = v / max(stock_lex.values()) * 4
    else:
        stock_lex_scaled[k] = v / min(stock_lex.values()) * -4



# <creating list of positive words/phrases into [positive]>
# <creating list of negative words/phrases into [negative]>

..


# get the list of company names

..

setofCompanies = [ ]  
for i in x:
    i = i.split('\n')[0]
    setofCompanies.append(i)   
sorted_set_of_companies = (sorted(set(setofCompanies)))  



# get the list of filler words

..

fillers=[]   
for i in x:
    i = i.split('\n')[0]
    fillers.append(i)    
sorted_set_of_fillers = (sorted(set(fillers)))  


# creating function to get company name

def get_company_name(title, sorted_set_of_companies, sorted_set_of_fillers):
    # <regex>
    title = title.split()
    list_of_words_in_title = [i for i in title]
    
    allCompanies = []
    for word in list_of_words_in_title:
        if word not in sorted_set_of_fillers and word in sorted_set_of_companies:
            company = word
            allCompanies.append(company)
    return allCompanies


# connecting to MySQL database

import mysql.connector as mysql
db = mysql.connect(
    host = “..”,
    user = “..”,
    password = “..”,
    database = “..”
)



cursor = db.cursor() #db object, cursor method (used later to retrieve ticker symbols from a table in database)


# sentiments

def get_sentiment(text):

    # <calculating sentiment scores>
    if sentiment_score > 0.8:
        sentiment = 'positive'
    elif sentiment_score < 0.2:
        sentiment = 'negative'
    else:
        sentiment = 'neutral'

    return sentiment


# ticker symbols

def get_symbol(company):

    #initialize ticker symbol
    symbol = ''

    #Retrieve ticker symbol
    sql_select_query = """select symbol from ticker_lookup where company = %s"""
    cursor.execute(sql_select_query, (company,))
    record = cursor.fetchall()

    for row in record:
        symbol = row[0]

    return symbol


# WSJ data

def retrieve_wsj_data(url):
    d = feedparser.parse(url)
    source = "wsj"
    great_day_today = time.strftime('%Y-%m-%d')
    
    for i, entry in enumerate(d.entries):
        title = entry.title
        company_list = get_company_name(title, sorted_set_of_companies, sorted_set_of_fillers)
       
        for i in company_list:
             company = i        
             symbol = get_symbol(company)
	     summary = entry.summary
             soup = BeautifulSoup(summary, 'html.parser')
             description = str(soup)
             title_descrp = str(title) + ' ' + str(description)
             sentiment = get_sentiment(title_descrp)

             query = "INSERT INTO feeddata (source, company, symbol, title, description, sentiment, published_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
             values = (source, company, symbol, title, description, sentiment, great_day_today)
             cursor.execute(query, values)
             db.commit()

retrieve_wsj_data('https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml')
retrieve_wsj_data('https://feeds.a.dj.com/rss/RSSMarketsMain.xml')
retrieve_wsj_data('https://feeds.a.dj.com/rss/RSSWSJD.xml')


# CNBC data

def retrieve_cnbc_data(url):
    e = feedparser.parse(url)
    source = "CNBC"
    great_day_today = time.strftime('%Y-%m-%d')

    for i, entry in enumerate(e.entries):
        title = entry.title
        company_list = get_company_name(title, sorted_set_of_companies, sorted_set_of_fillers)
        print(company_list)

        if len(company_list) > 0:
            for i in company_list:
                company = i
                symbol = get_symbol(company)
                summary = entry.summary
                soup = BeautifulSoup(url, 'html.parser')
                description = str(soup)
                title_descrp = str(title) + '\n' + str(description)
                sentiment = get_sentiment(title_descrp)

                query = "INSERT INTO feeddata (source, company, symbol, title, description, sentiment, published_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                values = (source, company, symbol, title, description, sentiment, great_day_today)
                cursor.execute(query, values)
                db.commit()

retrieve_cnbc_data('https://www.cnbc.com/id/15839069/device/rss/rss.html')



# used similar method for other news outlets as well
# changed according to RSS feed structure
