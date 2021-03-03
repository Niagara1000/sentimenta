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

'''
whenever i use sia, it will use vader_lexicon and CSVs under lexicon_data. ec2 -> vader. not in local.
'''
sia = SentimentIntensityAnalyzer()

# stock market lexicon
stock_lex = pd.read_csv('/var/www/html/get_sentiment/lexicon_data/stock_lex.csv')
stock_lex['sentiment'] = (stock_lex['Aff_Score'] + stock_lex['Neg_Score'])/2
stock_lex = dict(zip(stock_lex.Item, stock_lex.sentiment))

stock_lex_scaled = {}
for k, v in stock_lex.items():
    if v > 0:
        stock_lex_scaled[k] = v / max(stock_lex.values()) * 4
    else:
        stock_lex_scaled[k] = v / min(stock_lex.values()) * -4

positive = []
with open('/var/www/html/get_sentiment/lexicon_data/lm_positive.csv', 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        positive.append(row[0].strip())

negative = []
with open('/var/www/html/get_sentiment/lexicon_data/lm_negative.csv', 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        entry = row[0].strip().split(" ")
        if len(entry) > 1:
            negative.extend(entry)
        else:
            negative.append(entry[0])

final_lex = {}
final_lex.update({word:2.0 for word in positive})
final_lex.update({word:-2.0 for word in negative})
final_lex.update(stock_lex_scaled) 
final_lex.update(sia.lexicon)      
sia.lexicon = final_lex

# get the list of company names
with open('/var/www/html/get_sentiment/company_names.txt', 'r') as f:
    x = f.readlines()
    setofCompanies = [ ]

    
for i in x:
    i = i.split('\n')[0]
    setofCompanies.append(i)
    
sorted_set_of_companies = (sorted(set(setofCompanies)))   

# get the list of filler words
with open('/var/www/html/get_sentiment/fillerWords.txt', 'r') as f:
    x = f.readlines()
    fillers=[]
    
for i in x:
    i = i.split('\n')[0]
    fillers.append(i)
    
sorted_set_of_fillers = (sorted(set(fillers)))  

def get_company_name(title, sorted_set_of_companies, sorted_set_of_fillers):
    title = re.sub(r'[^\w\s]','',title)
    title = title.split()
    list_of_words_in_title = [i for i in title]
    
    allCompanies = []
    for word in list_of_words_in_title:
        if word not in sorted_set_of_fillers and word in sorted_set_of_companies:
            company = word
            allCompanies.append(company)
            
    return allCompanies


import mysql.connector as mysql
db = mysql.connect(
    host = "database-1.cs3cbxzps5ti.us-west-1.rds.amazonaws.com",
    user = "archana",
    passwd = "BGm09pwctOyp8ZGfd0mu",
    database = "newsfeed"
)

cursor = db.cursor() #db object, cursor method

def get_sentiment(text):

    sentiment_score = sia.polarity_scores(text)['compound']
    if sentiment_score > 0.8:
        sentiment = 'positive'
    elif sentiment_score < 0.2:
        sentiment = 'negative'
    else:
        sentiment = 'neutral'

    return sentiment

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

def retrieve_wsj_data(url):

    d = feedparser.parse(url)
    source = "wsj"
    great_day_today = time.strftime('%Y-%m-%d')

    for i, entry in enumerate(d.entries):
        title = entry.title
        company_list = get_company_name(title, sorted_set_of_companies, sorted_set_of_fillers) #['apple','disney']
        print(company_list)
        #company = ','.join(company_list)
        if len(company_list) > 0:
            #print(company_list[0])

            for i in company_list:
                company = i        
                symbol = get_symbol(company)

                summary = entry.summary ## entire article or summary is separate on the news?
                soup = BeautifulSoup(summary, 'html.parser')
                description = str(soup)
                title_descrp = str(title) + ' ' + str(description)
                sentiment = get_sentiment(title_descrp)

                query = "INSERT INTO feeddata (source, company, symbol, title, description, sentiment, published_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                values = (source, company, symbol, title, description, sentiment, great_day_today)
                cursor.execute(query, values)
                db.commit()

print("Starting WSJ... \n")
retrieve_wsj_data('https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml')
retrieve_wsj_data('https://feeds.a.dj.com/rss/RSSMarketsMain.xml')
retrieve_wsj_data('https://feeds.a.dj.com/rss/RSSWSJD.xml')

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
                # summary = entry.summary
                soup = BeautifulSoup(url, 'html.parser')
                description = str(soup)
                title_descrp = str(title) + '\n' + str(description)
                sentiment = get_sentiment(title_descrp)

                query = "INSERT INTO feeddata (source, company, symbol, title, description, sentiment, published_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                values = (source, company, symbol, title, description, sentiment, great_day_today)
                cursor.execute(query, values)
                db.commit()

print("Starting CNBC... \n")
retrieve_cnbc_data('https://www.cnbc.com/id/15839069/device/rss/rss.html')
##DEBUG----->retrieve_cnbc_data('https://www.cnbc.com/id/15837362/device/rss/rss.html')
##DEBUG---retrieve_cnbc_data('https://www.cnbc.com/id/10001147/device/rss/rss.html')


# Retrieve Finviz data
print("Starting Finviz... \n")
source = "Finviz"
great_day_today = time.strftime('%Y-%m-%d')

from requests import get
url = 'https://finviz.com/news.ashx'
response = get(url)

html_soup = BeautifulSoup(response.text, 'html.parser')
find_titles = html_soup.find_all("a", "nn-tab-link")

for i in find_titles:
    if find_titles.index!=0:
        title = i.get_text()
        description = ''
        sentiment = get_sentiment(title)            
        company_list = get_company_name(title, sorted_set_of_companies, sorted_set_of_fillers)
        print(company_list)

        if len(company_list) > 0:
            for i in company_list:
                company = i
                symbol = get_symbol(company)

                query = "INSERT INTO feeddata (source, company, symbol, title, description, sentiment, published_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                values = (source, company, symbol, title, description, sentiment, great_day_today)
                cursor.execute(query, values)
                db.commit()
