import sys
import psycopg2
import urllib2
import time
from collections import namedtuple
from dbhelper import DBHelper
import datetime
import requests
import logging

def scrape(symbol):
    myDB = DBHelper()
    url = "https://www.google.com/finance/getprices?q=%s&x=NSE&i=60&p=1d&f=d,c,o,h,l,v&df=cpct&auto=1" %symbol
    req = urllib2.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0')
    try:
        f = urllib2.urlopen(req)
        rawdata = f.read()
        f.close()
    except Exception as e:
        logging.error("Error fetching URL")
        f.close()
    base_timestamp = 0
    priceTuple = namedtuple('priceTuple', ['close', 'high', 'low', 'open', 'volume'])
    for idx, line in enumerate(rawdata.splitlines()):
        if (idx == 3):
            interval = int(line.strip().split('=')[1])
        if (idx >= 7):
            content = line.strip().split(',')
            if (content[0].startswith('a')):
                timestamp = datetime.datetime.fromtimestamp(int(content[0][1:]))
                base_timestamp = timestamp
                p = priceTuple._make(tuple(content[1:]))
            else:
                timestamp = base_timestamp + datetime.timedelta(seconds=int(content[0]) * interval)
                p = priceTuple._make(tuple(content[1:]))
            try:
                myDB.insertPrice(timestamp=timestamp, symbol=symbol, pricetuple=p)
            except Exception as e:
                logging.error(e.message)

    conn = myDB.getConn()
    conn.commit()
    conn.close()

def run():
    logging.basicConfig(filename='scyther.log', level= logging.DEBUG, format= '%(asctime)s %(message)s')
    logging.info('Backfill Started for %s at %s' %(datetime.datetime.now().date(),datetime.datetime.now()))
    myDB = DBHelper()
    scripsData = myDB.getAll(tbname = 'scrips')
    conn = myDB.getConn()
    conn.close()
    scripList = [scrip[0] for scrip in scripsData]
    idx = 0
    for scrip in scripList:
        scrape(scrip)
        print idx, scrip
        time.sleep(1)
        idx += 1
        logging.info('%s Scraped Successfully' %scrip)
    logging.info('Scraping completed successfully for %s' %datetime.datetime.now().date())

def main():
    run()


if __name__ == "__main__":
    main()