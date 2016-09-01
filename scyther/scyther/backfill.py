import sys
import psycopg2
import urllib2
import time
from collections import namedtuple
from dbhelper import DBHelper
import datetime
import requests

def scrape(symbol):
    myDB = DBHelper()
    url = "https://www.google.com/finance/getprices?q=%s&x=NSE&i=60&p=1d&f=d,c,o,h,l,v&df=cpct&auto=1" %symbol
    req = urllib2.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0')
    f = urllib2.urlopen(req)
    rawdata = f.read()
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
            except:
                print "Insert Failed"
    conn = myDB.getConn()
    conn.commit()
    conn.close()

def run():

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


def main():
    run()


if __name__ == "__main__":
    main()