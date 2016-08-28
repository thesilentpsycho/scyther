import sys
import psycopg2
import urllib
import time
from collections import namedtuple
from dbhelper import DBHelper
import datetime

def scrape(symbol):
    myDB = DBHelper()
    link = "https://www.google.com/finance/getprices?q=%s&x=NSE&i=60&p=15d&f=d,c,o,h,l,v&df=cpct&auto=1" %symbol

    f = urllib.urlopen(link)
    base_timestamp = 0
    priceTuple = namedtuple('priceTuple', ['close', 'high', 'low', 'open', 'volume'])
    for idx, line in enumerate(f):
        if (idx == 3):
            interval = int(line.strip().split('=')[1])
            print interval
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
                myDB.insertPrice(timestamp=timestamp, symbol="HDFC", pricetuple=p)
            except:
                print "Insert Failed"


def run():

    myDB = DBHelper()
    scripsData = myDB.getAll(tbname = 'scrips')
    scripList = [scrip[0] for scrip in scripsData]
    for scrip in scripList:
        scrape(scrip)
        time.sleep(1)



def main():
    run()


if __name__ == "__main__":
    main()