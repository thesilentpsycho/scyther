import sys
import psycopg2
import urllib
import time
from collections import namedtuple
from dbhelper import DBHelper
import datetime

def run():

    link = "https://www.google.com/finance/getprices?q=HDFC&x=NSE&i=60&p=15d&f=d,c,o,h,l,v&df=cpct&auto=1" \
           # %(row[0], 15)
    f = urllib.urlopen(link)
    base_timestamp = 0
    priceTuple = namedtuple('priceTuple',['close', 'high', 'low', 'open', 'volume'])
    for idx,line in enumerate(f):
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
                timestamp = base_timestamp + datetime.timedelta(seconds= int(content[0])*interval)
                p = priceTuple._make(tuple(content[1:]))
            myDB = DBHelper()
            try:
                myDB.insertPrice(timestamp= timestamp,symbol="HDFC",pricetuple=p)
            except:
                print "Insert Failed"


    # start_time = time.time()
    # rawdata = f.read()
    # rawdata = rawdata.strip('\n').split('\n',7)
    # print rawdata
    # interval = int(rawdata[3].split('=')[1])
    # data = rawdata[7]
    # print interval
    # lines = [line.strip('\n').split('\n') for line in data]
    # print lines
    # print data
    # print("--- %s seconds ---" % (time.time() - start_time))

def main():
    run()


if __name__ == "__main__":
    main()