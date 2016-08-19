import sys
import psycopg2
import urllib
import time
from collections import namedtuple

def run():

    try:
        conn = psycopg2.connect(database=BackFill().dbName, user=BackFill().user, password=BackFill().password,
                                host=BackFill().host, port=BackFill().port)
        cur = conn.cursor()
        # cur.execute("SELECT *  from scrips")
        # rows = cur.fetchall()
        # for row in rows:
        #     print row[0]

    except:
        print "Unable to connect to the database"

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
                timestamp = int(content[0][1:])
                base_timestamp = timestamp
                priceTuple._make(content[1:])
                print priceTuple
            else:
                timestamp = base_timestamp + int(content[0])*interval
                priceTuple._make(content[1:])
                print priceTuple

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




class BackFill:

    def __init__(self):
        self.dbName = "marketdatadb"
        self.user = "bhuwania"
        self.password = "29jan2008"
        self.host = "localhost"
        self.port = "5432"
        # User ID='bhuwania';Password='29jan2008';Host=localhost;Port=5432;Database=marketdatadb;Pooling=true;Min Pool Size=0;Max Pool Size=100;Connection Lifetime=0;


def main():
    run()


if __name__ == "__main__":
    main()