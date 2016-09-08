import urllib2
import json
import time
import requests
import logging


class GoogleFinanceAPI:
    def __init__(self):
        self.prefix = "http://finance.google.com/finance/info?client=ig&q="

    def get(self, symbol):
        symbols = urllib2.quote(symbol)
        url = self.prefix + "%s" % (symbols)
        req = urllib2.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0')
        try:
            f = urllib2.urlopen(req)
            rawdata = f.read()
            f.close()
        except Exception as e:
            logging.error("Error fetching URL")
            f.close()
        obj = json.loads(rawdata[3:])
        return obj
