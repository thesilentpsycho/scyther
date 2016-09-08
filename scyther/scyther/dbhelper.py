import psycopg2
import logging
from datetime import datetime

class DBHelper:
    def __init__(self):
        self.dbName = "marketdatadb"
        self.user = "bhuwania"
        self.password = "29jan2008"
        self.host = "localhost"
        self.port = "5432"
        self.tableName = "data1min"
        self.conn = None

    def getConn(self):

        try:
            if self.conn is None or self.conn.closed == 1:
                self.conn = psycopg2.connect(database=self.dbName, user=self.user, password=self.password,
                                    host=self.host, port=self.port)
            return self.conn

        except:
            logging.error('Unable to connect to the database')

    def insertPrice(self,timestamp,symbol,pricetuple):
        try:
            conn = self.getConn()
            cur = conn.cursor()
            query = "INSERT INTO %s VALUES ('%s', '%s', %s, %s, %s, %s, %s)" %(self.tableName,timestamp,symbol,pricetuple.open,pricetuple.close,pricetuple.low,pricetuple.high,pricetuple.volume)
            cur.execute(query)

        except Exception as e:
            logging.error('Insert Failed for %s %s' %(timestamp,symbol))
            raise

    def updatelivedata(self, quoteList):
        tableName = "livedata"
        try:
            conn = self.getConn()
            cur = conn.cursor()
            query = """UPDATE livedata SET currprice = %s, change = %s, changepercent = %s WHERE symbol = %s"""
            for quote in quoteList:
                if quote['l_fix']:
                    l_fix = float(quote['l_fix'])
                else: l_fix = 0.0
                if quote['c_fix']:
                    c_fix = float(quote['c_fix'])
                else:
                    c_fix = 0.0
                if quote['cp_fix']:
                    cp_fix = float(quote['cp_fix'])
                else:
                    cp_fix = 0.0
                cur.execute(query,(l_fix, c_fix, cp_fix, quote['t']))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print e.message
            logging.error('Insert Failed in %s' % ("updatelivedata"))
        finally:
            if conn is not None and conn.closed == 0:
                conn.close()

    def deletePrice(self):
        try:
            conn = self.getConn()

        except:
            logging.error('Delete failed')

    def updatePrice(self):

        try:
            conn = self.getConn()

        except:
            logging.error('Update Failed')

    def getPrice(self):

        try:
            conn = self.getConn()

        except:
            logging.error('Fetch Price failed')

    def getAll(self,tbname):
        if not tbname:
            tbname = self.tableName
        try:
            conn = self.getConn()
            cur = conn.cursor()
            query = "SELECT *  from %s" %tbname
            cur.execute(query)
            rows = cur.fetchall()
            cur.close()
            return rows

        except:
            logging.error('Error in fetching from %s' %tbname)

    def insertlivedata(self, scripList):
        tableName = "livedata"
        query = """INSERT INTO livedata VALUES (%s, %s, %s, %s)"""
        try:
            conn = self.getConn()
            cur = conn.cursor()
            for scrip in scripList:
                cur.execute(query, (scrip,0,0,0))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print e.message
            logging.error("Error in Insert at insertlivedata")
        finally:
            if conn is not None:
                conn.close()