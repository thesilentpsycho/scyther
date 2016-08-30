import psycopg2
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
        # self.tableName = kwargs.get('tbname',None)
        # if not self.tableName:
        #     self.tableName = "data1min"
        # User ID='bhuwania';Password='29jan2008';Host=localhost;Port=5432;Database=marketdatadb;Pooling=true;Min Pool Size=0;Max Pool Size=100;Connection Lifetime=0;

    def getConn(self):

        try:
            if self.conn is None:
                self.conn = psycopg2.connect(database=self.dbName, user=self.user, password=self.password,
                                    host=self.host, port=self.port)
            return self.conn

        except:
            print "Unable to connect to the database"

    def insertPrice(self,timestamp,symbol,pricetuple):
        try:
            conn = self.getConn()
            cur = conn.cursor()
            query = "INSERT INTO %s VALUES ('%s', '%s', %s, %s, %s, %s, %s)" %(self.tableName,timestamp,symbol,pricetuple.open,pricetuple.close,pricetuple.low,pricetuple.high,pricetuple.volume)
            cur.execute(query)

        except:
            print "Cannot insert"

    def deletePrice(self):
        try:
            cur = self.getConn()

        except:
            print "Cannot delete"

    def updatePrice(self):

        try:
            cur = self.getConn()

        except:
            print "Cannot Update"

    def getPrice(self):

        try:
            cur = self.getConn()

        except:
            print "Cannot Fetch"

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
            print "Cannot Fetch All"