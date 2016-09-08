import psycopg2
from dbhelper import DBHelper

mydb = DBHelper()
scrips = mydb.getAll(tbname="scrips")
conn = mydb.getConn()
conn.close()
scripList = [scrip[0] for scrip in scrips]

mydb.insertlivedata(scripList)

