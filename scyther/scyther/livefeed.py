from googlelive import GoogleFinanceAPI
import time
from dbhelper import DBHelper

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        query = ""
        temp = l[i:i + n]
        yield 'NSE:'+',NSE:'.join(temp)

def run():
    financeapi = GoogleFinanceAPI()
    while 1:
        myDB = DBHelper()
        scripsData = myDB.getAll(tbname='scrips')
        conn = myDB.getConn()
        conn.close()
        scripList = [scrip[0] for scrip in scripsData]
        chunkList = list(chunks(scripList,99))      #since URL can only be 2000 characters long and google finance returns only 100 symbols at a time
        for chunk in chunkList:
            time.sleep(1)
            quotelist = financeapi.get(chunk)
            myDB.updatelivedata(quotelist)
        time.sleep(2)

def main():
    run()

if __name__ == "__main__":
    main()