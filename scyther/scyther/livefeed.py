from googlelive import GoogleFinanceAPI
import time
from dbhelper import DBHelper

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        query = ""
        temp = l[i:i + n]
        yield ','.join(temp)

def run():
    c = GoogleFinanceAPI()
    while 1:
        myDB = DBHelper()
        scripsData = myDB.getAll(tbname='scrips')
        conn = myDB.getConn()
        conn.close()
        scripList = [scrip[0] for scrip in scripsData]
        chunkList = list(chunks(scripList,99))      #since google Api can only handle 99 scrips at a time. using 95
        for chunk in chunkList:
            quote = c.get(chunk)
            print quote
        break
        time.sleep(1)

def main():
    run()

if __name__ == "__main__":
    main()