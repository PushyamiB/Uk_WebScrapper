from datetime import datetime
import os
import time
DEBUG_MODE = False
while(True):
    if(DEBUG_MODE or (datetime.now().hour==11 and datetime.now().minute==20)):
        os.system('cd /home/dev04/webScrapingProj;/home/dev04/webScrapingProj/UK_WebScrapeEnv/bin/python -u /home/dev04/webScrapingProj/bb_amz_spen_v3.py >> /home/dev04/webScrapingProj/logs/port.log;/home/dev04/webScrapingProj/UK_WebScrapeEnv/bin/python /home/dev04/webScrapingProj/SQL_Insert.py')
        print("sleeping for 1 hour")
        time.sleep(3600)
    #print("not yet")
    time.sleep(0.5)
