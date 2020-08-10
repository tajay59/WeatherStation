


##################################################################################################################################
#                                                TIME SYNC CLASS                                                                 #
#                                                                                                                                #
#   self,tm_year,tm_mon,tm_mday,tm_hour,tm_min,tm_sec,tm_wday,tm_yday,tm_isdst                                                   #
#   (self.tm_year,self.tm_mon,self.tm_mday,self.tm_hour,self.tm_min,self.tm_sec,self.tm_wday,self.tm_yday,self.tm_isdst)         #
#                                                                                                                                #
##################################################################################################################################


class TimeSync:

    def __init__(self,now):

        import time
        import json
        import sys
        import pprint      
        import syslog
        import logging
        import math as M
        from collections import defaultdict
        #Database connection
        import paho.mqtt.client as mqtt
        from pymongo import MongoClient
        import pymongo  as pym

        self.pymongo        = pym
        self.logging        = logging
        self.syslog         = syslog
        self.json           = json
        self.time           = time
        self.clientMongo    = MongoClient('mongodb://localhost:27017/')
        self.db             = self.clientMongo['data']
        self.M              = M
        self.tm_year        = now.tm_year
        self.tm_mon         = now.tm_mon
        self.tm_mday        = now.tm_mday
        self.tm_hour        = now.tm_hour
        self.tm_min         = now.tm_min
        self.tm_sec         = now.tm_sec
        self.tm_wday        = now.tm_wday
        self.tm_yday        = now.tm_yday
        self.tm_isdst       = now.tm_isdst
        self.five           = 0
        self.ten            = 0
        self.hourly         = 0
        self.daily          = 0
        self.monthly        = 0
        self.yearly         = 0
        self.file           = "AWS: TimeSync.py"
        DESCENDING          = 1
        ASCENDING           = -1
        self.syslog.openlog(logoption=self.syslog.LOG_PID, facility=self.syslog.LOG_MAIL)
        #Error logging config
        self.logging.basicConfig(filename='/home/ubuntu/aws/main/app.log', filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')  #<----- update path for log file
        months              = {"1":31,"2":[28,29],"3":31,"4":30,"5":31,"6":30,"7":31,"8":31,"9":30,"10":31,"11":30,"12":31}


    def fiveminute(self):
        now = (self.tm_year,self.tm_mon,self.tm_mday,self.tm_hour,self.tm_min,self.tm_sec,self.tm_wday,self.tm_yday,self.tm_isdst)
        now_ts = self.M.floor(self.time.mktime(now))
        start = (self.tm_year,self.tm_mon,self.tm_mday,self.tm_hour,0,0,self.tm_wday,self.tm_yday,self.tm_isdst)
        TimeStamp = self.M.floor(self.time.mktime(start))
        start = TimeStamp
        array = []
        

        for i in range(13):
            array.append(TimeStamp)
            TimeStamp += 300

        if now_ts == array[0]:
            upper_ts = now_ts +300
            lower_ts = now_ts
        elif now_ts == array[-1]:
            upper_ts = now_ts
            lower_ts = now_ts - 300
        else:
            upper_ts = array[self.next(array,now_ts)]
            lower_ts = upper_ts - 300
       
        print("Now "+ str(self.time.ctime(now_ts)))
        print("upper ts "+ str(upper_ts))
        print("lower ts "+ str(lower_ts))
        print("starting ts ",self.time.ctime(lower_ts)," ending ts ",self.time.ctime(upper_ts)," actual = ", str(start+3600), "  array ",array)
        return [lower_ts,upper_ts]



    def tenminute(self):
        now = (self.tm_year,self.tm_mon,self.tm_mday,self.tm_hour,self.tm_min,self.tm_sec,self.tm_wday,self.tm_yday,self.tm_isdst)
        now_ts = self.M.floor(self.time.mktime(now))
        start = (self.tm_year,self.tm_mon,self.tm_mday,self.tm_hour,0,0,self.tm_wday,self.tm_yday,self.tm_isdst)
        TimeStamp = self.M.floor(self.time.mktime(start))
        start = TimeStamp
        array = []

        for i in range(7):
            array.append(TimeStamp)
            TimeStamp += 600

        if now == array[0]:
            upper_ts = now_ts +600
            lower_ts = now_ts
        elif now == array[-1]:
            upper_ts = now_ts
            lower_ts = now_ts - 600
        else:
            upper_ts = array[self.next(array,now_ts)]
            lower_ts = upper_ts - 600

        print("Now "+ str(now_ts))
        print("upper ts "+ str(upper_ts))
        print("lower ts "+ str(lower_ts))
        print("starting ts ",self.time.ctime(lower_ts)," ending ts ",self.time.ctime(upper_ts)," actual = ", str(start+3600), "  array ",array)
        return [lower_ts,upper_ts]


    def hour(self):
        now = (self.tm_year,self.tm_mon,self.tm_mday,self.tm_hour,self.tm_min,self.tm_sec,self.tm_wday,self.tm_yday,self.tm_isdst)
        now_ts = self.M.floor(self.time.mktime(now))
        start = (self.tm_year,self.tm_mon,self.tm_mday,self.tm_hour,0,0,self.tm_wday,self.tm_yday,self.tm_isdst)
        TimeStamp = self.M.floor(self.time.mktime(start))
        upper_ts = TimeStamp + 3600
        lower_ts = TimeStamp
        print("Now "+ str(now_ts))
        print("upper ts "+ str(upper_ts))
        print("lower ts "+ str(lower_ts))
        print("starting ts ",self.time.ctime(lower_ts)," ending ts ",self.time.ctime(upper_ts)," actual = ", str(lower_ts+3600))
        return [lower_ts,upper_ts]


    def day(self):
        now = (self.tm_year,self.tm_mon,self.tm_mday,self.tm_hour,self.tm_min,self.tm_sec,self.tm_wday,self.tm_yday,self.tm_isdst)
        now_ts = self.M.floor(self.time.mktime(now))
        start = (self.tm_year,self.tm_mon,self.tm_mday,0,0,0,self.tm_wday,self.tm_yday,self.tm_isdst)
        TimeStamp = self.M.floor(self.time.mktime(start))
        upper_ts = TimeStamp + 86400
        lower_ts = TimeStamp              
        print("Now "+ str(now_ts))
        print("upper ts "+ str(upper_ts))
        print("lower ts "+ str(lower_ts))
        print("starting ts ",self.time.ctime(lower_ts)," ending ts ",self.time.ctime(upper_ts)," actual = ", str(lower_ts+86400))
        return [lower_ts,upper_ts]


    def month(self):
        now = (self.tm_year,self.tm_mon,self.tm_mday,self.tm_hour,self.tm_min,self.tm_sec,self.tm_wday,self.tm_yday,self.tm_isdst)
        now_ts = self.M.floor(self.time.mktime(now))
        start = (self.tm_year,self.tm_mon,0,24,0,0,self.tm_wday,self.tm_yday,self.tm_isdst)
        end = (self.tm_year,self.tm_mon+1,0,24,0,0,self.tm_wday,self.tm_yday,self.tm_isdst)
        upper_ts = self.M.floor(self.time.mktime(end))
        lower_ts = self.M.floor(self.time.mktime(start))       
        print("Now "+ str(now_ts))
        print("upper ts "+ str(upper_ts))
        print("lower ts "+ str(lower_ts))
        print("starting ts ",self.time.ctime(lower_ts)," ending ts ",self.time.ctime(upper_ts))
        return [lower_ts,upper_ts]


    def year(self):
        now = (self.tm_year,self.tm_mon,self.tm_mday,self.tm_hour,self.tm_min,self.tm_sec,self.tm_wday,self.tm_yday,self.tm_isdst)
        now_ts = self.M.floor(self.time.mktime(now))
        start = (self.tm_year,1,0,24,0,0,self.tm_wday,self.tm_yday,self.tm_isdst)
        end = (self.tm_year+1,1,0,24,0,0,self.tm_wday,self.tm_yday,self.tm_isdst)
        upper_ts = self.M.floor(self.time.mktime(end))
        lower_ts = self.M.floor(self.time.mktime(start))
        print("Now "+ str(now_ts))
        print("upper ts "+ str(upper_ts))
        print("lower ts "+ str(lower_ts))
        print("starting ts ",self.time.ctime(lower_ts)," ending ts ",self.time.ctime(upper_ts))
        return [lower_ts,upper_ts]


    def update(self):
        fiveminute  = self.fiveminute()
        tenminute   = self.tenminute()
        hourly      = self.hour()
        daily       = self.day()
        monthly     = self.month()
        yearly      = self.year()

        result = self.testConnection()        # TEST IF CONNECTION TO LOCAL DATABASE IS AVAILABLE
        if result != "Server not available":
            try:
                self.db.time.update_one({'station': "local"}, {"$set": {"fiveminute": hourly[0], "tenminute":hourly[0], "hourly": hourly[0],"daily":daily[0], "monthly":monthly[0],"yearly":yearly[0]}})       #UPDATE DATABASE TIME MANAGEENT COLLECTION. THIS COLLECTION KEEPS TRACK OF THE LAST TIME EVERY OTHER COLLECTION WAS UPDATED
            except  self.pymongo.errors.PyMongoError as e:           
                message = str(e)
                self.d[message] += 1 
                self.syslog.syslog(self.syslog.LOG_ERR, self.file+', ERROR UPDATING TIMESTAMPS IN TIMESYNC CLASS  '+ message[0])
                self.logging.error(self.file+" ERROR UPDATING TIMESTAMPS IN TIMESYNC CLASS", exc_info=True)
            else:
                print("NOTHIN TO DO")



        else:
            self.logging.warning(self.file+"  UPDATE ENDED: NO UPDATES SENT, UNABLE TO CONNECT TO LOCAL DATABASE   : TIMESYNC FUNCTION")
            self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UPDATE ENDED: NO UPDATES SENT, UNABLE TO CONNECT TO LOCAL DATABASE   : TIMESYNC FUNCTION')
        

    
    def next(self,arr, target): 
        start = 0; 
        end = len(arr) - 1; 
      
        ans = -1; 
        while (start <= end): 
            mid = (start + end) // 2; 
            if (arr[mid] <= target): 
                start = mid + 1; 
      
            else: 
                ans = mid; 
                end = mid - 1; 
      
        return ans; 


    def testConnection(self):
        result = ""
        #print("Testing connection to Remote Database")
        try:
        # The ismaster command is cheap and does not require auth.
            result = self.clientMongo.admin.command('ismaster')
            self.logging.warning(self.file+" CONNECTED TO LOCAL SERVER ")
            
        except  self.pymongo.errors.PyMongoError as e:
            #print("Server not available")
            message = str(e)
            self.d[message] += 1 
                                        
            self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UNABLE TO CONNECT TO LOCAL DATABASE ')
            self.logging.error(self.file+"  UNABLE TO CONNECT TO LOCAL DATABASE ", exc_info=True)
            result = "Server not available"
        else:
            pass
        finally:
            return result




def main():
    import time
    a = time.time()
    b = time.localtime(a)
    T = TimeSync(b)
    T.update()

if __name__ == "__main__":
    main()
