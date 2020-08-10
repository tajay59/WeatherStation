


#################################################################################################################################################
#                                                                                                                                               #
#                                                    CLASSES TO UPDATE ALL COLLECTIONS IN LOCAL AND REMOTE DATABASE HOURLY                      #
#                                                                                                                                               #
#################################################################################################################################################



class UpdateHourly:

    def __init__(self,station):
        
              
        #Database connection
        import json
        import sys
        import time
        import pprint
        import logging
        import syslog
        import paho.mqtt.client as mqtt
        from pymongo import MongoClient
        import pymongo  as pym
        from timesync import TimeSync
        import math as M       
        from collections import defaultdict        
        from localUpdate import UpdateDatabase
        from remoteUpdate import UpdateRemoteDatabase

        self.json                       = json
        self.TimeSync                   = TimeSync
        self.logging                    = logging
        self.pymongo                    = pym
        self.syslog                     = syslog
        self.UpdateDatabase             = UpdateDatabase
        self.UpdateRemoteDatabase       = UpdateRemoteDatabase
        self.time                       = time
        self.M                          = M
        self.clientMongo                = MongoClient('mongodb://localhost:27017/')
        self.db                         = self.clientMongo['data']
        self.type                       = "UpdateHourly class"
        self.interval                   = 0
        self.count                      = 0
        self.sendingCollection          = self.db.time
        self.receivingCollection        = self.db.time
        self.receivingCollectionName    = ""
        self.updatedToThisPoint         = 0
        self.daily                      = 0
        self.monthly                    = 0
        self.yearly                     = 0
        self.timeVariable               = ""
        self.docsUpdatedCount           = 0
        self.document                   = {}
        self.station                    = station
        self.client                     = mqtt.Client()
        self.client.on_connect          = self.on_connect
        self.client.on_message          = self.on_message
        self.d                          = defaultdict(int)
        self.client.connect("localhost", 1883, 60)
        self.client.loop_start()
        self.start                      = 0
        self.end                        = 0
        self.file                       = "AWS: updateHourly.py"
        self.DESCENDING                 = 1
        self.ASCENDING                  = -1
        self.syslog.openlog(logoption=self.syslog.LOG_PID, facility=self.syslog.LOG_MAIL)
        #Error logging config
        self.logging.basicConfig(filename='/home/ubuntu/aws/main/app.log', filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')  #<----- update path for log file


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
                                        
            self.syslog.syslog(self.syslog.LOG_ERR, file+', UNABLE TO CONNECT TO LOCAL DATABASE ')
            self.logging.error(self.file+"  UNABLE TO CONNECT TO LOCAL DATABASE ", exc_info=True)
            result = "Server not available"
        else:
            pass
        finally:
            return result


    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(client, userdata, rc):
        print("Connected with result code "+str(rc))
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe("oneminute")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        print(msg.topic+" "+str(msg.payload))


    def Daily(self):
        now_ts = self.M.floor(self.time.time())
        now = self.time.localtime(now_ts)
        
        self.syslog.syslog(self.syslog.LOG_ERR, self.file+', Daily funct details:  now.tm_hour '+ str(now.tm_hour)+' now_ts '+str(now_ts)+' self.daily '+str(self.daily)+' sum '+str(self.daily + 86400))  # REMOVE THIS LINE BEFORE DEPLOYMENT
    
        #RUN DAILY UPDATE   
        if now.tm_hour == 0 or (now_ts > (self.daily + 86400)) :
            self.syslog.syslog(self.syslog.LOG_ERR, self.file+', STARTED UPDATING DAILY LOCAL  COLLECTION ')
            self.logging.warning(self.file+', STARTED UPDATING DAILY LOCAL  COLLECTION  ')
            rp = self.UpdateDatabase("daily",self.station)
            rp.setInterval()
            rp.updater()


    def Monthly(self):
        now_ts      = self.M.floor(self.time.time())               # GET CURRENT TIMESTAMP 
        now         = self.time.localtime(now_ts)
        tsync_ts    = self.TimeSync(now)
        ts          = tsync_ts.month()
        count       = ts[1] - ts[0]
        self.syslog.syslog(self.syslog.LOG_ERR, self.file+', Monthly funct details:  now.tm_mday '+str(now.tm_mday)+' now_ts '+str(now_ts)+' self.monthly '+str(self.monthly)+' count '+str(count)+ ' sum '+str(self.monthly + count))  # REMOVE THIS LINE BEFORE DEPLOYMENT

        
        #RUN MONTHLY UPDATE   
        if now.tm_mday == 1 or (now_ts > (self.monthly + count)):
            self.syslog.syslog(self.syslog.LOG_ERR, self.file+', STARTED UPDATING MONTHLY LOCAL  COLLECTION : TIMESTAMP '+str(count)+' self.monthly '+str(self.monthly))
            self.logging.warning(self.file+', STARTED UPDATING MONTHLY LOCAL  COLLECTION : TIMESTAMP '+str(count)+' self.monthly '+str(self.monthly))
            rp = self.UpdateDatabase("monthly",self.station)
            rp.setInterval()
            rp.updater()


    def Yearly(self):
        now_ts      = self.M.floor(self.time.time())               # GET CURRENT TIMESTAMP 
        now         = self.time.localtime(now_ts)
        tsync_ts    = self.TimeSync(now)
        ts          = tsync_ts.year()
        count       = ts[1] - ts[0]

        self.syslog.syslog(self.syslog.LOG_ERR, self.file+', Yearly funct details:  now.tm_mday '+str(now.tm_mday)+' now.tm_mon '+str(now.tm_mon)+' now_ts '+str(now_ts)+' self.yearly '+str(self.yearly )+' count '+str(count)+ ' sum '+str(self.yearly  + count))  # REMOVE THIS LINE BEFORE DEPLOYMENT
       
        #RUN YEARLY UPDATE   
        if (now.tm_mday == 1 and now.tm_mon == 1) or (now_ts > (self.yearly + count))  :
            self.syslog.syslog(self.syslog.LOG_ERR, self.file+', STARTED UPDATING YEARLY LOCAL  COLLECTION : TIMESTAMP '+str(count)+' self.yearly '+str(self.yearly))
            self.logging.warning(self.file+', STARTED UPDATING YEARLY LOCAL  COLLECTION : TIMESTAMP '+str(count)+' self.yearly '+str(self.yearly))
            rp = self.UpdateDatabase("yearly",self.station)
            rp.setInterval()
            rp.updater()



    def update(self):
        self.end                    = self.M.floor(self.time.time())  
        result                      = self.testConnection()        # TEST IF CONNECTION TO LOCAL DATABASE IS AVAILABLE
        if result != "Server not available":
            try:
                #CHECK AND UPDATE FIVE, TEN AND HOURLY TABLES NOW
                array = ["five","ten","hourly","daily","monthly","yearly"]
                for i in array:

                    update = self.UpdateDatabase(i,self.station)
                    update.setInterval()
                    update.updater()


            except self.pymongo.errors.PyMongoError as e:
                message = str(e)
                self.d[message] += 1 
                self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UNABLE TO CONNECT TO LOCAL DATABASE TO RUN HOURLY UPDATE')
                self.logging.error(self.file+"  UNABLE TO CONNECT TO LOCAL DATABASE TO RUN HOURLY UPDATE", exc_info=True)
            else:
                try:
                    #CHECK AND UPDATE DAY, MONTH AND YEAR TABLES IF THAT TIME IS NOW
                    self.document   = self.db.time.find_one({"station":"local"})
                    self.daily      = self.document["daily"]
                    self.monthly    = self.document["monthly"]
                    self.yearly     = self.document["yearly"]
                    #self.Daily()
                    #self.Monthly()
                    #self.Yearly()

                
                except  self.pymongo.errors.PyMongoError as e:
                    message = str(e)
                    self.d[message] += 1 
                    self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UNABLE TO UPDATE DAILY, MONTHLY OR YEARLY COLLECTIONS IN LOCAL DATABASE ')
                    self.logging.error(self.file+"  UNABLE TO UPDATE DAILY, MONTHLY AND YEARLY COLLECTIONS IN LOCAL DATABASE ", exc_info=True)
                else:
                    self.syslog.syslog(self.syslog.LOG_ERR, self.file+', FINISHED UPDATING DAILY, MONTHLY AND YEARLY COLLECTIONS IN LOCAL DATABASE ')
                    self.logging.error(self.file+"  FINISHED UPDATING DAILY, MONTHLY OR YEARLY COLLECTIONS IN LOCAL DATABASE ", exc_info=True)

            finally:
                
                try:
                    self.syslog.syslog(self.syslog.LOG_ERR, self.file+', STARTED UPDATING REMOTE DATABASE ')
                    self.logging.warning(self.file+', STARTED UPDATING REMOTE DATABASE ')

                    #CHECK AND UPDATE ALL COLLECTIONS IN REMOTE DATABASE
                    array = ["five","ten","hourly","daily","monthly","yearly"]
                    for i in array:
                        
                        rp = self.UpdateRemoteDatabase(i,self.station)
                        rp.setInterval()
                        rp.update()
                except  self.pymongo.errors.PyMongoError as e:
                    message = str(e)
                    self.d[message] += 1 
                    self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UNABLE TO UPDATE  COLLECTIONS IN REMOTE DATABASE ')
                    self.logging.error(self.file+"  UNABLE TO UPDATE COLLECTIONS IN REMOTE DATABASE ", exc_info=True)
                else:
                    self.syslog.syslog(self.syslog.LOG_ERR, self.file+', FINISHED  COLLECTIONS IN REMOTE DATABASE ')
                    self.logging.error(self.file+"  FINISHED UPDATING COLLECTIONS IN REMOTE DATABASE ", exc_info=True)
                

                result = self.json.dumps(self.d) 
                self.client.publish(self.station,self.time.ctime()+" update finished with "+result)
                self.start = self.time.time()
                interval = (self.start - self.end)/60
                self.syslog.syslog(self.syslog.LOG_ERR,self.file+', FINISHED UPDATING LOCAL AND REMOTE DATABASE IN '+str(interval)+" MINUTES")
                self.logging.warning(self.file+', FINISHED UPDATING LOCAL AND REMOTE DATABASE IN '+str(interval)+" MINUTES")





def main():
    update = UpdateHourly("station1")
    update.update()
    
    
if __name__ == "__main__":
    main()
    



