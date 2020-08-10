

#import sys



#################################################################################################################################################
#                                                    CLASSES TO UPDATE REMOTE DATABASE AT SPECIFIC INTERVALS
#################################################################################################################################################


class UpdateRemoteDatabase:
    def __init__(self,interval,station):
                
        #Database connection
        import json
        import time
        import syslog
        import logging
        import urllib.parse
        import math as M
        import pymongo  as pym
        import paho.mqtt.client as mqtt
        from   pymongo import MongoClient
        from   collections import defaultdict

        self.syslog                     = syslog
        self.pymongo                    = pym
        self.logging                    = logging
        self.M                          = M
        self.time                       = time
        self.json                       = json
        self.type                       = "update class"
        self.interval                   = interval
        self.count                      = 0
        self.receivingCollectionName    = ""
        self.localCount                 = 0
        self.updatedToThisPoint         = 0
        self.timeVariable               = ""
        self.station                    = station
        self.username                   = urllib.parse.quote_plus(station)
        self.password                   = urllib.parse.quote_plus('msojAwsStation1')
        self.localMongo                 = MongoClient('mongodb://localhost:27017/')
        self.localdb                    = self.localMongo['data']
        self.remoteMongo                = MongoClient('mongodb://%s:%s@176.58.103.182:27017' % (self.username, self.password)) #MongoClient('mongodb://176.58.103.182:27017/')
        self.remotedb                   = self.remoteMongo['data']      
        self.sendingCollection          = self.localdb.test
        self.receivingCollection        = self.localdb.test
        self.client                     = mqtt.Client()
        self.client.on_connect          = self.on_connect
        self.client.on_message          = self.on_message
        self.d                          = defaultdict(int)
        self.file                       = "AWS: remoteUpdate.py"
        self.DESCENDING                 = 1
        self.ASCENDING                  = -1
        self.syslog.openlog(logoption=self.syslog.LOG_PID, facility=self.syslog.LOG_MAIL)
        #Error logging config
        self.logging.basicConfig(filename='/home/ubuntu/aws/main/app.log', filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')  #<----- update path for log file
        self.client.connect("localhost", 1883, 60)
        self.client.loop_start()


    def testConnection(self):
        result = ""
        #print("Testing connection to Remote Database")
        try:
        # The ismaster command is cheap and does not require auth.
            result = self.remoteMongo.admin.command('ismaster')
            self.logging.warning(self.file+" CONNECTED TO REMOTE SERVER ")
            
        except  self.pymongo.errors.PyMongoError as e:
            #print("Server not available")
            message = str(e)
            self.d[message] += 1 
                                        
            self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UNABLE TO CONNECT TO REMOTE DATABASE ')
            self.logging.error(self.file+"  UNABLE TO CONNECT TO REMOTE DATABASE ", exc_info=True)
            result = "Server not available"
        else:
            pass
        finally:
            return result



    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, userdata, rc):
        #print("Connected with result code "+str(rc))
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        self.client.subscribe(self.station+"mssg")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        pass
        #print(msg.topic+" "+str(msg.payload))




    def update(self):
        self.logging.warning(self.file+"  UPDATE STARTED: SENDING UPDATES TO REMOTE DATABASE")
        self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UPDATE STARTED: SENDING UPDATES TO REMOTE DATABASE')
        result = self.testConnection()        # TEST IF CONNECTION TO REMOTE DATABASE IS AVAILABLE
        if result != "Server not available":
            self.tryCatch()

        else:
            message = self.json.dumps(self.d) 
            self.client.publish(self.station,time.ctime()+" update finished with "+message)
            self.logging.warning(self.file+"  UPDATE ENDED: NO UPDATES SENT, UNABLE TO CONNECT TO REMOTE DATABASE")
            self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UPDATE ENDED: NO UPDATES SENT, UNABLE TO CONNECT TO REMOTE DATABASE')




    def tryCatch(self):
        try:
            # GET LATEST TIMESTAMP
            ts          = list(self.receivingCollection.find({"station":self.station},{"_id":0,"TimeStamp":1}).sort("TimeStamp", self.ASCENDING).limit(1))    

        except  self.pymongo.errors.PyMongoError as e:
            #print("Server not available")
            message = str(e)
            self.d[message] += 1 
            self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UNABLE TO CONNECT TO REMOTE DATABASE TO GET LATEST TIMESTAMP')
            self.logging.error(self.file+"  UNABLE TO CONNECT TO REMOTE DATABASE TO GET LATEST TIMESTAMP", exc_info=True)
            
        else:
            
            if len(ts)  < 1:
                upper_ts = 0      
            else:
                # CONVERT UPPER TIMESTAMP TO AN INTEGER
                upper_ts    = int(ts[0]["TimeStamp"])
                
            #COUNT AND PULL ALL DOCS FROM LOCAL DATABASE > upper_ts
            self.localCount = self.sendingCollection.count_documents({})
            
            # PULL ALL THE DOCS FROM LOCAL DATABASE
            
            try:
                docs            = list(self.sendingCollection.find({"TimeStamp":{"$gt":upper_ts}}))  
            
            except  self.pymongo.errors.PyMongoError as e:
                #print("Server not available")
                message = str(e)
                self.d[message] += 1 
                                        
                self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UNABLE TO CONNECT TO LOCAL DATABASE TO GET LATEST TIMESTAMP '+ message[0])
                self.logging.error(self.file+"  UNABLE TO CONNECT TO LOCAL DATABASE TO GET LATEST TIMESTAMP", exc_info=True)
            else:
                # NOTHING TO RUN
                amount = len(docs)
                if amount <1:
                    self.logging.warning(self.file+"  REMOTE DATABASE ALREADY UPTODATE")
                    self.syslog.syslog(self.syslog.LOG_ERR, self.file+', REMOTE DATABASE ALREADY UPTODATE')
                    
                else:
                    for i in range(amount):
                        
                        # SEND EACH DATA POINT TO REMOTE DATABASE 
                        try:
                            result = self.receivingCollection.insert_one(docs[i])   
                        
                        except  self.pymongo.errors.PyMongoError as e:
                           
                            message = str(e)
                            if message.startswith("E11000"):
                                self.d["E11000 duplicate key error"] += 1 
                                self.syslog.syslog(self.syslog.LOG_ERR, self.file+', REMOTE DATABASE NOT UPDATED: DUPLICATION ERROR '+ message[0])
                                self.logging.error(self.file+"  Exception occurred :  REMOTE DATABASE NOT UPDATED - DUPLICATION ERROR", exc_info=True)
                            else:
                                self.d[message] += 1 
                                self.syslog.syslog(self.syslog.LOG_ERR, self.file+', REMOTE DATABASE NOT UPDATED '+ message[0])
                                self.logging.error(self.file+"  Exception occurred :  REMOTE DATABASE NOT UPDATED", exc_info=True)
                        else:
                            # NOTHING TO RUN
                            pass
                        finally:
                            # NOTHING TO RUN
                            pass
            finally:
                # NOTHING TO RUN
                pass


            #GET LAST UPDATED TIMESTAMP FROM REMOTE SERVER AND UPDATE LOCAL DATABASE
            ts1                         = list(self.receivingCollection.find({"station":self.station},{"_id":0,"TimeStamp":1}).sort("TimeStamp", self.ASCENDING).limit(1))      # GET UPPER TIMESTAMP

            if len(ts1) < 1:
                self.logging.warning(self.file+"   LOCAL COLLECTION IS EMPTY, NOTHING TO UPDATE")
                self.syslog.syslog(self.syslog.LOG_ERR, self.file+', LOCAL COLLECTION IS EMPTY, NOTHING TO UPDATE')
                pass
            else:
                self.updatedToThisPoint     = int(ts1[0]["TimeStamp"])   
                self.localdb.remotetime.update_one({'station': "local"}, {"$set": {self.timeVariable: self.updatedToThisPoint}})

        finally:
            result = self.json.dumps(self.d) 
            self.client.publish(self.station,self.time.ctime()+" "+self.receivingCollectionName.upper()+" update finished with "+result)
            self.logging.warning(self.file+"  UPDATE ENDED: FINISHED SENDING UPDATES TO REMOTE DATABASE")
            self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UPDATE ENDED: FINISHED SENDING UPDATES TO REMOTE DATABASE')
           

            

    def setInterval(self):
        
        if self.interval == "five":
            self.sendingCollection          = self.localdb.fiveminute
            self.receivingCollection        = self.remotedb.fiveminute
            self.receivingCollectionName    = "fiveminute"
            self.timeVariable               = "fiveminute"

        elif self.interval == "ten":
            self.sendingCollection          = self.localdb.tenminute
            self.receivingCollection        = self.remotedb.tenminute
            self.receivingCollectionName    = "tenminute"            
            self.timeVariable               = "tenminute"

        elif self.interval == "hourly":
            self.sendingCollection          = self.localdb.hourly
            self.receivingCollection        = self.remotedb.hourly
            self.receivingCollectionName    = "hourly"         
            self.timeVariable               = "hourly"

        elif self.interval == "daily":
            self.sendingCollection          = self.localdb.daily
            self.receivingCollection        = self.remotedb.daily
            self.receivingCollectionName    = "daily"            
            self.timeVariable               = "daily"

        elif self.interval == "monthly":
            self.sendingCollection          = self.localdb.monthly
            self.receivingCollection        = self.remotedb.monthly
            self.receivingCollectionName    = "monthly"
            self.timeVariable               = "monthly"

        elif self.interval == "yearly":
            self.sendingCollection          = self.localdb.yearly
            self.receivingCollection        = self.remotedb.yearly
            self.receivingCollectionName    = "yearly"            
            self.timeVariable               = "yearly"





