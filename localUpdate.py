


#################################################################################################################################################
#                                                                                                                                               #
#                                                    CLASSES TO UPDATE DATABASE AT SPECIFIC INTERVALS                                           #
#                                                                                                                                               #
#################################################################################################################################################



class UpdateDatabase:

    def __init__(self,interval,station):
        
        import json
        import time
        import logging
        import pymongo as pym
        import syslog
        #Database connection
        import paho.mqtt.client as mqtt
        from pymongo import MongoClient
        from collections import defaultdict
        import math as M

        self.syslog                     = syslog       
        self.M                          = M
        self.pymongo                    = pym
        self.logging                    = logging
        self.time                       = time
        self.json                       = json
        self.clientMongo                = MongoClient('mongodb://localhost:27017/')
        self.db                         = self.clientMongo['data']
        self.type                       = "update class"
        self.interval                   = interval
        self.sendingCollection          = self.db.test
        self.receivingCollection        = self.db.test
        self.receivingCollectionName    = ""
        self.now                        = 0
        self.boundaries                 = []
        self.updatedToThisPoint         = 0
        self.timeVariable               = ""
        self.docsUpdatedCount           = 0
        self.station                    = station
        self.client                     = mqtt.Client()
        self.client.on_connect          = self.on_connect
        self.client.on_message          = self.on_message
        self.d                          = defaultdict(int)
        self.file                       = "AWS: localUpdate.py"
        self.DESCENDING                 = 1
        self.ASCENDING                  = -1
        self.syslog.openlog(logoption=self.syslog.LOG_PID, facility=self.syslog.LOG_MAIL)
        #Error logging config
        self.logging.basicConfig(filename='/home/ubuntu/aws/main/app.log', filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')  #<----- update path for log file

        self.client.connect("localhost", 1883, 60)
        self.client.loop_start()

    def pt(self):
        print("timeVariable   ",self.timeVariable  ,"self.interval  ",self.interval  ,"self.sendingCollection ",self.sendingCollection," self.receivingCollection  ", self.receivingCollection ," self.receivingCollectionName ",self.receivingCollectionName )


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




    def countIsZero(self):
        
        result = self.testConnection()        # TEST IF CONNECTION TO LOCAL DATABASE IS AVAILABLE
        if result != "Server not available":
            try:
                #self.count  = int(self.sendingCollection.count_documents({}))              # COUNT HOW MANY DOCUMENTS ARE TO BE SENT TO THE AGGREGATION BUCKET
                #ts          = list(self.sendingCollection.find({},{"_id":0,"TimeStamp":1}).sort("TimeStamp", self.ASCENDING).limit(1))      # GET UPPER TIMESTAMP

                # GET LOWER TIMESTAMP
                ts         = list(self.sendingCollection.find({},{"_id":0,"TimeStamp":1}).sort("TimeStamp", self.DESCENDING).limit(1))
                
            except  self.pymongo.errors.PyMongoError as e:
                message = str(e)
                self.d[message] += 1 
                self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UNABLE TO CONNECT TO LOCAL DATABASE TO GET LATEST TIMESTAMP')
                self.logging.error(self.file+"  UNABLE TO CONNECT TO LOCAL DATABASE TO GET LATEST TIMESTAMP", exc_info=True)
            else:
                if len(ts) == 0 :
                    self.syslog.syslog(self.syslog.LOG_ERR, self.file+', '+self.timeVariable.upper()+' COLLECTION IN LOCAL DATABASE IS EMPTY, NOTHING TO UPDATE')
                    self.logging.warning(self.file+', '+self.timeVariable.upper()+' COLLECTION IN LOCAL DATABASE IS EMPTY, NOTHING TO UPDATE')
                    
                else:
                    #upper_ts    = int(ts[0]["TimeStamp"])                                      # CONVERT UPPER TIMESTAMP TO AN INTEGER
                    self.updatedToThisPoint    = int(ts[0]["TimeStamp"])                        # CONVERT LOWER TIMESTAMP TO AN INTEGER

                    if self.interval == "five":
                        self.gen5MinBoundaries()
                    elif self.interval == "ten":
                        self.gen10MinBoundaries()
                    elif self.interval == "hourly":
                        self.genHourBoundaries()
                    elif self.interval == "daily":
                        self.genDayBoundaries()
                    elif self.interval == "monthly":
                        self.genMonthBoundaries()
                    elif self.interval == "yearly":
                        self.genYearBoundaries()
                    else:
                        self.syslog.syslog(self.syslog.LOG_ERR, self.file+' '+self.timeVariable.upper()+' INVALID TIME INTERVAL, FOR COLLECTION IN LOCAL DATABASE; NOTHING TO UPDATE')
                        self.logging.warning(self.file+' '+self.timeVariable.upper()+' INVALID TIME INTERVAL, FOR COLLECTION IN LOCAL DATABASE; NOTHING TO UPDATE')
                        

                    if (len(self.boundaries) <=  1) :
                        self.syslog.syslog(self.syslog.LOG_ERR, self.file+' '+self.timeVariable.upper()+' COLLECTION IN LOCAL DATABASE ALREADY UPTODATE')
                        self.logging.warning(self.file+' '+self.timeVariable.upper()+' COLLECTION IN LOCAL DATABASE ALREADY UPTODATE')
                        

                    else:
                        pipeline        =  [ {"$match":{"TimeStamp":{"$gte":self.updatedToThisPoint}}}, { "$bucket": { "groupBy": "$TimeStamp", "boundaries":self.boundaries  , "default": "Other", "output": { "count": { "$sum": 1 }, "result" :{"$push": { "TimeStamp":"$TimeStamp","DewPoint":"$DewPoint","lat":"$location.lat","long":"$location.long","InTemp":"$Temperature.InTemp" ,"OutTemp":"$Temperature.OutTemp", "HighOutTemp":"$Temperature.HighOutTemp" ,"LowOutTemp":"$Temperature.LowOutTemp","Barometer":"$Barometer","HeatIndex":"$HeatIndex","WindChill":"$WindChill","rain":"$Rain.rain","RainRate":"$Rain.RainRate","Radi":"$Radiation.Radiation", "HighRadi":"$Radiation.HighRadiation","InHumid":"$Humidity.InHumidity", "OutHumid":"$Humidity.OutHumidity","ET":"$ET","uv":"$UV.UV", "hiUV":"$UV.HighUV","WindSpeed":"$Wind.WindSpeed", "WindDirection":"$Wind.WindDirection", "WindGust":"$Wind.WindGust","ConsoleBatteryVoltage":"$ConsoleBatteryVoltage","Sunrise":"$Sunrise","Sunset":"$Sunset"     } } }}  }, {"$match": { "_id": {"$ne": "Other"}  } },{        "$set":{"station":self.station,"TimeStamp":{"$max":"$result.TimeStamp"},"location.lat":{"$max":"$result.lat"},"location.long":{"$max":"$result.long"},"Temperature.InTemp":{"$avg":"$result.InTemp" },"Temperature.OutTemp":{"$avg":"$result.OutTemp" },"Temperature.HighOutTemp":{"$avg":"$result.HighOutTemp" },"Temperature.LowOutTemp":{"$avg":"$result.LowOutTemp" },"Barometer":{"$avg":"$result.Barometer"},"HeatIndex":{"$avg":"$result.HeatIndex"},"WindChill":{"$avg":"$result.WindChill"},"DewPoint":{"$avg":"$result.DewPoint"},"Rain.rain":{"$max":"$result.rain"},"Rain.RainRate":{"$max":"$result.RainRate"},"Radiation.Radiation":{"$avg":"$result.Radi"}, "Radiation.HighRadiation":{"$avg":"$result.HighRadi"},"Humidity.InHumid":{"$avg":"$result.InHumid"}, "Humidity.OutHumid":{"$avg":"$result.OutHumid"},"ET":{"$max":"$result.ET"},"UV.UV":{"$avg":"$result.uv"}, "UV.HighUV":{"$avg":"$result.HighUV"},"Wind.WindSpeed":{"$avg":"$result.WindSpeed"}, "Wind.WindDirection":{"$avg":"$result.WindDirection"}, "Wind.WindGust":{"$avg":"$result.WindGust"},"ConsoleBatteryVoltage":{"$min":"$result.ConsoleBatteryVoltage"},"Sunrise":{"$max":"$result.Sunrise"},"Sunset":{"$max":"$result.Sunset"} }},{"$project":{"_id":0,"station":1,"TimeStamp":1,"location":1,"Temperature":1,"Barometer":1,"HeatIndex":1,"WindChill":1,"DewPoint":1,"Rain":1,"Radiation":1,"Humidity":1,"ET":1,"UV":1,"Wind":1,"ConsoleBatteryVoltage":1,"Sunrise":1,"Sunset":1}},{ "$merge" : { "into": { "db": "data", "coll": self.receivingCollectionName }, "on": "TimeStamp",  "whenMatched": "keepExisting", "whenNotMatched": "insert" } } ]
                        self.tryCatch(pipeline) 


        else:
            self.logging.warning(self.file+' '+self.timeVariable.upper()+' UPDATE ENDED: NO UPDATES SENT, UNABLE TO CONNECT TO LOCAL DATABASE')
            self.syslog.syslog(self.syslog.LOG_ERR, self.file+' '+self.timeVariable.upper()+' UPDATE ENDED: NO UPDATES SENT, UNABLE TO CONNECT TO LOCAL DATABASE')




    def update(self):
    
        result = self.testConnection()        # TEST IF CONNECTION TO LOCAL DATABASE IS AVAILABLE
        if result != "Server not available":
            try:
                #DocsCount           = int(self.sendingCollection.count_documents({"TimeStamp":{"$gte":self.updatedToThisPoint}}))                       # COUNT HOW MANY DOCUMENTS ARE TO BE SENT TO THE AGGREGATION BUCKET

                # GET UPPER TIMESTAMP FROM SENDING COLLECTION
                ts                  = list(self.sendingCollection.find({},{"_id":0,"TimeStamp":1}).sort("TimeStamp", self.ASCENDING).limit(1))
                
            except  self.pymongo.errors.PyMongoError as e:
                message = str(e)
                self.d[message] += 1 
                self.syslog.syslog(self.syslog.LOG_ERR, self.file+','+self.timeVariable.upper()+' UNABLE TO CONNECT TO LOCAL DATABASE TO GET LATEST TIMESTAMP')
                self.logging.error(self.file+' '+self.timeVariable.upper()+'  UNABLE TO CONNECT TO LOCAL DATABASE TO GET LATEST TIMESTAMP')
            else:
                if len(ts) == 0 :
                    self.syslog.syslog(self.syslog.LOG_ERR, self.file+','+self.timeVariable.upper()+' COLLECTION IN LOCAL DATABASE IS EMPTY, NOTHING TO UPDATE')
                    self.logging.warning(self.file+' '+self.timeVariable.upper()+' COLLECTION IN LOCAL DATABASE IS EMPTY, NOTHING TO UPDATE')
                else:
                    #upper_ts            = int(ts[0]["TimeStamp"])
                    #TimeStamp           = self.updatedToThisPoint + 60
                    '''
                    for i in range(1,DocsCount+1):
                        boundaries.append(TimeStamp)

                        if TimeStamp + self.boundaryInterval  > upper_ts :
                            break
                        TimeStamp       = TimeStamp + self.boundaryInterval
                    '''
                    if self.interval == "five":
                        self.gen5MinBoundaries()
                    elif self.interval == "ten":
                        self.gen10MinBoundaries()
                    elif self.interval == "hourly":
                        self.genHourBoundaries()
                    elif self.interval == "daily":
                        self.genDayBoundaries()
                    elif self.interval == "monthly":
                        self.genMonthBoundaries()
                    elif self.interval == "yearly":
                        self.genYearBoundaries()
                    else:
                        self.syslog.syslog(self.syslog.LOG_ERR, self.file+','+self.timeVariable.upper()+' INVALID TIME INTERVAL, FOR COLLECTION IN LOCAL DATABASE; NOTHING TO UPDATE')
                        self.logging.warning(self.file+' '+self.timeVariable.upper()+' INVALID TIME INTERVAL, FOR COLLECTION IN LOCAL DATABASE; NOTHING TO UPDATE')
                        
                        
                    
                    if (len(self.boundaries) <=  1) :
                        self.syslog.syslog(self.syslog.LOG_ERR, self.file+', '+self.timeVariable.upper()+' COLLECTION IN LOCAL DATABASE UPTODATE')
                        self.logging.warning(self.file+' '+self.timeVariable.upper()+' COLLECTION IN LOCAL DATABASE UPTODATE')

                    else:
                        pipeline        =  [ {"$match":{"TimeStamp":{"$gte":self.updatedToThisPoint,"$lte":self.now}}}, { "$bucket": { "groupBy": "$TimeStamp", "boundaries":self.boundaries  , "default": "Other", "output": { "count": { "$sum": 1 }, "result" :{"$push": { "TimeStamp":"$TimeStamp","DewPoint":"$DewPoint","lat":"$location.lat","long":"$location.long","InTemp":"$Temperature.InTemp" ,"OutTemp":"$Temperature.OutTemp", "HighOutTemp":"$Temperature.HighOutTemp" ,"LowOutTemp":"$Temperature.LowOutTemp","Barometer":"$Barometer","HeatIndex":"$HeatIndex","WindChill":"$WindChill","rain":"$Rain.rain","RainRate":"$Rain.RainRate","Radi":"$Radiation.Radiation", "HighRadi":"$Radiation.HighRadiation","InHumid":"$Humidity.InHumidity", "OutHumid":"$Humidity.OutHumidity","ET":"$ET","uv":"$UV.UV", "hiUV":"$UV.HighUV","WindSpeed":"$Wind.WindSpeed", "WindDirection":"$Wind.WindDirection", "WindGust":"$Wind.WindGust","ConsoleBatteryVoltage":"$ConsoleBatteryVoltage","Sunrise":"$Sunrise","Sunset":"$Sunset"     } } }}  }, {"$match": { "_id": {"$ne": "Other"}  } },{        "$set":{"station":self.station,"TimeStamp":{"$max":"$result.TimeStamp"},"location.lat":{"$max":"$result.lat"},"location.long":{"$max":"$result.long"},"Temperature.InTemp":{"$avg":"$result.InTemp" },"Temperature.OutTemp":{"$avg":"$result.OutTemp" },"Temperature.HighOutTemp":{"$avg":"$result.HighOutTemp" },"Temperature.LowOutTemp":{"$avg":"$result.LowOutTemp" },"Barometer":{"$avg":"$result.Barometer"},"HeatIndex":{"$avg":"$result.HeatIndex"},"WindChill":{"$avg":"$result.WindChill"},"DewPoint":{"$avg":"$result.DewPoint"},"Rain.rain":{"$max":"$result.rain"},"Rain.RainRate":{"$max":"$result.RainRate"},"Radiation.Radiation":{"$avg":"$result.Radi"}, "Radiation.HighRadiation":{"$avg":"$result.HighRadi"},"Humidity.InHumid":{"$avg":"$result.InHumid"}, "Humidity.OutHumid":{"$avg":"$result.OutHumid"},"ET":{"$max":"$result.ET"},"UV.UV":{"$avg":"$result.uv"}, "UV.HighUV":{"$avg":"$result.HighUV"},"Wind.WindSpeed":{"$avg":"$result.WindSpeed"}, "Wind.WindDirection":{"$avg":"$result.WindDirection"}, "Wind.WindGust":{"$avg":"$result.WindGust"},"ConsoleBatteryVoltage":{"$min":"$result.ConsoleBatteryVoltage"},"Sunrise":{"$max":"$result.Sunrise"},"Sunset":{"$max":"$result.Sunset"} }},{"$project":{"_id":0,"station":1,"TimeStamp":1,"location":1,"Temperature":1,"Barometer":1,"HeatIndex":1,"WindChill":1,"DewPoint":1,"Rain":1,"Radiation":1,"Humidity":1,"ET":1,"UV":1,"Wind":1,"ConsoleBatteryVoltage":1,"Sunrise":1,"Sunset":1}},{ "$merge" : { "into": { "db": "data", "coll": self.receivingCollectionName }, "on": "TimeStamp",  "whenMatched": "keepExisting", "whenNotMatched": "insert" } } ] 
                        self.tryCatch(pipeline)

        else:
            
            self.logging.warning(self.file+"  UPDATE ENDED: NO UPDATES SENT, UNABLE TO CONNECT TO LOCAL DATABASE")
            self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UPDATE ENDED: NO UPDATES SENT, UNABLE TO CONNECT TO LOCAL DATABASE')





    def updater(self):
        self.now = self.time.time()
        result = self.testConnection()        # TEST IF CONNECTION TO LOCAL DATABASE IS AVAILABLE
        if result != "Server not available":
            try:
                #FIND WHEN THE LAST UPDATE WAS SENT BY CHECKING TIME COLLECTION
                self.updatedToThisPoint     = self.db.time.find_one({"station":"local"}, {"_id":0,self.timeVariable: 1})[self.timeVariable]

                # GET UPPER TIMESTAMP FROM SENDING COLLECTIONS
                test                        = list(self.sendingCollection.find({},{"_id":0,"TimeStamp":1}).sort("TimeStamp", self.ASCENDING).limit(1))


            except  self.pymongo.errors.PyMongoError as e:           
                message = str(e)
                self.d[message] += 1 
                self.syslog.syslog(self.syslog.LOG_ERR, self.file+',updater(self): '+self.timeVariable.upper()+' ERROR GETTING LASTEST TIMESTAMP  '+ message[0])
                self.logging.error(self.file+' updater(self): '+self.timeVariable.upper()+' ERROR GETTING LASTEST TIMESTAMP', exc_info=True)

            else:
                if len(test) < 1:
                    # SENDING COLLECTIONS IS EMPTY, NOTHING TO UPDATE FROM
                    self.syslog.syslog(self.syslog.LOG_ERR, self.file+',updater(self): '+self.timeVariable.upper()+' LOCAL COLLECTION IS EMPTY, NOTHING TO UPDATE')
                    self.logging.warning(self.file+' updater(self): '+self.timeVariable.upper()+' LOCAL COLLECTION IS EMPTY, NOTHING TO UPDATE')
                else:
                    
                            
                    if self.updatedToThisPoint  == 0:
                        self.countIsZero()
                    elif self.updatedToThisPoint > 0:
                        self.update()
            finally:
                self.docsUpdatedCount -= 1
                if self.docsUpdatedCount < 0:
                    self.docsUpdatedCount = 0
                self.logging.warning(self.file+"  UPDATE ENDED: SENT "+str(self.docsUpdatedCount)+" DOCUMENTS TO "+self.receivingCollectionName.upper()+" COLLECTION ")
                self.syslog.syslog(self.syslog.LOG_ERR, self.file+"  UPDATE ENDED: SENT "+str(self.docsUpdatedCount)+" DOCUMENTS TO "+self.receivingCollectionName.upper()+" COLLECTION ")
                message = self.json.dumps(self.d) 
                self.client.publish(self.station,self.time.ctime()+" UPDATE FINISHED: ADDED "+str(self.docsUpdatedCount)+" DOCUMENTS TO "+self.receivingCollectionName.upper()+" COLLECTION "+message )
                self.client.publish(self.station,self.time.ctime()+" ,BOUNDARIES: "+str(self.boundaries))



    def tryCatch(self,pipeline):
        try:
            # SEND AGGREGATION REQUEST TO DATABASE TO UPDATE RECEIVING COLLECTION WITH DATA FROM SENDING COLLECTION
            document    = list(self.sendingCollection.aggregate(pipeline))
            
        except  self.pymongo.errors.PyMongoError as e:           
            message = str(e)
            if message.startswith("E11000"):
                self.d["E11000 duplicate key error"] += 1 
                self.syslog.syslog(self.syslog.LOG_ERR, self.file+', LOCAL DATABASE NOT UPDATED: DUPLICATION ERROR IN AGGREDATION PIPELINE '+ message[0])
                self.logging.error(self.file+"  Exception occurred :  LOCAL DATABASE NOT UPDATED - DUPLICATION ERROR IN AGGREDATION PIPELINE ", exc_info=True)
            else:
                self.d[message] += 1 
                self.syslog.syslog(self.syslog.LOG_ERR, self.file+', ERROR IN AGGREDATION PIPELINE '+ message[0])
                self.logging.error(self.file+" ERROR IN AGGREDATION PIPELINE", exc_info=True)
        else:
            # GET UPPER TIMESTAMP FROM RECEIVING COLLECTION, AFTER AGGREGATION
            ts          = list(self.receivingCollection.find({},{"_id":0,"TimeStamp":1}).sort("TimeStamp", self.ASCENDING).limit(1))

            # CONVERT UPPER TIMESTAMP TO AN INTEGER
            upper_ts    = int(ts[0]["TimeStamp"])
            
            
            # UPDATE TIME COLLECTION WITH LATEST TIMESTAMPS
            self.db.time.update_one({'station': "local"}, {"$set": {self.timeVariable: upper_ts}})       #UPDATE DATABASE TIME MANAGENT COLLECTION. THIS COLLECTION KEEPS TRACK OF THE LAST TIME EVERY OTHER COLLECTION WAS UPDATED
            
            # COUNT AMOUNT OF DOCUMENTS ADDED TO COLLECTION
            try:
                self.docsUpdatedCount          = int(self.receivingCollection.count_documents({"TimeStamp":{"$gte":self.updatedToThisPoint,"$lte":upper_ts}}))                       # COUNT HOW MANY DOCUMENTS ARE TO BE SENT TO THE AGGREGATION BUCKET
            except  self.pymongo.errors.PyMongoError as e:           
                message = str(e)
                self.d[message] += 1 
                self.syslog.syslog(self.syslog.LOG_ERR, self.file+' UNABLE TO GET COUNT FROM '+self.timeVariable.upper()+' AFTER AGGREDATION '+ message[0])
                self.logging.error(self.file+'  UNABLE TO GET COUNT FROM '+self.timeVariable.upper()+' AFTER AGGREDATION ', exc_info=True)
            else:
                self.syslog.syslog(self.syslog.LOG_ERR, self.file+'  GOT COUNT FROM '+self.timeVariable.upper()+' COLLECTION AFTER AGGREDATION ')
                self.logging.error(self.file+' GOT COUNT FROM '+self.timeVariable.upper()+' COLLECTION AFTER AGGREDATION ')
            finally:
                pass

        finally:
            self.logging.warning(self.file+'  UPDATE ENDED: FINISHED SENDING UPDATES TO '+self.timeVariable.upper()+' COLLECTION IN LOCAL DATABASE')
            self.syslog.syslog(self.syslog.LOG_ERR, self.file+', UPDATE ENDED: FINISHED SENDING UPDATES TO '+self.timeVariable.upper()+' COLLECTION IN LOCAL DATABASE')



    def setInterval(self):
        if self.interval == "five":
            self.sendingCollection          = self.db.oneminute
            self.receivingCollection        = self.db.fiveminute
            self.receivingCollectionName    = "fiveminute"
            self.timeVariable               = "fiveminute"

        elif self.interval == "ten":
            self.sendingCollection          = self.db.oneminute
            self.receivingCollection        = self.db.tenminute
            self.receivingCollectionName    = "tenminute"
            self.timeVariable               = "tenminute"

        elif self.interval == "hourly":
            self.sendingCollection          = self.db.oneminute
            self.receivingCollection        = self.db.hourly
            self.receivingCollectionName    = "hourly"
            self.timeVariable               = "hourly"

        elif self.interval == "daily":
            self.sendingCollection          = self.db.hourly
            self.receivingCollection        = self.db.daily
            self.receivingCollectionName    = "daily"
            self.timeVariable               = "daily"

        elif self.interval == "monthly":
            self.sendingCollection          = self.db.daily
            self.receivingCollection        = self.db.monthly
            self.receivingCollectionName    = "monthly"
            self.timeVariable               = "monthly"

        elif self.interval == "yearly":
            self.sendingCollection          = self.db.monthly
            self.receivingCollection        = self.db.yearly
            self.receivingCollectionName    = "yearly"             
            self.timeVariable               = "yearly"




    def gen5MinBoundaries(self):
        # GET CURRENT TIMESTAMP OF NOW
        now = self.now
        
        # CONVERT CURRENT TIMESTAMP TO THE BEGINING OF PRESENT HOUR, THIS REPRESENTS UPPER TIMESTAMP FOR AGGREGATION
        timeStruct = self.time.localtime(now)
        nowTimeStruct = (timeStruct.tm_year,timeStruct.tm_mon,timeStruct.tm_mday,timeStruct.tm_hour,0,0,timeStruct.tm_wday,timeStruct.tm_yday,timeStruct.tm_isdst)
        now_ts = self.M.floor(self.time.mktime(nowTimeStruct))
        print("now ",self.time.ctime(now_ts))
        # self.updatedToThisPoint <--THIS REPRESENTS THE LOWER TIMESTAMP FOR AGGREGATION
        TimeStamp = self.updatedToThisPoint 

        # ZERO THE MINUTE POSITION
        timeStruct1 = self.time.localtime(TimeStamp)
        nowTimeStruct1 = (timeStruct1.tm_year,timeStruct1.tm_mon,timeStruct1.tm_mday,timeStruct1.tm_hour,0,1,timeStruct1.tm_wday,timeStruct1.tm_yday,timeStruct1.tm_isdst)
        TimeStamp = self.M.floor(self.time.mktime(nowTimeStruct1))

        # GENERATE BUCKET BOUNDARIES FOR AGGREGATION
        while TimeStamp <= now_ts +1:
            self.boundaries.append(TimeStamp)
            timeStruct1    = self.time.localtime(TimeStamp)
            nowTimeStruct1 = (timeStruct1.tm_year,timeStruct1.tm_mon,timeStruct1.tm_mday,timeStruct1.tm_hour,timeStruct1.tm_min+5,1,timeStruct1.tm_wday,timeStruct1.tm_yday,timeStruct1.tm_isdst)
            TimeStamp      = self.M.floor(self.time.mktime(nowTimeStruct1))




    def gen10MinBoundaries(self):
        # GET CURRENT TIMESTAMP OF NOW
        now = self.now
        
        # CONVERT CURRENT TIMESTAMP TO THE BEGINING OF PRESENT HOUR, THIS REPRESENTS UPPER TIMESTAMP FOR AGGREGATION
        timeStruct = self.time.localtime(now)
        nowTimeStruct = (timeStruct.tm_year,timeStruct.tm_mon,timeStruct.tm_mday,timeStruct.tm_hour,0,0,timeStruct.tm_wday,timeStruct.tm_yday,timeStruct.tm_isdst)
        now_ts = self.M.floor(self.time.mktime(nowTimeStruct))
        print("now ",self.time.ctime(now_ts))
        # self.updatedToThisPoint <--THIS REPRESENTS THE LOWER TIMESTAMP FOR AGGREGATION
        TimeStamp = self.updatedToThisPoint

        # ZERO THE MINUTE POSITION
        timeStruct1 = self.time.localtime(TimeStamp)
        nowTimeStruct1 = (timeStruct1.tm_year,timeStruct1.tm_mon,timeStruct1.tm_mday,timeStruct1.tm_hour,0,1,timeStruct1.tm_wday,timeStruct1.tm_yday,timeStruct1.tm_isdst)
        TimeStamp = self.M.floor(self.time.mktime(nowTimeStruct1))

        # GENERATE BUCKET BOUNDARIES FOR AGGREGATION
        while TimeStamp <= now_ts +1:
            self.boundaries.append(TimeStamp)
            timeStruct1 = self.time.localtime(TimeStamp)
            nowTimeStruct1 = (timeStruct1.tm_year,timeStruct1.tm_mon,timeStruct1.tm_mday,timeStruct1.tm_hour,timeStruct1.tm_min+10,1,timeStruct1.tm_wday,timeStruct1.tm_yday,timeStruct1.tm_isdst)
            TimeStamp = self.M.floor(self.time.mktime(nowTimeStruct1))




    def genHourBoundaries(self):
        # GET CURRENT TIMESTAMP OF NOW
        now = self.now
        
        # CONVERT CURRENT TIMESTAMP TO THE BEGINING OF PRESENT HOUR, THIS REPRESENTS UPPER TIMESTAMP FOR AGGREGATION
        timeStruct = self.time.localtime(now)
        nowTimeStruct = (timeStruct.tm_year,timeStruct.tm_mon,timeStruct.tm_mday,timeStruct.tm_hour,0,0,timeStruct.tm_wday,timeStruct.tm_yday,timeStruct.tm_isdst)
        now_ts = self.M.floor(self.time.mktime(nowTimeStruct))
        print("now ",self.time.ctime(now_ts))
        # self.updatedToThisPoint <--THIS REPRESENTS THE LOWER TIMESTAMP FOR AGGREGATION
        TimeStamp = self.updatedToThisPoint + 1


        # GENERATE BUCKET BOUNDARIES FOR AGGREGATION
        while TimeStamp <= now_ts +1:
            self.boundaries.append(TimeStamp)
            timeStruct1 = self.time.localtime(TimeStamp)
            nowTimeStruct1 = (timeStruct1.tm_year,timeStruct1.tm_mon,timeStruct1.tm_mday,timeStruct1.tm_hour+1,0,1,timeStruct1.tm_wday,timeStruct1.tm_yday,timeStruct1.tm_isdst)
            TimeStamp = self.M.floor(self.time.mktime(nowTimeStruct1))




    def genDayBoundaries(self):
        # GET CURRENT TIMESTAMP OF NOW
        now = self.now
        
        # CONVERT CURRENT TIMESTAMP TO 12AM OF PRESENT DAY, THIS REPRESENTS UPPER TIMESTAMP FOR AGGREGATION
        timeStruct = self.time.localtime(now)
        nowTimeStruct = (timeStruct.tm_year,timeStruct.tm_mon,timeStruct.tm_mday,0,0,0,timeStruct.tm_wday,timeStruct.tm_yday,timeStruct.tm_isdst)
        now_ts = self.M.floor(self.time.mktime(nowTimeStruct))
        print("now ",self.time.ctime(now_ts))
        # self.updatedToThisPoint <--THIS REPRESENTS THE LOWER TIMESTAMP FOR AGGREGATION
        TimeStamp = self.updatedToThisPoint + 1


        # GENERATE BUCKET BOUNDARIES FOR AGGREGATION
        while TimeStamp <= now_ts +1:
            self.boundaries.append(TimeStamp)
            timeStruct1 = self.time.localtime(TimeStamp)
            nowTimeStruct1 = (timeStruct1.tm_year,timeStruct1.tm_mon,timeStruct1.tm_mday+1,0,0,1,timeStruct1.tm_wday,timeStruct1.tm_yday,timeStruct1.tm_isdst)
            TimeStamp = self.M.floor(self.time.mktime(nowTimeStruct1))


           

    def genMonthBoundaries(self):
        # GET CURRENT TIMESTAMP OF NOW
        now = self.now

        # CONVERT CURRENT TIMESTAMP TO 12AM OF FIRST DAY IN PRESENT MONTH, THIS REPRESENTS UPPER TIMESTAMP FOR AGGREGATION
        timeStruct = self.time.localtime(now)
        nowTimeStruct = (timeStruct.tm_year,timeStruct.tm_mon,1,0,0,0,timeStruct.tm_wday,timeStruct.tm_yday,timeStruct.tm_isdst)
        now_ts = self.M.floor(self.time.mktime(nowTimeStruct))

        # self.updatedToThisPoint <--THIS REPRESENTS THE LOWER TIMESTAMP FOR AGGREGATION
        TimeStamp = self.updatedToThisPoint + 1


        # GENERATE BUCKET BOUNDARIES FOR AGGREGATION
        while TimeStamp <= now_ts +1:
            self.boundaries.append(TimeStamp)
            timeStruct1 = self.time.localtime(TimeStamp)
            nowTimeStruct1 = (timeStruct1.tm_year,timeStruct1.tm_mon+1,1,0,0,1,timeStruct1.tm_wday,timeStruct1.tm_yday,timeStruct1.tm_isdst)
            TimeStamp = self.M.floor(self.time.mktime(nowTimeStruct1))

           

    def genYearBoundaries(self):
        # GET CURRENT TIMESTAMP OF NOW
        now = self.now

        # CONVERT CURRENT TIMESTAMP TO 12AM OF FIRST DAY IN PRESENT YEAR, THIS REPRESENTS UPPER TIMESTAMP FOR AGGREGATION
        timeStruct = self.time.localtime(now)
        nowTimeStruct = (timeStruct.tm_year,1,1,0,0,0,timeStruct.tm_wday,timeStruct.tm_yday,timeStruct.tm_isdst)
        now_ts = self.M.floor(self.time.mktime(nowTimeStruct))

        # self.updatedToThisPoint <--THIS REPRESENTS THE LOWER TIMESTAMP FOR AGGREGATION
        TimeStamp = self.updatedToThisPoint + 1


        # GENERATE BUCKET BOUNDARIES FOR AGGREGATION
        while TimeStamp <= now_ts +1:
            self.boundaries.append(TimeStamp)
            timeStruct1 = self.time.localtime(TimeStamp)
            nowTimeStruct1 = (timeStruct1.tm_year+1,1,1,0,0,1,timeStruct1.tm_wday,timeStruct1.tm_yday,timeStruct1.tm_isdst)
            TimeStamp = self.M.floor(self.time.mktime(nowTimeStruct1))
