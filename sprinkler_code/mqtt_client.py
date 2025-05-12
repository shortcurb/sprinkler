import datetime,json,os,traceback,time,asyncio
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from redis_crud import ScheduleJob, ActiveJob, DataCrud
from gather_data import DataGather


class SprinkleClient:
    def __init__(self):
        load_dotenv()
        self.connected = False
        self.broker_ip = os.environ['mqttbroker']
        self.broker_port = int(os.environ['mqttport'])
        self.username = os.environ['mqttusername']
        self.password = os.environ['mqttpassword']

        self.statetopic = os.environ['mqttsprinklerstatetopic']
        self.cmdtopic = os.environ['mqttsprinklercmdtopic']
        self.responsetopic = os.environ['mqttsprinklerresposnetopic']

        self.commands = ['rain_delay','delay_stop','manual_sprinkle','remove_delay','gather_and_compute', 'skip_next','unskip_next']

    def connect_mqtt(self):
        # Handle reconnection logic elsewhere, just purely connecting here
        if self.connected:
            print('Already connected')
        else:
            self.mqtt_client = mqtt.Client()
            self.mqtt_client.on_message = self.on_message
            self.mqtt_client.on_disconnect=self.on_disconnect
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.username_pw_set(self.username, self.password)
            self.mqtt_client.connect(self.broker_ip, self.broker_port, 60)
            self.connected = True

    def publish_message(self,topic,message):
        if not hasattr(self,'mqtt_client'):
            self.connect_mqtt()

        if isinstance(message,dict):
            message = json.dumps(message,default=str,indent=2)
        try:
            result = self.mqtt_client.publish(topic, message)
        except Exception as e:
            traceback.print_exc()
            self.connected = False
            self.on_disconnect()
            # Let other parts handle the reconnection logic
    
    def gather_and_compute(self,payload):
        print('gathering and computing')
        now = int(time.time())
        sj = ScheduleJob()
        jobs = sj.get_all_jobs()
        job_imminent = False
        for job_id, job_info in jobs.items():
            if job_info['is_cancelled'] !=True and now>job_info['start_at'] and now < job_info['end_at']:
                job_imminent = True
        if job_imminent == False:
            dg = DataGather()
            asyncio.run(self._async_gather_and_compute(dg))

    async def _async_gather_and_compute(self, dg):
        weather_task = asyncio.create_task(dg.get_weather())
        solar_task = asyncio.create_task(dg.get_solar())
        await asyncio.gather(weather_task, solar_task)
        await dg.compute_next_run()

    def delay_stop(self,payload):
        #    payload looks like this: msg = {'command':'delay_stop','value':5,'units':'minutes'}
        # or payload looks like this: msg = {'command':'delay_stop','value':2,'units':'hours'}
        # A delay_stop <=10m is a delay, to be resumed
        # a delay_stop >10m is a stop, with cancellations
        sj = ScheduleJob()
        dc = DataCrud()
        now = int(time.time())
        delay_seconds = datetime.timedelta(**{payload['units']:payload['value']}).total_seconds()
        delay_until = int(now + delay_seconds)
        delay_until_dt = datetime.datetime.now() + datetime.timedelta(seconds = delay_seconds)
        delay_info = {'delay_stop_received_at':now,'delay_stop_until_s':delay_until,'delay_stop_until_dt':delay_until_dt}
        dc.update_data('delaystopinfo',delay_info)
        msg = "Delay/stop request received"
        for job_id,job_info in sj.get_all_jobs().items():
            # if the job isn't cancelled and ( the it starts before the delay or ends before the delay and it hasn't already completed)
            if (job_info['start_at'] < delay_until or job_info['end_at'] < delay_until) and job_info['is_cancelled'] != True and now<job_info['end_at']:
                if delay_seconds <=600:
                    sj.reschedule_existing_job(now,job_id, delay_seconds)
                    msg = f"Delaying until {delay_until_dt}, will resume then"
                else:
                    print('cancelling job',job_id)
                    sj.cancel_job(job_id,now)
                    msg = f"Cancelling jobs through {delay_until_dt}"
                print(msg)
        message = {'received_cmd':'stop','response':msg}
        self.publish_message(self.responsetopic,message)

    def skip_next(self,payload):
        sj = ScheduleJob()
        now = int(time.time())
        next_job_id = None
        next_job_info = {'start_at':now+10**10} 
        
        for job_id,job_info in sj.get_all_jobs().items():
            if job_info['is_cancelled'] == False and job_info['is_active'] == False and job_info['start_at'] > now and now < next_job_info['start_at']:
                next_job_id = job_id
                next_job_info = job_info
                
        print(next_job_id)
#        print('next_job_info',json.dumps(next_job_info,indent=2))
        sj.cancel_job(next_job_id,now)
        message = {'received_cmd':'skip_next','response':f"Skipping next job {next_job_id}"}
        self.publish_message(self.responsetopic,message)

    def unskip_next(self,payload):
        sj = ScheduleJob()
        now = int(time.time())
        next_job_id = None
        next_job_info = {'start_at':now+10**10} 
        for job_id,job_info in sj.get_all_jobs().items():
            if job_info['is_cancelled'] == True and job_info['start_at'] > now and now < next_job_info['start_at']:
                next_job_id = job_id
                next_job_info = job_info
        print(next_job_id)
        sj.uncancel_job(next_job_id)


    def remove_delay(self,payload):
        dc = DataCrud()
        dc.update_data('delaystopinfo',{})

    def manual_sprinkle(self,payload):
    #    payload looks like this: msg = {'command':'manual_sprinkle','value':[[1,5],[2,7],[3,4]],'units':None}
        sj = ScheduleJob()
        now = int(time.time())
        job_info = {
            'start_at':now,
            'zone_info':payload['value'],
            'source':'manual'
                    }
        try:
            job_id = sj.create_job(job_info)
            message = f"Manually created job with id {job_id}"
        except ValueError as e:
            message = e
        self.publish_message(self.responsetopic,{'received_cmd':'manual_sprinkle','response':message})

    def subcribe_loop(self):
#        self.on_disconnect('','','')
        self.connect_mqtt()
        self.mqtt_client.subscribe(self.cmdtopic)
        self.mqtt_client.loop_forever()

    def on_connect(self,client, userdata, flags, rc):
        if rc == 0:
            self.publish_message(self.statetopic,'Posting states here')
            self.publish_message(self.responsetopic,'Starting MQTT client listener')
            self.publish_message(self.cmdtopic,'Listening for commands here')
            print("Connected to broker")
        else:
            print("Connection failed with code", rc)

    def on_disconnect(self,client=None,userdata=None,rc=None):
        self.connected = False
        print('Unexpected disconnection from MQTT broker')
        while self.connected == False:
            try:
                self.connect_mqtt() # connect_mqtt handles setting self.connected to True
                # self.connect_mqtt will also raise an exception if if fails, so put the sleep in the except
                self.subscribe_loop()
            except KeyboardInterrupt:
                return
            except: 
                print('Waiting 5 seconds then attempting reconnection')
#                traceback.print_exc()
                time.sleep(5)

    def on_message(self,client, userdata, msg):
        try:
            payload = msg.payload.decode()
            print(f"Received message: {payload} on topic: {msg.topic}")
            if msg.topic == self.cmdtopic:
                payload = json.loads(payload)
                if payload['command'] in self.commands:
                    getattr(self,payload['command'])(payload) # This calls the given function and executes it with the payload as an argument
        except json.decoder.JSONDecodeError:
            # Only pay attention to json-encoded data
            pass
        except KeyboardInterrupt:
            return
        except Exception as e:
            traceback.print_exc()


if '__main__' == __name__:
    mc = SprinkleClient()
    mc.subcribe_loop()



"""
This thing will have to consist of three separate entities that work together, probably through redis. 
So the structure of data in redis is very important
The sprinkler should handle everything related to atruclly controlling the sprinklers
It will receive a data structure that has a datetime, a list of zones, 
If it has the datetime, is it then the scheduler that decides when to sprinkle?
I guess, yeah. while True: check queue time.sleep(3)
Also I like the idea of keeping datetimes in unix epocs. Makes for easier math
Okay so the sprinkler has a while True loop that checks the queue
The data structure in the queue should be a list of jobs
Jobs should have a datetime start, "active" boolean "cancelled" boolean,
maybe "source" for scheduled vs manual
also datetimes for scheduled at, cancelled at, ran at, etc
Each job needs a unique hash as well

There will also be a redis structure about the current job
When the current job is being executed, it should load that job
into this structure, and update it regularly with current zone,  
time sprinkled, time remaining, next zone, previous zone, expected end datetime
Maybe a cancelled boolean here as well? No.
The cancellation boolean live in the job and the tri-second sprinklerer check the job structure
for cancellation info every 3s?

Then also set up the weather and solar info getting to run via crontab and update redis
And a different (or maybe the same module, different functions?) function
will add jobs to the queue based on solar and weather data
Maybe this system will also clear old jobs out of the queue that are older than e.x. a week?


How do I queue upcoming jobs and make sure they don't overlap?
Maybe I need an intermediary CRUD system that will sit between the job queue
And the systems that might want to CRUD on it?
Yeah I ike that plan
"""




"""

    def delay(self,payload):
    #    payload looks like this: msg = {'command':'delay','value':5,'units':'minutes'}
        now = time.time()
        delay_seconds = datetime.timedelta(**{payload['units']:payload['value']}).total_seconds()
        delay_until = int(now + delay_seconds)
        delay_until_dt = datetime.datetime.now() + datetime.timedelta(**{payload['units']:payload['value']})
        sj = ScheduleJob()
        # don't worry about managing the jobs here
        for job_id,job_info in sj.get_all_jobs():
            if (job_info['start_at'] < delay_until or job_info['end_at'] < delay_until) and job_info['is_cancelled'] != True:
                if job_info['is_active'] == True:
                    print('this is where the computation comes in')
                else:
                    self.sj.reschedule_existing_job(now,job_id,delay_seconds)
            else:
                # really pass, the delay doesn't affect it
                pass
        

    again, what's the interaction between delay and stop?
    A delay implies a resumption,stop implies no resumption
    Maybe have one receipt payload that splits?
    If the payload indicates a timeframe greater than 10 minutes, consider it a stop?
    I think that's it. A delay is, for example to get off the deck when the sprinklers start
    A stop is, e.g. a rain stop or you need to do maintainence on the sprinkler system


 
    def stop(self,payload):
    #    payload looks like this: msg = {'command':'stop':'value':1,'units':'hours}
        now = time.time()
        delay_seconds = datetime.timedelta(**{payload['units']:payload['value']}).total_seconds()
        delay_until = int(now + delay_seconds)
        delay_until_dt = datetime.datetime.now() + datetime.timedelta(**{payload['units']:payload['value']})
        sj = ScheduleJob()
        all_jobs = sj.get_all_jobs()
        for job_id,job_info in all_jobs.items():
            if job_info['start_at'] < delay_until:
                job_info.update({
                    'is_cancelled':True,
                    'cancelled_at':now
                })
                sj.update_job(job_id,job_info)

        # Cancel by creating an immediate job for zone 7 to have a duation of the requested delay_seconds
        # add in stuff about skip for a time to the redis structure that holds that

        msg = {'received_cmd':'stop','response':f"stopping, manual delay until {delay_until_dt}"}
        self.publish_message(self.responsetopic,msg)


"""