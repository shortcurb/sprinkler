import datetime,json,os,traceback,time
import paho.mqtt.client as mqtt
import RPi.GPIO as gpio
from sprinklerer import add_sprinkle_task, cancel_tasks
from dotenv import load_dotenv

"""
There's something fucky going on with the MQTT broker connection. 
Its stuck in some kind of loop where it doesn't want to connect
Idk, troubleshoot later. Gotta make food now
"""


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

        self.commands = ['rain_delay','stop','manual_sprinkle','remove_delay']

    def connect_mqtt(self):
        # Handle reconnection logic elsewhere, just purely connecting here
        if self.connected:
            print('Already connected')
            self.connected == True
        else:
            self.mqtt_client = mqtt.Client()
            self.mqtt_client.on_message = self.on_message
            self.mqtt_client.on_disconnect=self.on_disconnect
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.username_pw_set(self.username, self.password)
            self.mqtt_client.connect(self.broker_ip, self.broker_port, 60)
            self.connected = True

    def publish_message(self,topic,message):
        if isinstance(message,dict):
            message = json.dumps(message,default=str,indent=2)
        try:
            result = self.mqtt_client.publish(topic, message)
        except Exception as e:
            traceback.print_exc()
            self.connected = False
            self.on_disconnect()
            # Let other parts handle the reconnection logic

    def rain_delay(self,payload):
        print('rain delay')
    #    payload looks like this: msg = {'command':'rain_delay','value':24,'units':'hours'}
        now = int(time.time())
        with open('manualinfo.json','r') as file:
            manual = json.load(file)
        manual.update({
            'rain_delay_start' : datetime.datetime.now(),
            'rain_delay_end' : datetime.datetime.now() + datetime.timedelta(**{payload['units']:payload['value']}),
            'last_updated':now
            })
        with open('manualinfo.json','w') as file:
            json.dump(manual,file,indent=2,default = str)
        self.publish_message(self.responsetopic,{'received_cmd':'rain_delay','response':f"dlaying due to rain until {manual['rain_delay_end']}"})

    def stop(self,payload):
    #    payload looks like this: msg = {'command':'stop':'value':1,'units':'hours}
        cancel_tasks()
        with open('manualinfo.json','r') as file:
            manual = json.load(file)
        manual.update({
            'manual_delay' : True,
            'manual_delay_expiration':datetime.datetime.now() + datetime.timedelta(**{payload['units']:payload['value']})
            })
        with open('manualinfo.json','w') as file:
            json.dump(manual,file,indent=2,default = str)
        msg = {'received_cmd':'stop','response':f"stopping, manual delay until {manual['manual_delay_expiration']}"}
        self.publish_message(self.responsetopic,msg)

    def manual_sprinkle(self,payload):
    #    payload looks like this: msg = {'command':'manual_sprinkle':'value':[[1,5],[2,7],[3,4]],'units':None}
        add_sprinkle_task(payload['value'])
        self.publish_message(self.responsetopic,{'received_cmd':'manual_sprinkle','response':f"manually sprinking {payload['value']}"})

    def remove_delay(self,payload):
        print('removing delay')
        manualinfo = self.readfile('manualinfo.json')
        manualinfo.update({
            'manual_delay':False,
            'rain_delay_start':None,
            'rain_delay_end':None
            })
        self.write_json(manualinfo,'manualinfo.json','w')
        self.publish_message(self.responsetopic,{'received_cmd':'remove_delay','response':f"overwriting previous delays"})

    def write_json(self,file_data,file_name,mode):
        now = int(time.time())
        file_data.update({'written_at':now})
        with open(file_name,mode) as file:
            json.dump(file_data,file,indent=2,default=str)

    def readfile(self,filename):
        with open(filename,'r') as file:
            return(json.load(file))

    def subcribe_loop(self):
        self.on_disconnect('','','')
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





