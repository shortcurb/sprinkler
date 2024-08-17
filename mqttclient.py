import datetime,json,os,traceback,time
import paho.mqtt.client as mqtt
import RPi.GPIO as gpio
from sprinklerer import add_sprinkle_task, cancel_tasks
from dotenv import load_dotenv

class MQTT_Client:
    def __init__(self):
        load_dotenv()
        self.statetopic = os.environ['mqttsprinklerstatetopic']
        self.cmdtopic = os.environ['mqttsprinklercmdtopic']
        self.responsetopic = os.environ['mqttsprinklerresposnetopic']
        self.commands = ['rain_delay','stop','manual_sprinkle']
        self.publish_message(self.responsetopic,'Starting MQTT client listener')
        self.publish_message(self.cmdtopic,'Listening for commands here')

    def connect_mqtt(self):
        self.broker_ip = os.environ['mqttbroker']
        self.broker_port = int(os.environ['mqttport'])
        self.username = os.environ['mqttusername']
        self.password = os.environ['mqttpassword']
        self.statetopic = os.environ['mqttsprinklerstatetopic']
        self.client = mqtt.Client()
        self.client.on_message = self.on_message
        self.client.username_pw_set(self.username, self.password)
        self.client.connect(self.broker_ip, self.broker_port, 60)

    def publish_message(self,topic,message):
        try:
            result = self.client.publish(topic, message)
            result.wait_for_publish()
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"Publish failed with return code {result.rc}. Attempting to reconnect.")
                self.connect_mqtt()
                self.publish_message(topic, message)
            else:
                print("Message published successfully")
        except TypeError:
            self.publish_message(topic,json.dumps(message,default=str,indent=2))
        except Exception as e:
            traceback.print_exc()
            self.connect_mqtt()
            self.publish_message(topic, message)
      
    def rain_delay(self,payload):
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
        self.publish_message(self.responsetopic,{'received_cmd':'stop','response':f"stopping, manual delay until {manual['manual_delay_expiration']}"})

    def manual_sprinkle(self,payload):
    #    payload looks like this: msg = {'command':'manual_sprinkle':'value':[[1,5],[2,7],[3,4]],'units':None}
        add_sprinkle_task(payload['value'])
        self.publish_message(self.responsetopic,{'received_cmd':'manual_sprinkle','response':f"manually sprinking {payload['value']}"})



    def subcribe_loop(self):
        self.client.subscribe(self.cmdtopic)
        self.client.loop_forever()

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to broker")
        else:
            print("Connection failed with code", rc)

    def on_message(self,client, userdata, msg):
        try:
            payload = msg.payload.decode()
            print(f"Received message: {payload} on topic: {msg.topic}")
            if msg.topic == self.cmdtopic:
                payload = json.loads(payload)
                if payload['command'] in self.commands:
                    getattr(self,payload['command'])(payload)
#                    globals()[payload['command']](payload)
        except Exception as e:
            print(e)



mc =MQTT_Client()
mc.subcribe_loop()







