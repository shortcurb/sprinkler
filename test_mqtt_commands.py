import datetime,json,os,time,traceback
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
        self.connect_mqtt()
        self.tester()

    def connect_mqtt(self):
        self.broker_ip = os.environ['mqttbroker']
        self.broker_port = int(os.environ['mqttport'])
        self.username = os.environ['mqttusername']
        self.password = os.environ['mqttpassword']
        self.statetopic = os.environ['mqttsprinklerstatetopic']
        self.client = mqtt.Client()
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

    def tester(self):
        a = {'command':'rain_delay','value':24,'units':'hours'}
        b = {'command':'stop','value':1,'units':'hours'}
        c = {'command':'manual_sprinkle','value':[[1,5],[2,7],[3,4]],'units':None}
        while True:
            for i in [a,b,c]:
                print(json.dumps(i,indent=2))
                self.publish_message(self.cmdtopic,json.dumps(i))
                time.sleep(4)
MQTT_Client()