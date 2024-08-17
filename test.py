import time,datetime,os,requests,json,pytz,asyncio,schedule
import paho.mqtt.client as mqtt
from paho.mqtt import client as mqtt_asyncio
import RPi.GPIO as gpio
from celery import Celery

from celery.result import AsyncResult
app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')


def raindelay(payload):
    print('nice')

broker_ip = "10.10.9.4"
broker_port = 1883
username = "kili"
password = "kili"
cmdtopic = "kili/sagehouse/sprinklercontrol/command"
responsetopic = "kili/sagehouse/sprinklercontrol/response"
statetopic = "kili/sagehouse/sprinklercontrol/state"
commands = ['raindelay']


client = mqtt.Client()
client.username_pw_set(username, password)
#client.on_connect = on_connect
#client.on_message = on_message
client.connect(broker_ip, broker_port, 60)

def publish_message(topic,message):
    result = client.publish(topic, message)
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"published {message}")
    else:
        print("Failed to publish message")


#msg = {'command':'rain_delay','value':24,'units':'hours'}
msg = {'command':'stop','value':0,'units':'hours'}

publish_message(cmdtopic,json.dumps(msg))



from sprinklerer import add_sprinkle_task
from sprinklerer import cancel_tasks
sprank = [[1,1],[2,1],[3,1],[4,1],[5,1],[6,1]]
#add_sprinkle_task(sprank)
#time.sleep(3)
cancel_tasks()


