import time,datetime,os,requests,json,pytz,asyncio,schedule
import paho.mqtt.client as mqtt
from paho.mqtt import client as mqtt_asyncio
import RPi.GPIO as gpio


zones = {
    1:{'name':'NE Corner', 'duration':5,'pin':17,'state':-1},
    2:{'name':'North Yard','duration':5,'pin':22,'state':-1},
    3:{'name':'NW Corner', 'duration':5,'pin':24,'state':-1},
    4:{'name':'East Yard', 'duration':5,'pin':25,'state':-1},
    5:{'name':'SW Yard',   'duration':5,'pin':16,'state':-1},
    6:{'name':'South Yard','duration':5,'pin':26,'state':-1},
}

gpio.setwarnings(False)
gpio.setmode(gpio.BCM)
for zoneinfo in zones.values():
    gpio.setup(zoneinfo['pin'],gpio.OUT)
    gpio.output(zoneinfo['pin'],gpio.LOW)

get_pin_state()
