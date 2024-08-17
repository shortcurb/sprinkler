from celery import Celery
from celery.result import AsyncResult
import RPi.GPIO as gpio
import json,math,time

"""
This is the meat and potatoes of the sprinkling system. It uses Celery and redis as a queueing system 
to coordinate turning the relays on and off. 
"""

app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

# One centralized place to manage whether on is LOW or on is HIGH
def on():
    return(gpio.LOW)
def off():
    return(gpio.HIGH)

def zones():
    zones = {
            1:{'name':'NE Corner', 'duration':7,'pin':17,'state':-1},
            2:{'name':'North Yard','duration':7,'pin':22,'state':-1},
            3:{'name':'NW Corner', 'duration':7,'pin':24,'state':-1},
            4:{'name':'East Yard', 'duration':7,'pin':25,'state':-1},
            5:{'name':'SW Yard',   'duration':7,'pin':16,'state':-1},
            6:{'name':'South Yard','duration':7,'pin':26,'state':-1},
        }
    return(zones)

def turnoff():
    gpio.setwarnings(False)
    gpio.setmode(gpio.BCM)
    for zoneinfo in zones().values():
        gpio.setup(zoneinfo['pin'],gpio.OUT)
        gpio.output(zoneinfo['pin'],off()) 

@app.task(bind=True)
def run_sprinklers(self,sprinkle):
    # zonetime looks like [[1,5],[2,7],[3,4]] a list of lists of zone #, sprinkler duration
    import time
    gpio.setwarnings(False)
    gpio.setmode(gpio.BCM)
    turnoff()
    zoners = zones()
    previouspin = zoners[1]['pin']
    for zoneduration in sprinkle:
        print(f"Sprinkling zone {zoneduration[0]} for {zoneduration[1]} minutes")
        if self.request.called_directly:  # Check if task is being revoked
            turnoff()
            break
        pin = zoners[zoneduration[0]]['pin']
        gpio.output(pin,on())
        rng = math.ceil(zoneduration[1]) * 60
        for i in range(rng):
            print('wait loop',i,'out of',rng)
            if self.request.called_directly:  # Check if task is being revoked
                turnoff()
                break
            time.sleep(1)
            if i > 3 and previouspin != pin:
                print('Overlapping to prevent water hammer!!')
                gpio.output(previouspin,off())
                previouspin = pin

    turnoff()
    return('Sprinkle complete')

def add_sprinkle_task(sprinkle):
    run_sprinklers.apply_async(args=[sprinkle])

def cancel_tasks():
    print('Cancelling tasks')
    i = app.control.inspect()
    try:
        for task in i.active()['celery@sprink']:
            print(task)
            app.control.revoke(task['id'],terminate=True)
    except: pass
    try:
        for task in i.scheduled()['celery@sprink']:
            print(task)
            app.control.revoke(task['id'],terminate=True)
    except: pass
    try:
        for task in i.reserved()['celery@sprink']:
            print(task)
            app.control.revoke(task['id'],terminate=True)
    except:pass
    turnoff()
