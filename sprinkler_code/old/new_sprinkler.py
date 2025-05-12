import time,datetime,os,requests,json,pytz,schedule,traceback
import paho.mqtt.client as mqtt
from sprinklerer import zones,add_sprinkle_task,turnoff,on,danger_test
from functools import partial
import RPi.GPIO as gpio
from dotenv import load_dotenv
from mqttclient import SprinkleClient


class Zones:
    def __init__(self):
        pass

    # One centralized place to manage whether on is LOW or on is HIGH
    def on(self):
    #    return(gpio.LOW) # no!
        return(gpio.HIGH) #confirmed!!
    def off(self):
        return(gpio.LOW) # confirmed!
    #    return(gpio.HIGH) # no!

    def zones(self):
        zones = {
                1:{'name':'NE Corner', 'duration':7,'pin':17,'state':-1},
                2:{'name':'North Yard','duration':7,'pin':22,'state':-1},
                3:{'name':'NW Corner', 'duration':7,'pin':24,'state':-1},
                4:{'name':'East Yard', 'duration':7,'pin':25,'state':-1},
                5:{'name':'SW Yard',   'duration':7,'pin':16,'state':-1},
                6:{'name':'South Yard','duration':7,'pin':26,'state':-1},
            }
        return(zones)

    def turnoff(self):
        gpio.setwarnings(False)
        gpio.setmode(gpio.BCM)
        for zoneinfo in zones().values(self):
            gpio.setup(zoneinfo['pin'],gpio.OUT)
            gpio.output(zoneinfo['pin'],self.off()) 



    def run_sprinklers(self,sprinkle):
        # zonetime looks like [[1,5],[2,7],[3,4]] a list of lists of zone #, sprinkler duration
        print('run_ing sprinklers')
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

class Schedule:
    def __init__(self):

        # this is where I should start the scheduler, set variables and stuff
        pass



    def compute_next_run(self):
        with open('manualinfo.json','r') as file:
            manualinfo = json.load(file)

#        print('solarinfo',json.dumps(self.solarinfo,indent=2))
#        print('weatherinfo',json.dumps(self.weatherinfo,indent=2))
#        print('manualinfo',json.dumps(manualinfo,indent=2))

        sunrisedelta = 0
        sunsetdelta = -30 * 60
        todayrise = datetime.datetime.fromisoformat(self.solarinfo['today']['sunrise'])+datetime.timedelta(seconds = sunrisedelta)
        todayset = datetime.datetime.fromisoformat(self.solarinfo['today']['sunset'])+datetime.timedelta(seconds = sunsetdelta)
        tmrwrise = datetime.datetime.fromisoformat(self.solarinfo['tomorrow']['sunrise'])+datetime.timedelta(seconds = sunrisedelta)
        tmrwset = datetime.datetime.fromisoformat(self.solarinfo['tomorrow']['sunset'])+datetime.timedelta(seconds = sunsetdelta)
        now = datetime.datetime.now(pytz.timezone('America/Denver'))
        datetime_list = [todayrise,todayset,tmrwrise,tmrwset]
        future_datetimes = [dt for dt in datetime_list if dt > now]

        if future_datetimes == []: # If there's stale data, future_datetimes will be empty. Refresh to fix
            self.get_weather()
            self.get_solar()

        self.next_run = min(future_datetimes)

        duration_modulation = 0
        days = list(self.weatherinfo.keys())
        today = self.weatherinfo[days[0]]
        tomorrow = self.weatherinfo[days[1]]
        if today['precipitation']>5 or tomorrow['precipitation'] > 10:
            duration_modulation -= 2
        if today['max_temp']> 100 or tomorrow['max_temp'] > 100:
            duration_modulation += 1

        sprinkle = [[key, value['duration'] + duration_modulation] for key, value in zones().items()]  

        manual_delay = datetime.datetime.strptime(manualinfo['manual_delay_expiration'].split('.')[0],"%Y-%m-%d %H:%M:%S").astimezone(pytz.timezone('America/Denver'))
        try:
            schedule.cancel_job(self.next_run_job)
        except NameError:
            pass
        except AttributeError:
            pass
        if self.next_run > manual_delay:
            sprinkle = [[1,1],[2,1],[3,1],[4,1],[5,1],[6,1]]
            partial_sprinklerer_task = partial(add_sprinkle_task,sprinkle)
            self.next_run = datetime.datetime.now(pytz.timezone('America/Denver')) + datetime.timedelta(seconds=3)
            self.next_run_job = schedule.every().day.at(datetime.datetime.strftime(self.next_run,'%H:%M:%S')).do(partial_sprinklerer_task)


    async def get_weather(self):
        if not hasattr(self,'weatherinfo'):
            self.weatherinfo = {}
        url = 'https://api.open-meteo.com/v1/forecast'
        data = {'latitude':39.93248,'longitude':-105.08279,'daily':['precipitation_sum','temperature_2m_min','temperature_2m_max'],'temperature_unit':'fahrenheit','timezone':'America/Denver'}

        now = int(time.time())
        r = requests.request("GET",url,params=data).json()['daily']
        for i in range(0,len(r['time'])):
            self.weatherinfo.update({r['time'][i]:{'precipitation':r['precipitation_sum'][i],'max_temp':r['temperature_2m_max'][i],'min_temp':r['temperature_2m_min'][i]},'last_updated':now})
        self.write_json(self.weatherinfo,'weatherinfo.json','w')

    async def _get_times(self,day):
        url = 'https://api.sunrise-sunset.org/json'
        data = {'lat':39.93248,'lng':-105.08279,'formatted':0,'date':day,'tzid':'America/Denver'}
        print('Getting solar times from API')
        r = requests.get(url,params = data)
        response_data = r.json()['results']
        return(response_data)

    async def get_solar(self):
        if not hasattr(self,'solarinfo'):
            self.solarinfo = {}
        now = int(time.time())
        self.solarinfo.update({'today':self._get_times('today')})
        time.sleep(5)
        self.solarinfo.update({'tomorrow':self._get_times('tomorrow'),'last_updated':now})
        self.write_json(self.solarinfo,'solarinfo.json','w')
