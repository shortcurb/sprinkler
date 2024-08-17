import time,datetime,os,requests,json,pytz,schedule,traceback
import paho.mqtt.client as mqtt
from sprinklerer import zones,add_sprinkle_task,turnoff,on,danger_test
from functools import partial
import RPi.GPIO as gpio
from dotenv import load_dotenv
from mqttclient import SprinkleClient

class Schedulator:
    def __init__(self):
        turnoff()
        load_dotenv()
        self.read_and_update_info()
        self.zones = zones()
        self.next_run = datetime.datetime.now()+datetime.timedelta(days = 100000)
        gpio.setwarnings(False)
        gpio.setmode(gpio.BCM)
        for zone in self.zones.values():
            pin = zone['pin']
            gpio.setup(pin,gpio.OUT)
        self.statetopic = os.environ['mqttsprinklerstatetopic']
        self.mq = SprinkleClient()
        self.mq.on_disconnect() # Counter-intuitively, the on_disconnect function holds the retry logic in case the broker goes down. Use it to (re)connect

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

    def get_manual(self):
        print('Cannot update manualinfo automatically. Requesting user intervention')

    def get_weather(self):
        if not hasattr(self,'weatherinfo'):
            self.weatherinfo = {}
        url = 'https://api.open-meteo.com/v1/forecast'
        data = {'latitude':39.93248,'longitude':-105.08279,'daily':['precipitation_sum','temperature_2m_min','temperature_2m_max'],'temperature_unit':'fahrenheit','timezone':'America/Denver'}

        now = int(time.time())
        r = requests.request("GET",url,params=data).json()['daily']
        for i in range(0,len(r['time'])):
            self.weatherinfo.update({r['time'][i]:{'precipitation':r['precipitation_sum'][i],'max_temp':r['temperature_2m_max'][i],'min_temp':r['temperature_2m_min'][i]},'last_updated':now})
        self.write_json(self.weatherinfo,'weatherinfo.json','w')

    def _get_times(self,day):
        url = 'https://api.sunrise-sunset.org/json'
        data = {'lat':39.93248,'lng':-105.08279,'formatted':0,'date':day,'tzid':'America/Denver'}
        print('Getting solar times from API')
        r = requests.get(url,params = data)
        response_data = r.json()['results']
        return(response_data)

    def get_solar(self):
        if not hasattr(self,'solarinfo'):
            self.solarinfo = {}
        now = int(time.time())
        self.solarinfo.update({'today':self._get_times('today')})
        time.sleep(5)
        self.solarinfo.update({'tomorrow':self._get_times('tomorrow'),'last_updated':now})
        self.write_json(self.solarinfo,'solarinfo.json','w')

    def write_json(self,file_data,file_name,mode):
        now = int(time.time())
        file_data.update({'written_at':now})
        with open(file_name,mode) as file:
            json.dump(file_data,file,indent=2,default=str)

    def read_and_update_info(self):
        now = int(time.time())
        infofiles = [
            ['solarinfo.json','solarinfo',self.get_solar],
            ['weatherinfo.json','weatherinfo',self.get_weather],
            ['manualinfo.json','manualinfo',self.get_manual]
        ]
        try:
            for item in infofiles:
                try:
                    with open(item[0],'r') as fileobj:
                        temp_data = json.load(fileobj)
                        setattr(self,item[1],temp_data)
                        last_update = temp_data.get('last_updated')
                        print('last_update',last_update)
                        
                        if last_update == None:
                            print(item[0],'data was updated too long ago, updating now')
                            item[2]()
                        else:
                            try:
                                if now-last_update > 60 * 60 * 24:
                                    print(item[0],'data was updated too long ago, updating now')
                                    item[2]()
                            except ValueError:
                                print(item[0],'data was updated too long ago, updating now')
                                item[2]()
                except FileNotFoundError:
                    print(item[0],'file not found, triggering update (which will create the file)')
                    item[2]()

        except AttributeError:
            var_name = item[1].split('.')[0]
            if not hasattr(self,var_name):
                setattr(self,var_name,{})
        except Exception as e:
            traceback.print_exc()

    def send_state(self):
        statemsg = {'currently_running':[],'next_run':datetime.datetime.strftime(self.next_run,'%a %-I:%-H %p '),'message':'Currently sprinkling ','sent_at':int(time.time())}
        runningzones = []
        zoners = zones()
        for zoneid,zoneinfo in zoners.items():
            pinstate = gpio.input(zoneinfo['pin'])
            zoneinfo.update({'state':pinstate})
            if pinstate==on():
                runningzones.append(zoneid)
        if runningzones == []:
            statemsg.update({'message':'No zones running'})
        else:
            for zone in runningzones:
                statemsg['currently_running'].append(zone)
                statemsg.update({'message':statemsg['message']+f"{self.zones[zone]['name']} "})
        self.mq.publish_message(self.statetopic,json.dumps(statemsg,default=str))

    def manager(self):
        self.state_job = schedule.every(2).seconds.do(self.send_state)
        self.weather_job = schedule.every().day.at("01:00").do(self.get_weather)
        self.solar_job = schedule.every().day.at("01:05").do(self.get_solar)
        self.compute_job = schedule.every().day.at("02:00").do(self.compute_next_run)
        self.weather_job = schedule.every().day.at("13:00").do(self.get_weather)
        self.solar_job = schedule.every().day.at("13:05").do(self.get_solar)
        self.compute_job = schedule.every().day.at("14:00").do(self.compute_next_run)
        while True:
            print('Loop, running pending')
            schedule.run_pending()
            time.sleep(.5) 
            print(json.dumps(schedule.jobs,indent=2,default=str))
            print(int(time.time()))

if __name__ == '__main__':
    s = Schedulator()
    s.compute_next_run()
    s.manager()
