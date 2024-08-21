import json,datetime,pytz,time,json,requests,asyncio
from controller import PinController
from redis_crud import DataCrud,ScheduleJob

class DataGather():
    def __init__(self):
        self.dc = DataCrud()

    def compute_next_run(self): # This should be part of the controller I think
        zones = PinController().zones
        solarinfo = self.dc.retrieve_data('solarinfo')
        weatherinfo = self.dc.retrieve_data('weatherinfo')
        delaystopinfo = self.dc.retrieve_data('delaystopinfo')

#        print('solarinfo',json.dumps(solarinfo,indent=2))
#        print('weatherinfo',json.dumps(weatherinfo,indent=2))
#        print('delaystopinfo',json.dumps(delaystopinfo,indent=2))
#        print('\n\n')

        sunrisedelta = 0
        sunsetdelta = -30 * 60
        todayrise = int((datetime.datetime.fromisoformat(solarinfo['today']['sunrise'])+datetime.timedelta(seconds = sunrisedelta)).timestamp())
        todayset = int((datetime.datetime.fromisoformat(solarinfo['today']['sunset'])+datetime.timedelta(seconds = sunsetdelta)).timestamp())
        tmrwrise = int((datetime.datetime.fromisoformat(solarinfo['tomorrow']['sunrise'])+datetime.timedelta(seconds = sunrisedelta)).timestamp())
        tmrwset = int((datetime.datetime.fromisoformat(solarinfo['tomorrow']['sunset'])+datetime.timedelta(seconds = sunsetdelta)).timestamp())
        now = int(time.time()) #datetime.datetime.now(pytz.timezone('America/Denver'))
        datetime_list = [todayrise,todayset,tmrwrise,tmrwset]
        future_datetimes = [dt for dt in datetime_list if dt > now]

        if future_datetimes == []: # If there's stale data, future_datetimes will be empty. Refresh to fix
            self.get_weather()
            self.get_solar()

        next_run = min(future_datetimes)

        duration_modulation = 0
        days = list(weatherinfo.keys())
        days.sort()
        today = weatherinfo[days[0]]
        tomorrow = weatherinfo[days[1]]
        print('today',today)
        print('tomorrow',tomorrow)
        if today['precipitation']>5 or tomorrow['precipitation'] > 10:
            duration_modulation -= 2
        if today['max_temp']> 100 or tomorrow['max_temp'] > 100:
            duration_modulation += 1

        sprinkle = [[key, value['duration'] + duration_modulation] for key, value in zones.items()]  
        if next_run != None:
            if next_run > delaystopinfo['delay_stop_until_s']:
                payload = {
                    'start_at':next_run,
                    'zone_info':sprinkle,
                    'source':'compute_next_run'
                }
                job_id = ScheduleJob().create_job(payload)
                print(job_id)
            else:
                print('next_run overlaps with existing delay')

    async def get_weather(self):
        weatherinfo = {}
        url = 'https://api.open-meteo.com/v1/forecast'
        data = {'latitude':39.93248,'longitude':-105.08279,'daily':['precipitation_sum','temperature_2m_min','temperature_2m_max'],'temperature_unit':'fahrenheit','timezone':'America/Denver'}

        now = int(time.time())
        r = requests.request("GET",url,params=data).json()['daily']
        for i in range(0,len(r['time'])):
            weatherinfo.update({r['time'][i]:{'precipitation':r['precipitation_sum'][i],'max_temp':r['temperature_2m_max'][i],'min_temp':r['temperature_2m_min'][i]},'last_updated':now})
        self.dc.update_data('weatherinfo',weatherinfo)

    async def _get_times(self,day):
        url = 'https://api.sunrise-sunset.org/json'
        data = {'lat':39.93248,'lng':-105.08279,'formatted':0,'date':day,'tzid':'America/Denver'}
        print('Getting solar times from API')
        r = requests.get(url,params = data)
        response_data = r.json()['results']
        return(response_data)

    async def get_solar(self):
        solarinfo = {}
        now = int(time.time())
        solarinfo.update({'today':await self._get_times('today')})
        time.sleep(5)
        solarinfo.update({'tomorrow':await self._get_times('tomorrow'),'last_updated':now})
        self.dc.update_data('solarinfo',solarinfo)

if '__main__' == __name__:
    dg = DataGather()
    asyncio.run(dg.get_solar())
    asyncio.run(dg.get_weather())
    dg.compute_next_run()
    # run this via cron at like 2am and 2pm
#    print(json.dumps(dg.dc.retrieve_data('solarinfo'),indent=2))
#    print(json.dumps(dg.dc.retrieve_data('weatherinfo'),indent=2))