from redis_crud import ActiveJob, ScheduleJob
from mqtt_client import SprinkleClient
import time,json,asyncio,datetime
import RPi.GPIO as gpio

class PinController():
    def __init__(self):
        gpio.setwarnings(False)
        gpio.setmode(gpio.BCM)
        self.zones = {
                1:{'name':'NE Corner', 'duration':7,'pin':17,'state':-1},
                2:{'name':'North Yard','duration':7,'pin':22,'state':-1},
                3:{'name':'NW Corner', 'duration':7,'pin':24,'state':-1},
                4:{'name':'East Yard', 'duration':7,'pin':25,'state':-1},
                5:{'name':'SW Yard',   'duration':7,'pin':16,'state':-1},
                6:{'name':'South Yard','duration':7,'pin':26,'state':-1},
            }
        for zoneinfo in self.zones.values():
            gpio.setup(zoneinfo['pin'],gpio.OUT)
            gpio.output(zoneinfo['pin'],self.off()) 

        
    def on(self):
    #    return(gpio.LOW) # no!
        return(gpio.HIGH) #confirmed!!
    def off(self):
        return(gpio.LOW) # confirmed!
    #    return(gpio.HIGH) # no!

    def read_state(self):
        for zone,zoneinfo in self.zones.items():
            pinstate = gpio.input(zoneinfo['pin'])
            self.zones
            zoneinfo.update({'state':pinstate})
        return(self.zones)

    def zone_on(self,zone):
        pin = self.zones[int(zone)]['pin']
        gpio.output(pin,self.on())

    def zone_off(self,zone):
        pin = self.zones[int(zone)]['pin']
        gpio.output(pin,self.off())

    def all_off(self):
        for zone in self.zones.keys():
            self.zone_off(zone)

class ScheduleMaster:
    def __init__(self):
        self.aj = ActiveJob()
        self.sj = ScheduleJob()
        self.pc = PinController()
        self.pc.all_off()

    async def send_state(self,now):
        zones = self.pc.read_state()
        jobs = self.sj.get_all_jobs()
        next_job_info = {'start_at':3000000000}
        for job_id,job_info in jobs.items():
            if job_info['is_cancelled']!=True and job_info['start_at'] < next_job_info['start_at'] and job_info['start_at']>now:
                next_job_info = job_info
        sc = SprinkleClient()
        statemsg = {'currently_running':[],'next_run':datetime.datetime.strftime(datetime.datetime.fromtimestamp(next_job_info['start_at']),'%a %-I:%H %p '),'message':'Currently sprinkling ','sent_at':int(time.time())}
        runningzones = []
        for zoneid,zoneinfo in zones.items():
            pinstate = gpio.input(zoneinfo['pin'])
            zoneinfo.update({'state':pinstate})
            if pinstate==self.pc.on():
                runningzones.append(zoneid)
        if runningzones == []:
            statemsg.update({'message':'No zones running'})
        else:
            for zone in runningzones:
                statemsg['currently_running'].append(zone)
                statemsg.update({'message':statemsg['message']+f"{zones[zone]['name']} "})

        sc.publish_message(sc.statetopic,statemsg)

    def active_processor(self,now,job_info):
        for item in job_info['zone_info']:
            if now > item['on_at'] and now < item['off_at']:
                self.pc.zone_on(item['zone'])
            else:
                self.pc.zone_off(item['zone'])

    async def job_check(self,now):
        scheduled_jobs = self.sj.get_all_jobs()
        active = False
        for job_id,job_info in scheduled_jobs.items():
            # If its not a cancelled job and now is between the start and end + 10s, its active
            if job_info['is_cancelled'] != True and now > job_info['start_at'] and now < job_info['end_at']+10:

                if job_info['is_active'] != True:
                    job_info.update({'is_active':True})
                    self.sj.update_job(job_id,job_info)

                self.active_processor(now,job_info)
                active = True
            else:
                if job_info['is_active'] != False:
                    job_info.update({'is_active':False})
                    self.sj.update_job(job_id,job_info)
        if active == False:
            self.pc.all_off()   

    async def main_loop(self):
        possible_funcs = [
            [1, self.job_check],
            [5, self.send_state],
        ]
        while True:
            now = int(time.time())
            for interval, func in possible_funcs:
                if now % interval == 0:
                    print('doing',func.__name__)
                    asyncio.create_task(func(now))  # Fire-and-forget
            print(now)
            await asyncio.sleep(1)  # Non-blocking sleep for 1 second
            

if __name__ == '__main__':
    asyncio.run(ScheduleMaster().main_loop())
