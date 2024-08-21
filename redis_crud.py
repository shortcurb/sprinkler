import redis, secrets, string, json,time,traceback
from typing import Dict, Any, List, Optional, Union


class ScheduleJob:
    def __init__(self):
        self.redisclient = redis.Redis(host='localhost', port=6379)

    def uncompute_existing_job(self,job_info): # this is silly
        rudimentary_zones = []
        for item in job_info['zone_info']:
            rudimentary_zones.append([
                item['zone'],(item['end_at']-item['start_at']).total_seconds()/60
            ])
        job_info.update({
            'zone_info':rudimentary_zones,
        })
        return(job_info)

    def reschedule_existing_job(self, now, job_id, delay_s):
        job_info = self.get_job(job_id)
        self.cancel_job(job_id)
        for item in job_info['zone_info']:
            # if the zone hasn't yet been sprinkled, start and end are delayed equally
            # elif the zone is in the middle of being sprinkled, move the start to now + delay and keep the end as end + delay
            # (else) if the zone has already been sprinkled, forget about it
            print(json.dumps(item,indent=2))
            if now < item['on_at'] and now < item['off_at']:
                on_time = item['on_at'] + delay_s
            elif now > item['on_at'] and now < item['off_at']:
                on_time = now + delay_s - 5 # to account for the overlap
            item.update({
                    'on_at':int(on_time),
                    'off_at':int(item['off_at']+delay_s)
                })    
        job_info.update({
            'start_at': int(job_info['zone_info'][0]['on_at']),
            'end_at':int(job_info['end_at'] + delay_s)
        })
        try:
            self.check_for_existing(job_info)
            self.update_job(job_id,job_info)
        except ValueError:
            return('Delayed job interfers with another scheduled job, deleting this one and defering to following. ')

    def compute_new_job(self,job_info):
        start_at = job_info['start_at']
        new_job_info = {
            'start_at':start_at,
            'zone_info':[],
            'source':job_info['source']
        }
        counted_seconds = 0
        for item in job_info['zone_info']:
            duration = item[1] * 60
            zone_info_active = {
                'zone':item[0],
                'on_at':int(start_at + counted_seconds),
                'off_at':int(start_at + counted_seconds + duration + 5)
            }
            new_job_info['zone_info'].append(zone_info_active)
            counted_seconds += duration
        new_job_info.update({
            'end_at':int(start_at + counted_seconds - 5)
        })
        return new_job_info

    def check_for_existing(self,job_id, job_info: Dict):
        job_start = job_info['start_at']
        job_end = job_info['end_at']
        existing_jobs = self.get_all_jobs()

        for existing_job_id,existing_job_info in existing_jobs.items():
            if existing_job_info['is_cancelled'] != True and existing_job_id !=job_id: # only worry about clashes with different jobs and non-cancelled jobs
                if job_start >= existing_job_info['start_at'] and job_start <= existing_job_info['end_at']:
                    raise ValueError(f"Job scheduled to start during existing job {existing_job_id}")
                elif (job_end >= existing_job_info['start_at'] and job_end <= existing_job_info['end_at']):
                    raise ValueError(f"Job scheduled to end during existing job {existing_job_id}")

    def get_all_jobs(self):
        jobs = {}
        for key in self.redisclient.scan_iter("jobs:*"):
            job_data = self.redisclient.get(key)
            if job_data:
                jobs.update({key.decode('utf-8'):json.loads(job_data)})
        return jobs       

    def create_job(self,job_info: Dict):
        job_info = self.compute_new_job(job_info)
        try:
            job_id = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(8))
            self.check_for_existing(job_id,job_info)
            job_info.update({
                'created_at':int(time.time()),
                'updated_at':None,
                'cancelled_at':None,
                'is_cancelled':False,
                'is_active':False,
                })
#            print(json.dumps(job_info,indent=2))
            self.redisclient.set(f"jobs:{job_id}",json.dumps(job_info))
            return job_id
        except ValueError as e:
            traceback.print_exc()
            print(e)
            return e

    def get_job(self, job_id: str):
        if 'jobs:' not in job_id:
            job_id = f"jobs:{job_id}"
        job_data = self.redisclient.get(job_id)
        if job_data:
            return json.loads(job_data)
        return None

    def update_job(self,job_id: str, update_job: Dict):
        try:
            if update_job['is_cancelled'] != True: # only check for existing if the one you're updating isn't a cancellation
                self.check_for_existing(job_id,update_job)
            update_job.update({'updated_at':int(time.time())})
            self.redisclient.set(job_id,json.dumps(update_job))
        except ValueError as e:
            traceback.print_exc()

    def cancel_job(self,job_id:str,now=int(time.time())):
        job_info = self.get_job(job_id)
        job_info.update({'is_cancelled':True,'cancelled_at':now})
        self.update_job(job_id,job_info)

    def _delete_job(self,job_id:str):
        self.redisclient.delete(job_id)

class ActiveJob:
    def __init__(self):
        self.redisclient = redis.Redis(host='localhost', port=6379)
    
    def get_zone_pin(self):
        return json.loads(self.redisclient.get('zone_pin').decode('utf-8'))

    def update_zone_pins(self,zone_pin):
        self.redisclient.set('zone_pin',json.dumps(zone_pin))

class DataCrud:
    def __init__(self):
        self.redisclient = redis.Redis(host='localhost', port=6379)

    def update_data(self,id,data):
        if isinstance(data,Dict):
            data = json.dumps(data,default=str)
        self.redisclient.set(id,data)
    
    def retrieve_data(self, id):
        data = self.redisclient.get(id)
        if data != None:
            data = json.loads(data)
        return data
    
"""
This thing will have to consist of three separate entities that work together, probably through redis. 
So the structure of data in redis is very important
The sprinkler should handle everything related to atruclly controlling the sprinklers
It will receive a data structure that has a datetime, a list of zones, 
If it has the datetime, is it then the scheduler that decides when to sprinkle?
I guess, yeah. while True: check queue time.sleep(3)
Also I like the idea of keeping datetimes in unix epocs. Makes for easier math
Okay so the sprinkler has a while True loop that checks the queue
The data structure in the queue should be a list of jobs
Jobs should have a datetime start, "active" boolean "cancelled" boolean,
maybe "source" for scheduled vs manual
also datetimes for scheduled at, cancelled at, ran at, etc
Each job needs a unique hash as well

There will also be a redis structure about the current job
When the current job is being executed, it should load that job
into this structure, and update it regularly with current zone,  
time sprinkled, time remaining, next zone, previous zone, expected end datetime
Maybe a cancelled boolean here as well? No.
The cancellation boolean live in the job and the tri-second sprinklerer check the job structure
for cancellation info every 3s?

Then also set up the weather and solar info getting to run via crontab and update redis
And a different (or maybe the same module, different functions?) function
will add jobs to the queue based on solar and weather data
Maybe this system will also clear old jobs out of the queue that are older than e.x. a week?


How do I queue upcoming jobs and make sure they don't overlap?
Maybe I need an intermediary CRUD system that will sit between the job queue
And the systems that might want to CRUD on it?
Yeah I ike that plan
"""
