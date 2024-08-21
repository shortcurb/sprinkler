from redis_crud import ScheduleJob, ActiveJob
import time, json



new_job_info = {
    'start_at':int(time.time())+1,
    'zone_info':[[1,.4],[2,.4],[3,.4]],
    'source':'test',
}


cj = ScheduleJob()

#ActiveJob().create_active('jobs:ZKIVQKJV')
while True:
    jobs = cj.get_all_jobs()
    for job_id,job_info in jobs.items():
        print(job_id,'start_at',job_info['start_at'],'is_cancelled',job_info['is_cancelled'])
#    print(json.dumps(jobs,indent=2))
    print(int(time.time()))
    time.sleep(1)