from redis_crud import ScheduleJob, ActiveJob
import time,json

cj = ScheduleJob()
jobs = cj.get_all_jobs()
for job in jobs:
    cj._delete_job(job)
