from redis_crud import ScheduleJob, ActiveJob
import json
sj = ScheduleJob()

#print(json.dumps(sj.get_all_jobs(),indent=2))
id = 'O1yKVDOP'
a = sj.get_job(id)
print(a)
a.update({'active':True})
sj.update_job(id,a)