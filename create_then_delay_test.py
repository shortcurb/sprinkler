from redis_crud import ScheduleJob, ActiveJob
from mqtt_client import SprinkleClient
import time,json

new_job_info = {'command':'manual_sprinkle','value':[[1,.5],[2,.5],[3,.5],[4,.5],[5,.5],[6,.2]],'units':None}

sc = SprinkleClient()
sc.connect_mqtt()
sc.publish_message(sc.cmdtopic,new_job_info)
for i in range(20):
    print(i)
    time.sleep(1)
delaypay = {'command':'delay_stop','value':1,'units':'minutes'}
sc.publish_message(sc.cmdtopic,delaypay)

