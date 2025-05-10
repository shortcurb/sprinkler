from redis_crud import ScheduleJob, ActiveJob
from mqtt_client import SprinkleClient
import time,json

new_job_info = {'command':'gather_and_compute','value':[],'units':None}

sc = SprinkleClient()
sc.connect_mqtt()
sc.publish_message(sc.cmdtopic,new_job_info)
