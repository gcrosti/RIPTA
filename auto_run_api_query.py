import os
from apiqueries import query_tripupdates, query_vehicle_pos
from apscheduler.schedulers.background import BlockingScheduler as scheduler


sch = scheduler()
sch.add_job(query_vehicle_pos,'interval',seconds=300)
sch.add_job(query_tripupdates,'interval',seconds=300)
sch.start()
