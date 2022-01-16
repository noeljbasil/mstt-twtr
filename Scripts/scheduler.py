from apscheduler.schedulers.blocking import BlockingScheduler
from stt_twtr import scrapping_job

sched = BlockingScheduler()

@sched.scheduled_job('cron', hour=1, minute=0,second=0)

def scheduled_job():
	scrapping_job()

sched.start()