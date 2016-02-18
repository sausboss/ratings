import os
from apscheduler.schedulers.blocking import BlockingScheduler
from PostgreSQLStreetAccount import getMessages
from USRatingChangePerformance import getEventIDs


if __name__ == '__main__':

    # Create scheduler
    sched = BlockingScheduler()

    # Schedule streetaccount messages
    interval = 1
    sched.add_job(getMessages, 'interval',  minutes=interval)
    print('Fetching new StreetAccount messages every ' + str(interval) + ' minutes')

    # Schedule update to performance table
    sched.add_job(getEventIDs, 'cron', day_of_week='mon-fri', hour=17)
    print('Updating tick data business days at 5PM')

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        sched.start()

    except (KeyboardInterrupt, SystemExit):
        pass
