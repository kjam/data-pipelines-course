''' Celery settings and app '''
from celery import Celery
from kombu import Queue
from configparser import ConfigParser
from datetime import datetime, timedelta
import os



config = ConfigParser()
current_dir = os.path.dirname(os.path.realpath(__file__))

if os.environ.get('DEPLOY') == 'PROD':
    config.read(os.path.join(current_dir, 'config/prod.cfg'))
else:
    config.read(os.path.join(current_dir, 'config/dev.cfg'))

app = Celery('tasks', broker=config.get('celery', 'broker_url'))

CELERY_CONFIG = {
    'CELERY_IMPORTS': ['tasks'],
    'CELERY_TIMEZONE': 'Europe/Berlin',
    'CELERY_IGNORE_RESULT': False,
    'CELERY_TRACK_STARTED': True,
    'CELERY_DEFAULT_QUEUE': 'default',
    'CELERY_QUEUES': (Queue('default'), Queue('priority'),),
    'CELERY_DEFAULT_RATE_LIMIT': '20/s',
    'CELERY_RESULT_BACKEND': 'amqp://',
    'CELERY_CHORD_PROPAGATES': True,
    'CELERYD_TASK_TIME_LIMIT': 7200,
    'CELERYD_POOL_RESTARTS': True,
    'CELERYD_TASK_LOG_FORMAT':
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'CELERY_ANNOTATIONS': {
        'celery.chord_unlock': {'hard_time_limit': 360},
    },
    'CELERYBEAT_SCHEDULE': {
        'get_stock_info_60s': {
            'task': 'tasks.get_stock_info',
            'schedule': timedelta(seconds=60),
            'args': ('FB', datetime(2016, 1, 1), datetime.today())
        }
    }
}


app.conf.update(**CELERY_CONFIG)
