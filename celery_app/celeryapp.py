''' Celery settings and app '''
from celery import Celery
from configparser import ConfigParser
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
    'CELERY_IGNORE_RESULT': False,
    'CELERY_TRACK_STARTED': True,
    'CELERY_DEFAULT_RATE_LIMIT': '20/s',
    'CELERY_RESULT_BACKEND': 'rpc://',
    'CELERY_CHORD_PROPAGATES': True,
    'CELERYD_TASK_TIME_LIMIT': 7200,
    'CELERYD_POOL_RESTARTS': True,
    'CELERYD_TASK_LOG_FORMAT':
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}

app.conf.update(**CELERY_CONFIG)
