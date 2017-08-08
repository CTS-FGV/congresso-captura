from airflow import DAG
from airflow.operators.bash_operator import BashOperator 

import yaml
from datetime import datetime, timedelta

import imp
utils = imp.load_source('utils','/congresso-em-numeros/utils.py')

with open('/cron-jobs/captura/cn-database/cn_database/config_server.yaml', 'r') as f:
    server = yaml.load(f)

host = server['host']
database = server['database']
user = server['user']
password = server['password']


default_args = {
    'owner': 'cts',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['joao.carabetta@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
} 

dag = DAG('bot_slack',
		 description='Automatiza mensagem sobre MPVs que irão expirar na semana',
         schedule_interval='0 9 * * 0',
         start_date=datetime(2017, 3, 20),
         catchup=False,
         default_args=default_args)


to_bot = 'sshpass -p {} ssh -o StrictHostKeyChecking=no Admin@{} ' \
         'jupyter nbconvert 0 --inplace  --ExecutePreprocessor.timeout=None ' \
         '--execute cts/congresso/bot_slack/'.format(password = server['password'],
                                                     host = server['host'])

### ATUALIZA MPV
bot_MPV_path =  to_bot + 'MPV.ipynb'

bot_MPV = BashOperator(task_id= 'MPV',
			    bash_command=bot_MPV_path,
                on_failure_callback = utils.slack_notify_dag_error,
				dag=dag
)

