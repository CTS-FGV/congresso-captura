from airflow.operators import PythonOperator
from airflow.models import DAG

from datetime import datetime, timedelta

from cn_database.senado_materia_atualizadas import atualizar

args = {
    'owner': 'mark',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='senado_materia',
          default_args=args,
          schedule_interval='0 * * * 1,2,3,4,5',
          dagrun_timeout=timedelta(seconds=30))

atualizar = \
    PythonOperator(task_id='atualizar_materias',
                   provide_context=True,
                   python_callable=atualizar(),
                   dag=dag)
