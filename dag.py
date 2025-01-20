
from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

# 제발 성공공


with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *", # 매일 0시 0분에 스케줄 실행
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"), # dag이 언제부터 도는지 설정, utc는 세계표준시라 한국 시간에 맞춰야 함
    catchup=False, # False는 누락된 값을 생략, True는 누락된 값을 가져옴 2달치
    dagrun_timeout=datetime.timedelta(minutes=60), # 타임아웃 값
    tags=["example", "example2"], # airflow 밑에 파란 박스값을 설정해주는 거임 
    params={"example_key": "example_value"}, # Task들에 공통적으로 주는 값
) as dag:
     # [START howto_operator_bash]
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2