#### 0. 들어가며

airflow의 몇가지 장점(dependency 설정, backfill 용이, 모니터링 가능)에 의해 셋팅된 환경에서 DA가 관리하는 마트의 dag코드를 작성하고 설정하는 과정을 정리한 글입니다.

#### 1. Airflow란?

Airflow는 python 코드로 워크플로우를 작성하고, 스케쥴링, 모니터링 하는 플랫폼입니다. Airflow는 작업들 사이의 관계와 순서를 DAG((Directed Acyclic Graph; 방향성 비순환 그래프)형태의 워크플로우로 표현하고 있으며, 이를 통해 배치 테이블간 dependency 관리 및 backfill을 용이하게 할 수 있습니다.

#### 2. 기본 동작 원리

Airflow는 Python DAG를 읽고, 거기에 맞춰 Scheduler가 Task를 스케줄링하면, Worker가 Task를 가져가 실행합니다. Task의 실행상태는 Database에 저장되고, 사용자는 UI를 통해서 각 Task의 실행 상태, 성공 여부 등을 확인할 수 있습니다.

<img width="862" alt="airflow_workflow이미지" src="https://user-images.githubusercontent.com/99888382/156910578-61f8ebe2-7222-400c-ad5a-5578637b9db8.png">
<img width="904" alt="airflow_element설명이미지" src="https://user-images.githubusercontent.com/99888382/156910579-d044a30b-94e9-4715-b912-8a81b9c0e53f.png">

#### 3. 기본 컨셉 및 핵심 용어

- DAG ( Directed Acyclic Graph )

  : DAG는 실행하고 싶은 Task 들의 관계와 dependency를 표현하고 있는 Task들의 모음으로 아래와 같은 그림으로 표시될수 있습니다.
<img width="525" alt="airflow_dag이미지" src="https://user-images.githubusercontent.com/99888382/156910580-b470914e-3db0-43e9-a171-a17dbf566615.png">
- Operator

  : task를 만들기 위해 사용되는 Airflow class로 operator나 sensor가 하나의 task로 만들어집니다.

    - BashOperator : bash command를 실행
    - PythonOperator : python 함수를 실행
    - Sensor : 하나의 Task로 사용할 수 있는데 특정 조건이 채워지기를 기다리면 조건을 만족하는 경우 이후 Task로 넘어가게 하는 역할 ( ex: 시간, 파일, db row, 등등을 기다리는 센서 )
- Task

  : 하나의 작업 단위를 Task라고 하며, 하나 혹은 여러개의 Task를 이용하여 하나의 DAG를 생성합니다.


#### 4.기타

- Date Parameter 받기

  마트를 빌드시 쿼리 내에서 기준 날짜를 계산하는 방식이 아닌 Airflow의 execution_date를 사용하여 날짜를 파라미터 형태로 받는 방법을 이용합니다.

    - 장점

      backfill 시에 start_date 등의 설정을 바꿀 필요 없이 airflow의 해당 기간 배치를 다시 돌리면 airflow 배치 날짜 기준으로 들어가므로 backfill이 용이

    - 방법
        - 1. 빌드 스크립트를 파라미터 받는 형식으로 작성한다

          ```python
          import sys
          from pyspark.sql import SparkSession
          
          spark = SparkSession.builder.enableHiveSupport().getOrCreate()
          
          def main(today_date):
              build_mart = spark.sql(
              """
                  select * from table where date = '{today_date}'
              """.format(today_date=today_date)
              )
              build_mart.registerTempTable("build_mart")
              if(spark.catalog._jcatalog.tableExists("origin_mart_name")):
                  spark.sql("""ALTER TABLE origin_mart_name DROP IF EXISTS PARTITION (date='{today_date}')""".format(today_date=today_date))
          
              build_mart.write.mode("append").partitionBy("date").saveAsTable("origin_mart_name")
          
          if __name__ == "__main__":
              today = sys.argv[1]
              main(today)
          ```

        - Dag코드에 날짜를 선언해 준다

            ```python
            from datetime import datetime
            import pendulum
            from airflow import DAG
            from airflow.operators.python_operator import PythonOperator
            from batch.script import dependency_first, dependency_second
            
            local_tz = pendulum.timezone("Asia/Seoul")
            TODAY_DATE = '{{ execution_date.in_timezone("Asia/Seoul").strftime("%Y-%m-%d") }}'
            
            default_args = {
                "depends_on_past": False,
                "start_date": datetime(2021, 1, 1, tzinfo=local_tz),
                "retries": 3,
                "email_on_failure": False
            }
            
            dag = DAG(
                dag_id="mart",
                default_args=default_args,
                schedule_interval="20 6 * * *",
                tags=["test"],
                max_active_runs=1,
                catchup=True,
            )
            dag.doc_md = __doc__
            
            dependency_first_sensor = PythonOperator(
                task_id="dependency_first",
                python_callable=dependency_first.main,
                dag=dag,
            )
            dependency_second_sensor = PythonOperator(
                task_id="dependency_second",
                python_callable=dependency_second.main,
                dag=dag,
            )
            
            build_mart_dag = BatchOperator(
                task_id="build_mart",
                name="build_mart",
                running_time=60,
                pyspark_file= 스크립트 경로 ,
                other_arguments=[TODAY_DATE],
                dag=dag,
                depends_on_past=False,    
            )
            
            [dependency_first_sensor, dependency_second_sensor] >> build_mart_dag
            ```

- Operator
    - ShortCircuitOperator : 특정 조건이 만족할 때 workflow를 돌게 하는 오퍼레이터
        - ex ) 하나의 dag로 일/주/월 배치를 다르게 할 때 사용

            ```python
            from datetime import datetime
            import pendulum
            from airflow import DAG
            from airflow.operators.python import ShortCircuitOperator
            local_tz = pendulum.timezone("Asia/Seoul")
            
            TODAY_DATE = '{{ execution_date.in_timezone("Asia/Seoul").strftime("%Y-%m-%d") }}'
            NEXT= '{{ next_execution_date.in_timezone("Asia/Seoul").strftime("%Y-%m-%d") }}'
            
            default_args = {
                "depends_on_past": False,
                "start_date": datetime(2021, 1, 1, tzinfo=local_tz),
                "retries": 3
            }
            
            dag = DAG(
                dag_id="build_test_mart",
                default_args=default_args,
                schedule_interval="20 6 * * *",
                tags=["test"],
                max_active_runs=1,
                catchup=True,
            )
            dag.doc_md = __doc__
            
            def _weekly_check_time(**kwargs):
                return datetime.strptime(kwargs["NEXT"], "%Y-%m-%d").date().weekday() == 0
            
            weekly_check_time = ShortCircuitOperator(
                task_id="weekly_check_time",
                python_callable=_weekly_check_time,
                op_kwargs={"NEXT": NEXT},
                dag=dag,
            )
            
            build_weekly_dag = BatchOperator(
                task_id="build_weekly",
                name="build_weekly",
                running_time=60,
                pyspark_file= 스크립트경로,
                other_arguments=[TODAY_DATE],
                dag=dag, 
            )
            
            weekly_check_time >> build_weekly_dag
            
            '''
            def _monthly_check_time(**kwargs):
                return kwargs["NEXT"][-2:] == "01"
            '''
            ```
    - AwsGlueCatalogPartitionSensor : 해당일자 파티션이 빌드됨을 확인하는 센서
