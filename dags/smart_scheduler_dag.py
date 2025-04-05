"""
智能调度系统的Airflow DAG示例
此DAG演示了如何与智能调度系统集成，自动获取执行计划
"""
from datetime import datetime, timedelta
import requests
import json
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

# 默认的DAG参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 智能调度系统API地址
SCHEDULER_API_URL = Variable.get("scheduler_api_url", "http://localhost:5000")

def get_execution_plan():
    """获取今日需要执行的表和其依赖关系"""
    try:
        response = requests.get(f"{SCHEDULER_API_URL}/api/v1/dependency/refresh", timeout=60)
        response.raise_for_status()
        
        # 获取今日需要执行的表
        tables_response = requests.get(f"{SCHEDULER_API_URL}/api/v1/tables", timeout=30)
        tables_response.raise_for_status()
        
        tables_data = tables_response.json()
        execution_plan = {}
        
        # 获取每个表的依赖关系
        for table in tables_data['tables']:
            if not table['is_enabled']:
                continue
                
            table_name = table['table_name']
            deps_response = requests.get(
                f"{SCHEDULER_API_URL}/api/v1/tables/{table_name}/dependencies", 
                timeout=30
            )
            deps_response.raise_for_status()
            
            execution_plan[table_name] = deps_response.json()['dependencies']
        
        return execution_plan
        
    except Exception as e:
        logging.error(f"获取执行计划失败: {str(e)}")
        raise

def execute_table_task(table_name, **kwargs):
    """执行单个表的处理任务"""
    try:
        # 调用智能调度系统的立即执行API
        response = requests.post(
            f"{SCHEDULER_API_URL}/api/v1/execute/immediate",
            json={"table_name": table_name},
            timeout=60
        )
        response.raise_for_status()
        
        result = response.json()
        execution_id = result.get('execution_id')
        
        # 轮询执行状态直到完成
        status = "RUNNING"
        while status == "RUNNING":
            status_response = requests.get(
                f"{SCHEDULER_API_URL}/api/v1/status/{execution_id}",
                timeout=30
            )
            status_response.raise_for_status()
            
            status_data = status_response.json()
            status = status_data.get('status')
            
            if status == "FAILED":
                raise Exception(f"表 {table_name} 执行失败: {status_data.get('error_message')}")
            
            if status == "RUNNING":
                import time
                time.sleep(10)  # 等待10秒后再次检查
        
        return f"表 {table_name} 执行完成，执行ID: {execution_id}"
        
    except Exception as e:
        logging.error(f"表 {table_name} 执行失败: {str(e)}")
        raise

# 创建DAG
with DAG(
    'smart_scheduler_dag',
    default_args=default_args,
    description='从智能调度系统获取执行计划并执行',
    schedule_interval='0 1 * * *',  # 每天凌晨1点执行
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['smart_scheduler'],
) as dag:

    start = DummyOperator(task_id="start")
    
    # 获取执行计划
    get_plan = PythonOperator(
        task_id='get_execution_plan',
        python_callable=get_execution_plan,
        dag=dag,
    )
    
    end = DummyOperator(task_id="end")
    
    start >> get_plan
    
    # 动态生成任务组和任务
    def generate_table_tasks(execution_plan):
        """根据执行计划动态生成任务"""
        if not execution_plan:
            return
            
        # 创建表处理任务
        for table_name, dependencies in execution_plan.items():
            with TaskGroup(group_id=f"process_{table_name}") as table_group:
                table_task = PythonOperator(
                    task_id=f"execute_{table_name}",
                    python_callable=execute_table_task,
                    op_kwargs={'table_name': table_name},
                )
                
                if dependencies:
                    # 创建所有依赖表的引用
                    dep_tasks = []
                    for dep_table in dependencies:
                        if f"process_{dep_table}" in dag.task_group_dict:
                            dep_tasks.append(dag.task_group_dict[f"process_{dep_table}"])
                    
                    # 设置依赖关系        
                    if dep_tasks:
                        for dep_task in dep_tasks:
                            dep_task >> table_group
                
            # 将任务组连接到DAG
            get_plan >> table_group >> end
            
    # 这个函数会在DAG解析时执行
    generate_table_tasks({}) 