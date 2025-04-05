"""
月级调度DAG，负责执行月级调度任务
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import logging
import os
import sys
import requests

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 加载配置
config_path = Variable.get('scheduler_config_path', '/etc/smart_scheduler/config.json')
scheduler_api_url = Variable.get('scheduler_api_url', 'http://localhost:5000')

# 默认参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': int(Variable.get('default_retries', '5')),
    'retry_delay': timedelta(minutes=int(Variable.get('retry_delay_minutes', '1'))),
}

# 创建DAG
dag = DAG(
    'scheduled_monthly',
    default_args=default_args,
    description='月级调度任务',
    schedule_interval='0 2 1 * *',  # 每月1号凌晨2点执行
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['smart_scheduler', 'monthly'],
)

# 开始任务
start = DummyOperator(task_id='start', dag=dag)

# 获取需要执行的表
def get_scheduled_tables(**context):
    """获取需要月级调度的表"""
    logging.info("获取月级调度表")
    
    try:
        # 调用API获取调度表
        response = requests.get(f"{scheduler_api_url}/api/tables/scheduled", 
                               params={'frequency': 'monthly'})
        
        if response.status_code == 200:
            tables = response.json().get('tables', [])
            logging.info(f"获取到 {len(tables)} 个月级调度表")
            return tables
        else:
            logging.error(f"获取调度表失败: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        logging.error(f"获取调度表异常: {str(e)}")
        return []

# 生成执行计划
def generate_execution_plan(**context):
    """生成执行计划"""
    ti = context['ti']
    tables = ti.xcom_pull(task_ids='get_scheduled_tables')
    
    if not tables:
        logging.warning("没有需要执行的表")
        return []
    
    logging.info(f"为 {len(tables)} 个表生成执行计划")
    
    try:
        # 调用API生成执行计划
        response = requests.post(f"{scheduler_api_url}/api/plan/generate", 
                                json={'tables': tables})
        
        if response.status_code == 200:
            plan = response.json().get('plan', [])
            logging.info(f"生成执行计划成功，共 {len(plan)} 个批次")
            return plan
        else:
            logging.error(f"生成执行计划失败: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        logging.error(f"生成执行计划异常: {str(e)}")
        return []

# 执行计划
def execute_plan(**context):
    """执行计划"""
    ti = context['ti']
    plan = ti.xcom_pull(task_ids='generate_execution_plan')
    
    if not plan:
        logging.warning("没有可执行的计划")
        return {}
    
    logging.info(f"开始执行计划，共 {len(plan)} 个批次")
    
    try:
        # 调用API执行计划
        response = requests.post(f"{scheduler_api_url}/api/plan/execute", 
                                json={
                                    'plan': plan,
                                    'context': {
                                        'run_id': context['run_id'],
                                        'execution_type': 'monthly',
                                        'dag_id': context['dag'].dag_id,
                                        'execution_date': context['execution_date'].isoformat()
                                    }
                                })
        
        if response.status_code == 200:
            result = response.json()
            logging.info(f"执行计划完成")
            return result
        else:
            logging.error(f"执行计划失败: {response.status_code} - {response.text}")
            return {'status': 'error', 'message': response.text}
    except Exception as e:
        logging.error(f"执行计划异常: {str(e)}")
        return {'status': 'error', 'message': str(e)}

# 定义任务
get_tables_task = PythonOperator(
    task_id='get_scheduled_tables',
    python_callable=get_scheduled_tables,
    dag=dag,
)

plan_task = PythonOperator(
    task_id='generate_execution_plan',
    python_callable=generate_execution_plan,
    dag=dag,
)

execute_task = PythonOperator(
    task_id='execute_plan',
    python_callable=execute_plan,
    dag=dag,
)

# 结束任务
end = DummyOperator(task_id='end', dag=dag)

# 设置任务依赖
start >> get_tables_task >> plan_task >> execute_task >> end 