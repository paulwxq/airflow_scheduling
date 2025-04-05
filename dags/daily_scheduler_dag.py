"""
日级调度DAG，负责执行日级调度任务
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import logging
import os
import sys
import requests

# 导入配置
from config import (
    DEFAULT_ARGS, DAILY_SCHEDULE, SCHEDULER_API_URL, 
    MAX_POLL_TIME_MINUTES, POLL_INTERVAL_SECONDS,
    API_TIMEOUT_SHORT, API_TIMEOUT_MEDIUM, API_TIMEOUT_LONG,
    get_dag_id
)

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 定义获取日级调度表的函数
def get_daily_tables(**context):
    """获取所有日级调度表"""
    try:
        # 强制刷新依赖图
        refresh_response = requests.post(
            f"{SCHEDULER_API_URL}/api/v1/dependency/refresh",
            timeout=API_TIMEOUT_SHORT
        )
        refresh_response.raise_for_status()
        
        # 获取日级调度表
        tables_response = requests.get(
            f"{SCHEDULER_API_URL}/api/v1/tables?frequency=daily",
            timeout=API_TIMEOUT_SHORT
        )
        tables_response.raise_for_status()
        
        tables_data = tables_response.json()
        tables = []
        
        # 筛选出启用的表
        for table in tables_data.get('tables', []):
            if table.get('is_enabled', True):
                tables.append(table.get('name'))
        
        logging.info(f"找到 {len(tables)} 个日级调度表")
        context['ti'].xcom_push(key='daily_tables', value=tables)
        return tables
    
    except Exception as e:
        logging.error(f"获取日级调度表失败: {str(e)}")
        raise

# 定义生成执行计划的函数
def generate_execution_plan(**context):
    """生成执行计划"""
    try:
        ti = context['ti']
        tables = ti.xcom_pull(task_ids='get_daily_tables', key='daily_tables')
        
        if not tables:
            logging.warning("没有日级调度表，跳过生成执行计划")
            ti.xcom_push(key='execution_plan', value=[])
            return []
        
        # 调用API生成执行计划
        plan_response = requests.post(
            f"{SCHEDULER_API_URL}/api/v1/plan/generate",
            json={"tables": tables, "include_dependencies": True},
            timeout=API_TIMEOUT_MEDIUM
        )
        plan_response.raise_for_status()
        
        plan_data = plan_response.json()
        execution_plan = plan_data.get('plan', [])
        
        logging.info(f"生成执行计划成功，共 {len(execution_plan)} 个批次")
        ti.xcom_push(key='execution_plan', value=execution_plan)
        return execution_plan
    
    except Exception as e:
        logging.error(f"生成执行计划失败: {str(e)}")
        raise

# 定义执行表的函数
def execute_table(table_name, **context):
    """执行单个表处理"""
    try:
        logging.info(f"开始处理表: {table_name}")
        
        # 调用API执行表处理
        execute_response = requests.post(
            f"{SCHEDULER_API_URL}/api/v1/execute/immediate",
            json={"table_name": table_name},
            timeout=API_TIMEOUT_LONG
        )
        execute_response.raise_for_status()
        
        result = execute_response.json()
        execution_id = result.get('run_id')
        
        logging.info(f"表 {table_name} 处理已开始，执行ID: {execution_id}")
        
        # 轮询执行状态
        start_time = datetime.now()
        
        while (datetime.now() - start_time).total_seconds() < MAX_POLL_TIME_MINUTES * 60:
            status_response = requests.get(
                f"{SCHEDULER_API_URL}/api/v1/status/{execution_id}",
                timeout=API_TIMEOUT_SHORT
            )
            status_response.raise_for_status()
            
            status_data = status_response.json()
            status = status_data.get('status')
            
            if status in ['success', 'failed']:
                if status == 'success':
                    logging.info(f"表 {table_name} 处理成功")
                    return f"表 {table_name} 处理成功，执行ID: {execution_id}"
                else:
                    error_msg = status_data.get('error_message', '未知错误')
                    logging.error(f"表 {table_name} 处理失败: {error_msg}")
                    raise Exception(f"表 {table_name} 处理失败: {error_msg}")
            
            logging.info(f"表 {table_name} 处理中，状态: {status}")
            import time
            time.sleep(POLL_INTERVAL_SECONDS)
        
        # 超时处理
        logging.warning(f"表 {table_name} 处理超时，请检查状态")
        return f"表 {table_name} 处理超时，执行ID: {execution_id}"
        
    except Exception as e:
        logging.error(f"表 {table_name} 处理失败: {str(e)}")
        raise

# 创建DAG
with DAG(
    get_dag_id('daily'),
    default_args=DEFAULT_ARGS,
    description='每日执行的调度DAG',
    schedule_interval=DAILY_SCHEDULE,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['smart_scheduler', 'daily'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    # 获取调度表
    get_tables = PythonOperator(
        task_id='get_daily_tables',
        python_callable=get_daily_tables,
    )
    
    # 生成执行计划
    generate_plan = PythonOperator(
        task_id='generate_execution_plan',
        python_callable=generate_execution_plan,
    )
    
    end = DummyOperator(task_id='end')
    
    start >> get_tables >> generate_plan
    
    # 动态生成表处理任务
    def generate_table_tasks():
        """根据执行计划动态生成任务"""
        def _execute_batch(**context):
            """执行批次处理"""
            ti = context['ti']
            execution_plan = ti.xcom_pull(task_ids='generate_execution_plan', key='execution_plan')
            batch_index = context['batch_index']
            
            if not execution_plan or batch_index >= len(execution_plan):
                logging.warning(f"批次 {batch_index} 不存在，跳过执行")
                return
            
            batch = execution_plan[batch_index]
            results = {}
            
            # 遍历处理批次中的所有表
            for table in batch:
                table_result = execute_table(table, **context)
                results[table] = table_result
            
            return results
        
        # 创建批次处理任务组
        with TaskGroup(group_id="execute_batches") as batch_group:
            # 先创建一个空的批次组
            # 实际的批次任务会在DAG运行时根据执行计划创建
            batch_task = PythonOperator(
                task_id='execute_batch_0',
                python_callable=_execute_batch,
                op_kwargs={'batch_index': 0},
            )
        
        generate_plan >> batch_group >> end
    
    # 执行动态生成任务
    generate_table_tasks() 