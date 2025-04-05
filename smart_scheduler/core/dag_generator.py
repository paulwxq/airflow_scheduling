"""
DAG生成器模块，负责创建不同类型的Airflow DAG
"""
import logging
import uuid
import json
import os
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta

import networkx as nx
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import State

from .dependency_manager import DependencyManager
from .execution_planner import ExecutionPlanGenerator
from .execution_strategy import ExecutionStrategy
from .airflow_client import AirflowClient
from ..db.connectors import PostgreSQLConnector

# 设置日志
logger = logging.getLogger(__name__)


class DAGTemplate:
    """DAG模板基类"""
    
    def __init__(self, frequency: Optional[str], dag_id_prefix: str, dependency_manager: DependencyManager = None,
                pg_connector: PostgreSQLConnector = None):
        """
        初始化DAG模板
        
        Args:
            frequency: 调度频率，'hourly', 'daily', 'weekly', 'monthly'或None
            dag_id_prefix: DAG ID前缀
            dependency_manager: 依赖图管理器
            pg_connector: PostgreSQL连接器
        """
        self.frequency = frequency  # 'hourly', 'daily', 'weekly', 'monthly'或None
        self.dag_id_prefix = dag_id_prefix
        self.dependency_manager = dependency_manager or DependencyManager()
        self.pg_connector = pg_connector or PostgreSQLConnector()
        self.execution_planner = ExecutionPlanGenerator(self.dependency_manager, self.pg_connector)
        self.execution_strategy = ExecutionStrategy(self.pg_connector)
        
        logger.info(f"初始化DAG模板: {dag_id_prefix}, 频率: {frequency}")
    
    def create_dag(self, dag_id: str = None, **kwargs) -> DAG:
        """
        创建DAG实例
        
        Args:
            dag_id: DAG ID，如果为None则自动生成
            **kwargs: 其他DAG参数
            
        Returns:
            DAG实例
        """
        if dag_id is None:
            dag_id = f"{self.dag_id_prefix}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        dag_params = self._get_dag_params()
        dag_params.update(kwargs)
        
        with DAG(
            dag_id=dag_id,
            schedule_interval=self._get_schedule_interval(),
            start_date=datetime(2025, 1, 1),
            catchup=False,
            **dag_params
        ) as dag:
            self._build_dag_tasks(dag)
            return dag
    
    def _get_schedule_interval(self) -> Optional[str]:
        """
        获取调度间隔
        
        Returns:
            调度间隔表达式
        """
        if self.frequency == 'hourly':
            return '@hourly'
        elif self.frequency == 'daily':
            return '@daily'
        elif self.frequency == 'weekly':
            return '@weekly'
        elif self.frequency == 'monthly':
            return '@monthly'
        else:
            return None
    
    def _get_dag_params(self) -> Dict[str, Any]:
        """
        获取DAG参数
        
        Returns:
            DAG参数字典
        """
        # 从配置表读取参数
        max_concurrency = int(self.pg_connector.get_system_config('max_concurrency', '4'))
        max_retries = int(self.pg_connector.get_system_config('default_max_retries', '5'))
        retry_delay = int(self.pg_connector.get_system_config('default_retry_delay_minutes', '1'))
        
        return {
            'max_active_tasks': max_concurrency,
            'concurrency': max_concurrency,
            'default_args': {
                'owner': 'airflow',
                'depends_on_past': False,
                'email_on_failure': True,
                'email_on_retry': False,
                'retries': max_retries,
                'retry_delay': timedelta(minutes=retry_delay),
            },
            'tags': ['smart_scheduler', self.frequency or 'immediate']
        }
    
    def _build_dag_tasks(self, dag: DAG) -> None:
        """
        构建DAG任务，子类需要实现此方法
        
        Args:
            dag: DAG实例
        """
        raise NotImplementedError("子类必须实现此方法")


class ScheduledDAGTemplate(DAGTemplate):
    """周期性调度DAG模板"""
    
    def __init__(self, frequency: str, dependency_manager: DependencyManager = None, 
                pg_connector: PostgreSQLConnector = None):
        """
        初始化周期性调度DAG模板
        
        Args:
            frequency: 调度频率，'hourly', 'daily', 'weekly', 'monthly'
            dependency_manager: 依赖图管理器
            pg_connector: PostgreSQL连接器
        """
        super().__init__(frequency, f"scheduled_{frequency}", dependency_manager, pg_connector)
    
    def _build_dag_tasks(self, dag: DAG) -> None:
        """
        构建周期性DAG的任务
        
        Args:
            dag: DAG实例
        """
        # 1. 获取调度表任务
        get_tables_task = PythonOperator(
            task_id='get_scheduled_tables',
            python_callable=self._get_scheduled_tables,
            op_kwargs={'frequency': self.frequency},
            dag=dag,
        )
        
        # 2. 生成执行计划任务
        plan_task = PythonOperator(
            task_id='generate_execution_plan',
            python_callable=self._generate_execution_plan,
            op_kwargs={},
            dag=dag,
        )
        
        # 3. 执行计划任务
        execute_task = PythonOperator(
            task_id='execute_plan',
            python_callable=self._execute_plan,
            op_kwargs={},
            dag=dag,
        )
        
        # 设置任务依赖
        get_tables_task >> plan_task >> execute_task
    
    def _get_scheduled_tables(self, frequency: str, **context) -> List[str]:
        """
        获取指定频率的所有已订阅表
        
        Args:
            frequency: 调度频率
            **context: 上下文信息
            
        Returns:
            表名列表
        """
        logger.info(f"获取{frequency}频率的调度表")
        tables = self.pg_connector.get_scheduled_tables(frequency)
        context['ti'].xcom_push(key='scheduled_tables', value=tables)
        
        logger.info(f"找到{len(tables)}个{frequency}频率的调度表")
        return tables
    
    def _generate_execution_plan(self, **context) -> List[List[str]]:
        """
        生成优化的执行计划
        
        Args:
            **context: 上下文信息
            
        Returns:
            优化后的执行计划
            
        Raises:
            ValueError: 如果生成计划失败
        """
        ti = context['ti']
        tables = ti.xcom_pull(task_ids='get_scheduled_tables', key='scheduled_tables')
        
        if not tables:
            logger.warning("没有需要处理的表")
            ti.xcom_push(key='execution_plan', value=[])
            return []
        
        logger.info(f"为{len(tables)}个表生成执行计划")
        
        try:
            # 强制刷新依赖图
            self.dependency_manager.refresh_dependency_graph()
            
            # 生成执行计划
            plan = self.execution_planner.generate_plan(tables, include_dependencies=True)
            
            # 优化执行计划
            optimized_plan = self.execution_planner.optimize_plan(plan)
            
            ti.xcom_push(key='execution_plan', value=optimized_plan)
            
            logger.info(f"生成执行计划成功，共{len(plan)}个表，分为{len(optimized_plan)}个批次")
            return optimized_plan
        except Exception as e:
            error_msg = f"生成执行计划失败: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    def _execute_plan(self, **context) -> Dict[str, Dict[str, Any]]:
        """
        执行计划
        
        Args:
            **context: 上下文信息
            
        Returns:
            执行结果
        """
        ti = context['ti']
        execution_plan = ti.xcom_pull(task_ids='generate_execution_plan', key='execution_plan')
        
        if not execution_plan:
            logger.warning("没有可执行的计划")
            return {}
        
        # 创建执行上下文
        run_id = context['run_id']
        execution_context = {
            'run_id': run_id,
            'execution_type': self.frequency,
            'dag_id': context['dag'].dag_id,
            'execution_date': context['execution_date'].isoformat()
        }
        
        logger.info(f"开始执行计划，run_id: {run_id}")
        
        # 执行计划
        results = self.execution_strategy.execute_plan(execution_plan, execution_context)
        
        # 统计结果
        success_count = sum(1 for r in results.values() if r.get('status') == 'success')
        failed_count = sum(1 for r in results.values() if r.get('status') == 'failed')
        
        logger.info(f"执行计划完成，成功: {success_count}，失败: {failed_count}")
        
        return results


class ImmediateDAGTemplate(DAGTemplate):
    """立即执行DAG模板"""
    
    def __init__(self, dependency_manager: DependencyManager = None, pg_connector: PostgreSQLConnector = None):
        """
        初始化立即执行DAG模板
        
        Args:
            dependency_manager: 依赖图管理器
            pg_connector: PostgreSQL连接器
        """
        super().__init__(None, "immediate", dependency_manager, pg_connector)
    
    def create_dag_for_table(self, table_name: str) -> DAG:
        """
        为指定表创建立即执行DAG
        
        Args:
            table_name: 表名
            
        Returns:
            DAG实例
        """
        # 创建DAG ID，包含表名和时间戳
        dag_id = f"immediate_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # 创建DAG，并指定表名为参数
        dag = self.create_dag(dag_id=dag_id, params={'table_name': table_name})
        
        logger.info(f"为表 {table_name} 创建立即执行DAG: {dag_id}")
        
        return dag
    
    def _build_dag_tasks(self, dag: DAG) -> None:
        """
        构建立即执行DAG的任务
        
        Args:
            dag: DAG实例
        """
        # 开始任务
        start_task = DummyOperator(task_id='start', dag=dag)
        
        # 处理任务
        process_task = PythonOperator(
            task_id='process_immediate',
            python_callable=self._process_immediate,
            op_kwargs={},
            dag=dag,
        )
        
        # 结束任务
        end_task = DummyOperator(task_id='end', dag=dag)
        
        # 设置任务依赖
        start_task >> process_task >> end_task
    
    def _process_immediate(self, **context) -> Dict[str, Any]:
        """
        处理立即执行请求
        
        Args:
            **context: 上下文信息
            
        Returns:
            处理结果
            
        Raises:
            ValueError: 如果处理失败
        """
        # 获取参数
        params = context['params']
        table_name = params.get('table_name')
        
        if not table_name:
            error_msg = "缺少表名参数"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"立即执行表 {table_name} 的处理")
        
        # 强制刷新依赖图
        self.dependency_manager.refresh_dependency_graph()
        
        try:
            # 为表生成执行计划
            plan = self.execution_planner.generate_plan([table_name], include_dependencies=True)
            
            # 优化执行计划
            optimized_plan = self.execution_planner.optimize_plan(plan)
            
            # 创建执行上下文
            execution_context = {
                'run_id': context['run_id'],
                'execution_type': 'immediate',
                'requested_table': table_name,
                'dag_id': context['dag'].dag_id,
                'execution_date': context['execution_date'].isoformat()
            }
            
            # 执行计划
            results = self.execution_strategy.execute_plan(optimized_plan, execution_context)
            
            # 检查目标表的执行结果
            table_result = results.get(table_name)
            if table_result and table_result.get('status') == 'failed':
                error_msg = f"表 {table_name} 执行失败: {table_result.get('error')}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            return results
            
        except Exception as e:
            error_msg = f"立即执行表 {table_name} 失败: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg)


class DAGManager:
    """DAG管理器，负责创建和管理DAG"""
    
    _instance = None
    _lock = object()
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DAGManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, dependency_manager: DependencyManager = None, pg_connector: PostgreSQLConnector = None,
                airflow_api_url: str = None, airflow_auth: tuple = None):
        """
        初始化DAG管理器
        
        Args:
            dependency_manager: 依赖图管理器
            pg_connector: PostgreSQL连接器
            airflow_api_url: Airflow API基础URL
            airflow_auth: Airflow认证信息(用户名, 密码)
        """
        # 避免重复初始化
        if self._initialized:
            return
            
        self.dependency_manager = dependency_manager or DependencyManager()
        self.pg_connector = pg_connector or PostgreSQLConnector()
        
        # 初始化AirflowClient
        self.airflow_client = AirflowClient(airflow_api_url, airflow_auth)
        
        # 初始化DAG模板
        self.templates = {
            'hourly': ScheduledDAGTemplate('hourly', self.dependency_manager, self.pg_connector),
            'daily': ScheduledDAGTemplate('daily', self.dependency_manager, self.pg_connector),
            'weekly': ScheduledDAGTemplate('weekly', self.dependency_manager, self.pg_connector),
            'monthly': ScheduledDAGTemplate('monthly', self.dependency_manager, self.pg_connector),
            'immediate': ImmediateDAGTemplate(self.dependency_manager, self.pg_connector)
        }
        
        # DAG注册状态跟踪
        self.registered_dags = {}
        
        self._initialized = True
        logger.info("DAG管理器初始化完成")
    
    def _get_dag_registration_path(self, frequency: str = None) -> str:
        """
        获取DAG注册路径
        
        Args:
            frequency: 调度频率
            
        Returns:
            DAG注册路径
        """
        # 获取Airflow DAGs文件夹
        dags_folder = os.environ.get('AIRFLOW__CORE__DAGS_FOLDER', './dags')
        
        # 为不同频率的DAG设置不同的文件名
        if frequency:
            return os.path.join(dags_folder, f"smart_scheduler_{frequency}_dag.py")
        return os.path.join(dags_folder, "immediate_dag.py")
        
    def _generate_dag_code(self, dag: DAG) -> str:
        """
        生成DAG代码
        
        Args:
            dag: DAG实例
            
        Returns:
            生成的DAG代码
        """
        # 这里简化处理，实际生成代码应该更复杂
        # 通常可能需要调用某种序列化机制或使用模板引擎
        # 这里仅演示概念
        
        template = '''"""
自动生成的DAG文件 - {dag_id}
生成时间: {timestamp}
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import requests
import logging

# 默认参数
default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}}

# API地址
scheduler_api_url = Variable.get("scheduler_api_url", "http://localhost:5000")

# 创建DAG
dag = DAG(
    '{dag_id}',
    default_args=default_args,
    description='{description}',
    schedule_interval='{schedule_interval}',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['smart_scheduler', '{frequency}'],
)

# DAG的主要逻辑通过调用API实现
# 请参考具体的API文档

'''
        # 从DAG实例获取信息填充模板
        dag_info = {
            'dag_id': dag.dag_id,
            'timestamp': datetime.now().isoformat(),
            'description': dag.description or f"智能调度系统 - {dag.dag_id}",
            'schedule_interval': dag.schedule_interval or 'None',
            'frequency': next((tag for tag in dag.tags if tag != 'smart_scheduler'), 'custom')
        }
        
        return template.format(**dag_info)
    
    def register_dag(self, dag: DAG, persist: bool = True) -> Dict[str, Any]:
        """
        注册DAG到Airflow
        
        Args:
            dag: DAG实例
            persist: 是否持久化到文件系统
            
        Returns:
            注册结果
        """
        try:
            if not persist:
                # 如果不需要持久化，仅记录DAG状态
                self.registered_dags[dag.dag_id] = {
                    'dag': dag,
                    'registered_at': datetime.now(),
                    'persisted': False
                }
                
                logger.info(f"DAG已注册到内存: {dag.dag_id}")
                return {
                    'status': 'success',
                    'dag_id': dag.dag_id,
                    'message': 'DAG已注册到内存'
                }
            
            # 生成DAG代码
            dag_code = self._generate_dag_code(dag)
            
            # 获取频率信息
            frequency = next((tag for tag in dag.tags if tag in ['hourly', 'daily', 'weekly', 'monthly', 'immediate']), None)
            
            # 注册DAG到Airflow
            result = self.airflow_client.register_dynamic_dag(
                dag_id=dag.dag_id,
                dag_code=dag_code,
                unpause=True
            )
            
            if result['status'] == 'success':
                # 记录DAG注册状态
                self.registered_dags[dag.dag_id] = {
                    'dag': dag,
                    'registered_at': datetime.now(),
                    'persisted': True,
                    'file_path': self._get_dag_registration_path(frequency)
                }
                
                logger.info(f"DAG已持久化注册: {dag.dag_id}")
            
            return result
            
        except Exception as e:
            error_msg = f"注册DAG失败: {str(e)}"
            logger.error(error_msg)
            return {
                'status': 'error',
                'dag_id': dag.dag_id,
                'message': error_msg
            }
    
    def initialize_scheduled_dags(self) -> Dict[str, Dict[str, Any]]:
        """
        初始化所有周期性DAG
        
        Returns:
            DAG注册结果字典，键为频率，值为注册结果
        """
        results = {}
        
        for frequency, template in self.templates.items():
            if frequency != 'immediate':
                # 创建DAG
                dag = template.create_dag()
                
                # 注册DAG
                register_result = self.register_dag(dag, persist=True)
                results[frequency] = register_result
                
                logger.info(f"已初始化并注册{frequency}调度DAG: {dag.dag_id}, 结果: {register_result['status']}")
        
        return results
    
    def execute_immediate(self, table_name: str) -> Dict[str, Any]:
        """
        立即执行表处理
        
        Args:
            table_name: 表名
            
        Returns:
            执行结果
            
        Raises:
            ValueError: 如果执行失败
        """
        try:
            # 获取立即执行模板
            template = self.templates['immediate']
            
            # 创建立即执行DAG
            dag = template.create_dag_for_table(table_name)
            
            # 注册DAG（不持久化，仅用于本次执行）
            register_result = self.register_dag(dag, persist=False)
            
            if register_result['status'] != 'success':
                return {
                    'status': 'error',
                    'message': f"注册DAG失败: {register_result['message']}",
                    'table_name': table_name
                }
            
            # 触发DAG运行
            execution_date = datetime.now()
            execution_id = f'manual_{table_name}_{execution_date.strftime("%Y%m%d%H%M%S")}'
            
            dag_run = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=execution_date,
                run_id=execution_id,
                conf={'table_name': table_name}
            )
            
            logger.info(f"触发立即执行DAG: {dag.dag_id}, run_id: {dag_run.run_id}")
            
            return {
                'status': 'success',
                'run_id': dag_run.run_id,
                'dag_id': dag.dag_id,
                'execution_date': execution_date.isoformat(),
                'table_name': table_name
            }
            
        except Exception as e:
            error_msg = f"立即执行失败: {str(e)}"
            logger.error(error_msg)
            return {
                'status': 'error',
                'message': error_msg,
                'table_name': table_name
            }
    
    def get_dag_run_status(self, run_id: str) -> Dict[str, Any]:
        """
        获取DAG运行状态
        
        Args:
            run_id: 运行ID
            
        Returns:
            DAG运行状态
        """
        try:
            # 首先尝试从Airflow API获取状态
            dag_id = run_id.split('_')[0] if '_' in run_id else None
            
            if dag_id:
                try:
                    run_info = self.airflow_client.get_dag_run(dag_id, run_id)
                    if 'dag_run_id' in run_info or 'dag_id' in run_info:
                        return {
                            'run_id': run_id,
                            'dag_id': run_info.get('dag_id', dag_id),
                            'state': run_info.get('state'),
                            'start_date': run_info.get('start_date'),
                            'end_date': run_info.get('end_date'),
                            'source': 'airflow_api'
                        }
                except:
                    # 如果API调用失败，回退到数据库查询
                    pass
            
            # 查询数据库获取执行状态
            query = """
                SELECT table_name, execution_type, start_time, end_time, 
                       status, retry_count, error_message
                FROM execution_history
                WHERE execution_id = %s
            """
            result = self.pg_connector.execute_query(query, (run_id,))
            
            if not result:
                return {
                    'status': 'not_found',
                    'run_id': run_id,
                    'message': f'找不到执行ID: {run_id}'
                }
            
            row = result[0]
            return {
                'run_id': run_id,
                'table_name': row[0],
                'execution_type': row[1],
                'start_time': row[2].isoformat() if row[2] else None,
                'end_time': row[3].isoformat() if row[3] else None,
                'status': row[4],
                'retry_count': row[5],
                'error_message': row[6],
                'source': 'database'
            }
            
        except Exception as e:
            logger.error(f"获取DAG运行状态失败: {str(e)}")
            return {
                'status': 'error',
                'run_id': run_id,
                'message': f'获取状态失败: {str(e)}'
            }
    
    def get_all_registered_dags(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有已注册的DAG
        
        Returns:
            DAG信息字典
        """
        return self.registered_dags 