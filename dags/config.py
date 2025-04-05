"""
智能调度系统 - Airflow DAG配置文件
包含所有DAG文件所需的配置参数
"""
import os
from datetime import timedelta
from airflow.models import Variable

# DAG基础配置
DAG_OWNER = Variable.get('dag_owner', 'airflow')
DEFAULT_RETRIES = int(Variable.get('default_retries', '5'))
RETRY_DELAY_MINUTES = int(Variable.get('retry_delay_minutes', '1'))
DEFAULT_CATCHUP = Variable.get('default_catchup', 'false').lower() == 'true'

# 调度器API配置
SCHEDULER_API_URL = Variable.get('scheduler_api_url', 'http://localhost:5000')
SCHEDULER_CONFIG_PATH = Variable.get('scheduler_config_path', '/etc/smart_scheduler/config.json')

# 调度时间配置
HOURLY_SCHEDULE = Variable.get('hourly_schedule', '5 * * * *')  # 每小时第5分钟
DAILY_SCHEDULE = Variable.get('daily_schedule', '0 1 * * *')    # 每天凌晨1点
WEEKLY_SCHEDULE = Variable.get('weekly_schedule', '0 2 * * 1')  # 每周一凌晨2点
MONTHLY_SCHEDULE = Variable.get('monthly_schedule', '0 3 1 * *') # 每月1日凌晨3点

# 执行超时配置
DEFAULT_EXECUTION_TIMEOUT = int(Variable.get('execution_timeout_minutes', '60'))
MAX_POLL_TIME_MINUTES = int(Variable.get('max_poll_time_minutes', '30'))
POLL_INTERVAL_SECONDS = int(Variable.get('poll_interval_seconds', '10'))

# API连接超时配置
API_TIMEOUT_SHORT = int(Variable.get('api_timeout_short', '30'))
API_TIMEOUT_MEDIUM = int(Variable.get('api_timeout_medium', '60'))
API_TIMEOUT_LONG = int(Variable.get('api_timeout_long', '300'))

# 默认DAG参数
DEFAULT_ARGS = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': DEFAULT_RETRIES,
    'retry_delay': timedelta(minutes=RETRY_DELAY_MINUTES),
    'execution_timeout': timedelta(minutes=DEFAULT_EXECUTION_TIMEOUT)
}

# DAG ID命名规范
def get_dag_id(frequency, version='v1'):
    """生成标准化的DAG ID"""
    return f'smart_scheduler_{frequency}_{version}'

# 尝试读取本地覆盖配置
try:
    from config_local import *
except ImportError:
    pass 