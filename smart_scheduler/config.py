"""
智能调度系统配置文件
包含所有智能调度系统运行所需的配置参数
"""
import os
import logging

# 日志级别配置
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# 数据库连接配置
PG_CONN_STRING = os.getenv('PG_CONN_STRING', 'postgresql://postgres:postgres@localhost:5432/dataops')

# Neo4j配置
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'neo4j')

# Airflow API配置
AIRFLOW_API_URL = os.getenv('AIRFLOW_API_URL', 'http://localhost:8080/api/v1')
AIRFLOW_USER = os.getenv('AIRFLOW_USER', 'airflow')
AIRFLOW_PASSWORD = os.getenv('AIRFLOW_PASSWORD', 'airflow')

# API服务配置
API_HOST = os.getenv('API_HOST', '0.0.0.0')
API_PORT = int(os.getenv('API_PORT', '5000'))
DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'

# 更新间隔配置
UPDATE_CHECK_INTERVAL = int(os.getenv('UPDATE_CHECK_INTERVAL', '900'))  # 15分钟

# 执行配置
MAX_CONCURRENCY = int(os.getenv('MAX_CONCURRENCY', '10'))
MAX_RETRY = int(os.getenv('MAX_RETRY', '5'))
RETRY_DELAY_MINUTES = int(os.getenv('RETRY_DELAY_MINUTES', '1'))
HISTORY_RETENTION_DAYS = int(os.getenv('HISTORY_RETENTION_DAYS', '180'))

# 监控配置
ENABLE_EMAIL_ALERTS = os.getenv('ENABLE_EMAIL_ALERTS', 'false').lower() == 'true'
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.example.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '25'))
SMTP_USER = os.getenv('SMTP_USER', '')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', '')
SMTP_FROM = os.getenv('SMTP_FROM', 'scheduler@example.com')
ALERT_RECIPIENTS = os.getenv('ALERT_RECIPIENTS', 'admin@example.com').split(',')

# 日志配置
def configure_logging():
    """配置日志"""
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

# 在导入配置模块时自动加载本地配置覆盖（如果存在）
try:
    from smart_scheduler.config_local import *
    logging.info("已加载本地配置覆盖")
except ImportError:
    pass 