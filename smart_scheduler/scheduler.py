"""
调度器模块，负责协调各个组件，管理系统生命周期
"""
import os
import sys
import json
import time
import logging
import threading
from datetime import datetime
from typing import Dict, Any, Optional

from .core.dependency_manager import DependencyManager
from .core.dag_generator import DAGManager
from .api.app import SchedulerAPI
from .db.connectors import PostgreSQLConnector, Neo4jConnector
from .monitoring import MonitoringService

# 设置日志
logger = logging.getLogger(__name__)


class Scheduler:
    """调度器，系统的主要协调组件"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(Scheduler, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self, config_file: str = None):
        """
        初始化调度器
        
        Args:
            config_file: 配置文件路径
        """
        # 避免重复初始化
        if self._initialized:
            return
            
        logger.info("开始初始化调度器...")
        
        # 加载配置
        self.config = self._load_config(config_file)
        
        # 初始化连接器
        self.pg_conn = PostgreSQLConnector(self.config.get('postgresql'))
        self.neo4j_conn = Neo4jConnector(
            self.config.get('neo4j', {}).get('uri'),
            self.config.get('neo4j', {}).get('user'),
            self.config.get('neo4j', {}).get('password')
        )
        
        # 初始化依赖图管理器
        self.dependency_manager = DependencyManager(self.neo4j_conn, self.pg_conn)
        
        # 初始化DAG管理器
        self.dag_manager = DAGManager(
            self.dependency_manager,
            self.pg_conn,
            self.config.get('airflow', {}).get('api_url'),
            self.config.get('airflow', {}).get('auth')
        )
        
        # 初始化监控服务
        self.monitoring_service = MonitoringService(self.pg_conn)
        
        # 初始化API服务
        self.api_service = SchedulerAPI(
            self.dependency_manager,
            self.dag_manager,
            self.pg_conn
        )
        
        # 更新检查间隔（秒）
        self.update_check_interval = int(self.config.get('update_check_interval', 900))  # 默认15分钟
        
        # 控制标志
        self.running = False
        self.update_check_thread = None
        
        self._initialized = True
        logger.info("调度器初始化完成")
    
    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """
        加载配置
        
        Args:
            config_file: 配置文件路径
            
        Returns:
            配置字典
        """
        if config_file and os.path.exists(config_file):
            logger.info(f"从文件 {config_file} 加载配置")
            try:
                with open(config_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"加载配置文件失败: {str(e)}")
        
        # 默认配置
        logger.info("使用默认配置")
        return {
            'postgresql': os.getenv('PG_CONN_STRING', 'postgresql://postgres:postgres@localhost:5432/dataops'),
            'neo4j': {
                'uri': os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
                'user': os.getenv('NEO4J_USER', 'neo4j'),
                'password': os.getenv('NEO4J_PASSWORD', 'neo4j')
            },
            'airflow': {
                'api_url': os.getenv('AIRFLOW_API_URL', 'http://localhost:8080/api/v1'),
                'auth': (os.getenv('AIRFLOW_USER', 'airflow'), os.getenv('AIRFLOW_PASSWORD', 'airflow'))
            },
            'update_check_interval': int(os.getenv('UPDATE_CHECK_INTERVAL', '900')),  # 15分钟
            'api_host': os.getenv('API_HOST', '0.0.0.0'),
            'api_port': int(os.getenv('API_PORT', '5000')),
            'debug': os.getenv('DEBUG', 'false').lower() == 'true'
        }
    
    def start(self) -> None:
        """启动调度器"""
        if self.running:
            logger.warning("调度器已在运行")
            return
        
        logger.info("启动调度器...")
        self.running = True
        
        # 初始化依赖图
        logger.info("初始化依赖图...")
        self.dependency_manager.refresh_dependency_graph()
        
        # 初始化周期性DAG
        logger.info("初始化周期性DAG...")
        self.dag_manager.initialize_scheduled_dags()
        
        # 启动更新检查线程
        self.update_check_thread = threading.Thread(
            target=self._update_check_loop,
            daemon=True
        )
        self.update_check_thread.start()
        logger.info("已启动依赖图更新检查线程")
        
        # 启动API服务
        api_host = self.config.get('api_host', '0.0.0.0')
        api_port = int(self.config.get('api_port', 5000))
        debug_mode = self.config.get('debug', False)
        
        logger.info(f"启动API服务在 {api_host}:{api_port}, 调试模式: {debug_mode}")
        try:
            self.api_service.run(
                host=api_host,
                port=api_port,
                debug=debug_mode
            )
        except Exception as e:
            logger.error(f"API服务启动失败: {str(e)}")
            self.stop()
    
    def stop(self) -> None:
        """停止调度器"""
        if not self.running:
            logger.warning("调度器未运行")
            return
        
        logger.info("停止调度器...")
        self.running = False
        
        # 等待更新检查线程结束
        if self.update_check_thread and self.update_check_thread.is_alive():
            logger.info("等待更新检查线程结束...")
            self.update_check_thread.join(timeout=10)
        
        # 关闭Neo4j连接
        logger.info("关闭Neo4j连接...")
        self.neo4j_conn.close()
        
        # 执行清理任务
        logger.info("执行清理任务...")
        self._cleanup()
        
        logger.info("调度器已停止")
    
    def _update_check_loop(self) -> None:
        """更新检查循环"""
        logger.info(f"更新检查循环启动，间隔: {self.update_check_interval}秒")
        
        while self.running:
            try:
                # 检查依赖图是否需要更新
                logger.debug("检查依赖图更新...")
                updated = self.dependency_manager.check_updates_and_refresh()
                
                if updated:
                    logger.info(f"依赖图已更新，时间: {datetime.now()}")
                    
                    # 执行数据清理
                    self._perform_maintenance()
            except Exception as e:
                logger.error(f"更新检查失败: {str(e)}")
            
            # 等待下一次检查
            for _ in range(self.update_check_interval):
                if not self.running:
                    break
                time.sleep(1)
    
    def _perform_maintenance(self) -> None:
        """执行维护任务"""
        try:
            # 清理旧的执行历史
            result = self.monitoring_service.cleanup_old_history()
            logger.info(f"清理了 {result.get('deleted_count', 0)} 条历史记录")
            
            # 其他维护任务...
        except Exception as e:
            logger.error(f"执行维护任务失败: {str(e)}")
    
    def _cleanup(self) -> None:
        """执行清理任务"""
        try:
            # 执行最终清理任务
            pass
        except Exception as e:
            logger.error(f"执行清理任务失败: {str(e)}")
    
    @staticmethod
    def get_instance(config_file: str = None) -> 'Scheduler':
        """
        获取调度器实例
        
        Args:
            config_file: 配置文件路径
            
        Returns:
            调度器实例
        """
        return Scheduler(config_file) 