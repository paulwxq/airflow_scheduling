"""
Flask API应用，提供REST接口与数据运营平台交互
"""
import json
import uuid
import logging
from datetime import datetime
from typing import Dict, Any, List

from flask import Flask, jsonify, request, Response

from ..core.dependency_manager import DependencyManager
from ..core.execution_planner import ExecutionPlanGenerator
from ..core.execution_strategy import ExecutionStrategy
from ..db.connectors import PostgreSQLConnector

# 设置日志
logger = logging.getLogger(__name__)


class SchedulerAPI:
    """调度API类，负责创建Flask应用并提供API路由"""
    
    def __init__(self):
        """初始化API"""
        self.app = Flask(__name__)
        
        # 初始化组件
        self.dependency_manager = DependencyManager()
        self.pg_connector = PostgreSQLConnector()
        self.execution_planner = ExecutionPlanGenerator(self.dependency_manager, self.pg_connector)
        self.execution_strategy = ExecutionStrategy(self.pg_connector)
        
        # 设置路由
        self._setup_routes()
        
        logger.info("调度API初始化完成")
    
    def close(self):
        """关闭API服务器并释放资源"""
        logger.info("正在关闭API服务器和资源...")
        # 关闭PostgreSQL连接
        if hasattr(self, 'pg_connector'):
            self.pg_connector.close()
        
        # 关闭Neo4j连接 (通过dependency_manager)
        if hasattr(self, 'dependency_manager') and hasattr(self.dependency_manager, 'neo4j_connector'):
            self.dependency_manager.neo4j_connector.close()
        
        logger.info("API服务器资源已释放")
    
    def _setup_routes(self) -> None:
        """设置API路由"""
        # 依赖图管理
        self.app.route('/api/v1/dependency/refresh', methods=['POST'])(self.refresh_dependency)
        self.app.route('/api/v1/dependency/status', methods=['GET'])(self.get_dependency_status)
        
        # 立即执行
        self.app.route('/api/v1/execute/immediate', methods=['POST'])(self.execute_immediate)
        
        # 执行状态
        self.app.route('/api/v1/status/<execution_id>', methods=['GET'])(self.get_execution_status)
        
        # 配置管理
        self.app.route('/api/v1/config/<key>', methods=['GET', 'PUT'])(self.handle_config)
        
        # 表管理
        self.app.route('/api/v1/tables', methods=['GET'])(self.get_all_tables)
        self.app.route('/api/v1/tables/<table_name>/dependencies', methods=['GET'])(self.get_table_dependencies)
        
        # 健康检查
        self.app.route('/health', methods=['GET'])(self.health_check)
    
    def refresh_dependency(self) -> Response:
        """
        刷新依赖图
        
        Returns:
            API响应
        """
        try:
            was_refreshed = self.dependency_manager.refresh_dependency_graph()
            
            if was_refreshed:
                return jsonify({
                    'status': 'success',
                    'message': '依赖图已刷新',
                    'timestamp': datetime.now().isoformat()
                }), 200
            else:
                return jsonify({
                    'status': 'error',
                    'message': '依赖图刷新失败',
                    'timestamp': datetime.now().isoformat()
                }), 500
        except Exception as e:
            logger.error(f"刷新依赖图失败: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500
    
    def get_dependency_status(self) -> Response:
        """
        获取依赖图状态
        
        Returns:
            API响应
        """
        try:
            # 获取节点和边的数量
            node_count = len(self.dependency_manager.get_all_tables())
            
            # 检查是否有循环依赖
            cycles = self.dependency_manager.has_cycles()
            
            # 获取上次刷新时间
            last_refresh_time = datetime.fromtimestamp(self.dependency_manager.last_refresh_time)
            
            return jsonify({
                'status': 'success',
                'node_count': node_count,
                'has_cycles': len(cycles) > 0,
                'cycles': cycles,
                'last_refresh_time': last_refresh_time.isoformat()
            }), 200
        except Exception as e:
            logger.error(f"获取依赖图状态失败: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500
    
    def execute_immediate(self) -> Response:
        """
        立即执行表处理
        
        Returns:
            API响应
        """
        try:
            data = request.json
            table_name = data.get('table_name')
            
            if not table_name:
                return jsonify({
                    'status': 'error',
                    'message': '缺少表名参数'
                }), 400
            
            # 强制刷新依赖图，确保使用最新依赖关系
            self.dependency_manager.refresh_dependency_graph()
            
            # 生成执行计划
            try:
                plan = self.execution_planner.generate_plan([table_name], include_dependencies=True)
            except ValueError as e:
                return jsonify({
                    'status': 'error',
                    'message': str(e)
                }), 400
            
            # 优化执行计划
            try:
                optimized_plan = self.execution_planner.optimize_plan(plan)
            except ValueError as e:
                return jsonify({
                    'status': 'error',
                    'message': str(e)
                }), 400
            
            # 创建执行上下文
            execution_id = f'immediate_{uuid.uuid4()}'
            context = {
                'run_id': execution_id,
                'execution_type': 'immediate',
                'request_time': datetime.now().isoformat(),
                'requested_by': request.headers.get('X-User', 'unknown'),
                'requested_table': table_name
            }
            
            # 异步执行计划（实际项目中，应该使用后台任务）
            # 这里为了简化，同步执行
            results = self.execution_strategy.execute_plan(optimized_plan, context)
            
            # 检查表的执行结果
            table_result = results.get(table_name, {})
            status = table_result.get('status', 'unknown')
            
            if status == 'success':
                return jsonify({
                    'status': 'success',
                    'execution_id': execution_id,
                    'message': f'表 {table_name} 处理成功',
                    'timestamp': datetime.now().isoformat(),
                    'plan_size': len(plan),
                    'batch_count': len(optimized_plan)
                }), 200
            else:
                return jsonify({
                    'status': 'error',
                    'execution_id': execution_id,
                    'message': f'表 {table_name} 处理失败: {table_result.get("error", "未知错误")}',
                    'timestamp': datetime.now().isoformat()
                }), 500
                
        except Exception as e:
            logger.error(f"立即执行失败: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500
    
    def get_execution_status(self, execution_id: str) -> Response:
        """
        获取执行状态
        
        Args:
            execution_id: 执行ID
            
        Returns:
            API响应
        """
        try:
            query = """
                SELECT table_name, execution_type, start_time, end_time, 
                       status, retry_count, error_message
                FROM execution_history
                WHERE execution_id = %s
            """
            result = self.pg_connector.execute_query(query, (execution_id,))
            
            if not result:
                return jsonify({
                    'status': 'error',
                    'message': f'找不到执行ID: {execution_id}'
                }), 404
            
            row = result[0]
            return jsonify({
                'execution_id': execution_id,
                'table_name': row[0],
                'execution_type': row[1],
                'start_time': row[2].isoformat() if row[2] else None,
                'end_time': row[3].isoformat() if row[3] else None,
                'status': row[4],
                'retry_count': row[5],
                'error_message': row[6]
            }), 200
        except Exception as e:
            logger.error(f"获取执行状态失败: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500
    
    def handle_config(self, key: str) -> Response:
        """
        处理配置
        
        Args:
            key: 配置键
            
        Returns:
            API响应
        """
        try:
            # GET请求，获取配置
            if request.method == 'GET':
                value = self.pg_connector.get_system_config(key)
                if value is None:
                    return jsonify({
                        'status': 'error',
                        'message': f'找不到配置项: {key}'
                    }), 404
                
                return jsonify({
                    'key': key,
                    'value': value
                }), 200
            
            # PUT请求，更新配置
            elif request.method == 'PUT':
                data = request.json
                value = data.get('value')
                
                if value is None:
                    return jsonify({
                        'status': 'error',
                        'message': '缺少value参数'
                    }), 400
                
                self.pg_connector.update_system_config(key, value)
                return jsonify({
                    'status': 'success',
                    'message': f'配置已更新: {key} = {value}'
                }), 200
        except Exception as e:
            logger.error(f"处理配置失败: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500
    
    def get_all_tables(self) -> Response:
        """
        获取所有表
        
        Returns:
            API响应
        """
        try:
            # 从依赖图获取所有表
            all_tables = self.dependency_manager.get_all_tables()
            
            # 从PostgreSQL获取表的调度信息
            scheduled_tables = []
            query = """
                SELECT table_name, schedule_frequency, is_enabled, priority
                FROM table_schedule
            """
            results = self.pg_connector.execute_query(query)
            
            table_info = {}
            for row in results:
                table_info[row[0]] = {
                    'schedule_frequency': row[1],
                    'is_enabled': row[2],
                    'priority': row[3]
                }
            
            # 合并信息
            tables = []
            for table in all_tables:
                info = table_info.get(table, {})
                tables.append({
                    'name': table,
                    'schedule_frequency': info.get('schedule_frequency'),
                    'is_enabled': info.get('is_enabled', False),
                    'priority': info.get('priority', 5)
                })
            
            return jsonify({
                'status': 'success',
                'count': len(tables),
                'tables': tables
            }), 200
        except Exception as e:
            logger.error(f"获取所有表失败: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500
    
    def get_table_dependencies(self, table_name: str) -> Response:
        """
        获取表的依赖
        
        Args:
            table_name: 表名
            
        Returns:
            API响应
        """
        try:
            # 获取表的依赖
            dependencies = self.dependency_manager.get_table_dependencies(table_name)
            
            # 获取依赖于此表的表
            dependents = self.dependency_manager.get_table_dependents(table_name)
            
            return jsonify({
                'status': 'success',
                'table_name': table_name,
                'dependencies': dependencies,
                'dependents': dependents
            }), 200
        except Exception as e:
            logger.error(f"获取表依赖失败: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500
    
    def health_check(self) -> Response:
        """
        健康检查
        
        Returns:
            API响应
        """
        return jsonify({
            'status': 'ok',
            'timestamp': datetime.now().isoformat()
        }), 200
    
    def run(self, host: str = '0.0.0.0', port: int = 5000, debug: bool = False) -> None:
        """
        运行API服务
        
        Args:
            host: 主机
            port: 端口
            debug: 是否开启调试模式
        """
        self.app.run(host=host, port=port, debug=debug)


def create_app() -> Flask:
    """
    创建Flask应用
    
    Returns:
        Flask应用
    """
    api = SchedulerAPI()
    return api.app


if __name__ == '__main__':
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建API并运行
    api = SchedulerAPI()
    api.run(debug=True) 