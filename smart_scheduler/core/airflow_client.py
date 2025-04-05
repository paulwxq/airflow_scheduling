"""
Airflow API客户端模块，提供与Airflow API交互的功能
"""
import os
import json
import logging
import requests
import time
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

# 设置日志
logger = logging.getLogger(__name__)


class AirflowClient:
    """Airflow API客户端，用于与Airflow交互"""
    
    def __init__(self, api_url: str = None, auth: tuple = None, timeout: int = 30):
        """
        初始化Airflow API客户端
        
        Args:
            api_url: Airflow API基础URL，例如 'http://localhost:8080/api/v1'
            auth: 认证信息 (username, password)
            timeout: 请求超时时间（秒）
        """
        self.api_url = api_url or os.environ.get('AIRFLOW_API_URL', 'http://localhost:8080/api/v1')
        self.auth = auth or (
            os.environ.get('AIRFLOW_USERNAME', 'airflow'),
            os.environ.get('AIRFLOW_PASSWORD', 'airflow')
        )
        self.timeout = timeout
        
        self.session = requests.Session()
        self.session.auth = self.auth
        
        logger.info(f"初始化Airflow客户端: {self.api_url}")
    
    def _make_request(self, method: str, endpoint: str, data: Dict = None, params: Dict = None) -> Dict:
        """
        发送API请求
        
        Args:
            method: HTTP方法，如 'GET', 'POST', 'PUT', 'DELETE'
            endpoint: API端点，不包含基础URL
            data: 请求数据，用于POST/PUT请求
            params: 查询参数
            
        Returns:
            响应数据
            
        Raises:
            Exception: 请求失败时抛出异常
        """
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        
        try:
            logger.debug(f"发送 {method} 请求到 {url}")
            response = self.session.request(
                method=method,
                url=url,
                json=data,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            if response.content:
                return response.json()
            return {}
            
        except requests.RequestException as e:
            error_msg = f"Airflow API请求失败: {str(e)}"
            logger.error(error_msg)
            
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_details = e.response.json()
                    logger.error(f"错误详情: {json.dumps(error_details)}")
                except:
                    logger.error(f"响应状态码: {e.response.status_code}, 内容: {e.response.text}")
            
            raise Exception(error_msg)
    
    def get_dags(self, only_active: bool = True) -> List[Dict]:
        """
        获取所有DAG
        
        Args:
            only_active: 是否只返回激活的DAG
            
        Returns:
            DAG列表
        """
        params = {'limit': 100}
        if only_active:
            params['only_active'] = 'true'
            
        response = self._make_request('GET', 'dags', params=params)
        return response.get('dags', [])
    
    def get_dag(self, dag_id: str) -> Dict:
        """
        获取指定DAG的详细信息
        
        Args:
            dag_id: DAG ID
            
        Returns:
            DAG详细信息
        """
        return self._make_request('GET', f'dags/{dag_id}')
    
    def create_or_update_dag_code(self, dag_id: str, dag_code: str, dag_file_path: str = None) -> bool:
        """
        创建或更新DAG代码文件
        
        Args:
            dag_id: DAG ID
            dag_code: DAG代码内容
            dag_file_path: DAG文件路径，如果为None则使用默认路径
            
        Returns:
            是否成功
        """
        try:
            # 获取DAG文件存储路径
            dags_folder = os.environ.get('AIRFLOW__CORE__DAGS_FOLDER', '/opt/airflow/dags')
            
            if dag_file_path is None:
                dag_file_path = os.path.join(dags_folder, f"{dag_id}.py")
            
            # 创建目录
            os.makedirs(os.path.dirname(dag_file_path), exist_ok=True)
            
            # 写入文件
            with open(dag_file_path, 'w') as f:
                f.write(dag_code)
                
            logger.info(f"DAG代码已写入文件: {dag_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"写入DAG代码失败: {str(e)}")
            return False
    
    def pause_dag(self, dag_id: str) -> Dict:
        """
        暂停DAG
        
        Args:
            dag_id: DAG ID
            
        Returns:
            操作结果
        """
        return self._make_request('PATCH', f'dags/{dag_id}', data={'is_paused': True})
    
    def unpause_dag(self, dag_id: str) -> Dict:
        """
        恢复DAG
        
        Args:
            dag_id: DAG ID
            
        Returns:
            操作结果
        """
        return self._make_request('PATCH', f'dags/{dag_id}', data={'is_paused': False})
    
    def trigger_dag(self, dag_id: str, conf: Dict = None, execution_date: str = None) -> Dict:
        """
        触发DAG运行
        
        Args:
            dag_id: DAG ID
            conf: 配置参数
            execution_date: 执行日期，ISO格式字符串
            
        Returns:
            运行详情
        """
        data = {
            'conf': conf or {}
        }
        
        if execution_date:
            data['execution_date'] = execution_date
            
        return self._make_request('POST', f'dags/{dag_id}/dagRuns', data=data)
    
    def get_dag_runs(self, dag_id: str, state: str = None, limit: int = 100) -> List[Dict]:
        """
        获取DAG运行记录
        
        Args:
            dag_id: DAG ID
            state: 状态过滤，如 'running', 'success', 'failed'
            limit: 最大返回数量
            
        Returns:
            运行记录列表
        """
        params = {'limit': limit}
        if state:
            params['state'] = state
            
        response = self._make_request('GET', f'dags/{dag_id}/dagRuns', params=params)
        return response.get('dag_runs', [])
    
    def get_dag_run(self, dag_id: str, run_id: str) -> Dict:
        """
        获取DAG运行详情
        
        Args:
            dag_id: DAG ID
            run_id: 运行ID
            
        Returns:
            运行详情
        """
        return self._make_request('GET', f'dags/{dag_id}/dagRuns/{run_id}')
    
    def get_task_instances(self, dag_id: str, run_id: str) -> List[Dict]:
        """
        获取任务实例列表
        
        Args:
            dag_id: DAG ID
            run_id: 运行ID
            
        Returns:
            任务实例列表
        """
        response = self._make_request('GET', f'dags/{dag_id}/dagRuns/{run_id}/taskInstances')
        return response.get('task_instances', [])
    
    def register_dynamic_dag(self, dag_id: str, dag_code: str, unpause: bool = True, 
                           wait_for_registration: bool = True, timeout: int = 60) -> Dict:
        """
        注册动态生成的DAG
        
        Args:
            dag_id: DAG ID
            dag_code: DAG代码内容
            unpause: 注册后是否自动启用DAG
            wait_for_registration: 是否等待DAG注册完成
            timeout: 等待超时时间（秒）
            
        Returns:
            注册结果
        """
        # 1. 写入DAG代码文件
        success = self.create_or_update_dag_code(dag_id, dag_code)
        if not success:
            return {'status': 'error', 'message': 'DAG代码文件创建失败'}
        
        # 2. 等待DAG被Airflow调度器解析
        if wait_for_registration:
            start_time = time.time()
            dag_found = False
            
            while time.time() - start_time < timeout:
                try:
                    dag_info = self.get_dag(dag_id)
                    if dag_info.get('dag_id') == dag_id:
                        dag_found = True
                        break
                except:
                    pass
                
                # 等待一段时间再重试
                time.sleep(2)
            
            if not dag_found:
                return {'status': 'error', 'message': f'等待DAG注册超时，DAG可能未被正确解析: {dag_id}'}
        
        # 3. 如果需要，启用DAG
        if unpause:
            try:
                self.unpause_dag(dag_id)
                logger.info(f"DAG已启用: {dag_id}")
            except Exception as e:
                logger.warning(f"启用DAG失败: {dag_id}, 错误: {str(e)}")
        
        return {
            'status': 'success',
            'dag_id': dag_id,
            'message': 'DAG已成功注册' + (' 并启用' if unpause else '')
        }
    
    def wait_for_dag_run_completion(self, dag_id: str, run_id: str, 
                                  timeout: int = 3600, polling_interval: int = 10) -> Dict:
        """
        等待DAG运行完成
        
        Args:
            dag_id: DAG ID
            run_id: 运行ID
            timeout: 最大等待时间（秒）
            polling_interval: 轮询间隔（秒）
            
        Returns:
            DAG运行结果
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                run_info = self.get_dag_run(dag_id, run_id)
                state = run_info.get('state')
                
                # 检查是否完成
                if state in ['success', 'failed']:
                    return {
                        'status': 'completed',
                        'state': state,
                        'dag_id': dag_id,
                        'run_id': run_id,
                        'execution_date': run_info.get('execution_date'),
                        'start_date': run_info.get('start_date'),
                        'end_date': run_info.get('end_date')
                    }
                
                # 如果仍在运行，等待下一个轮询间隔
                logger.debug(f"DAG运行中: {dag_id}/{run_id}, 状态: {state}")
                time.sleep(polling_interval)
                
            except Exception as e:
                logger.error(f"获取DAG运行状态失败: {str(e)}")
                time.sleep(polling_interval)
        
        # 超时
        return {
            'status': 'timeout',
            'message': f'等待DAG运行完成超时: {dag_id}/{run_id}',
            'dag_id': dag_id,
            'run_id': run_id
        } 