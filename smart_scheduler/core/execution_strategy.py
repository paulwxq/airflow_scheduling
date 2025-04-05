"""
执行策略模块，负责执行表处理任务并处理失败情况
"""
import os
import sys
import uuid
import logging
import concurrent.futures
import traceback
from typing import List, Dict, Any, Optional, Set, Tuple, Union

from ..db.connectors import PostgreSQLConnector

# 设置日志
logger = logging.getLogger(__name__)


class ExecutionStrategy:
    """执行策略类，负责实际执行表处理任务"""
    
    def __init__(self, pg_connector: PostgreSQLConnector = None):
        """
        初始化执行策略
        
        Args:
            pg_connector: PostgreSQL连接器
        """
        self.pg_connector = pg_connector or PostgreSQLConnector()
        logger.info("执行策略初始化完成")
    
    def execute_plan(self, plan: Union[List[str], List[List[str]]], context: Dict[str, Any] = None) -> Dict[str, Dict[str, Any]]:
        """
        执行计划
        
        Args:
            plan: 执行计划，可以是表名列表或分批次的表名列表
            context: 执行上下文，包含run_id等信息
            
        Returns:
            执行结果，键为表名，值为执行结果字典
        """
        if not plan:
            return {}
        
        if context is None:
            context = {'run_id': f'manual_{uuid.uuid4()}', 'execution_type': 'manual'}
        
        results = {}
        
        # 如果计划是分批次的（优化后的计划）
        if isinstance(plan[0], list):
            logger.info(f"开始执行批量计划，共 {len(plan)} 个批次")
            for i, batch in enumerate(plan):
                logger.info(f"执行第 {i+1}/{len(plan)} 批次，共 {len(batch)} 个表")
                batch_results = self._execute_batch(batch, context)
                results.update(batch_results)
        else:
            # 单批次计划
            logger.info(f"执行单批次计划，共 {len(plan)} 个表")
            results = self._execute_batch(plan, context)
        
        # 统计执行结果
        success_count = sum(1 for r in results.values() if r.get('status') == 'success')
        failed_count = sum(1 for r in results.values() if r.get('status') == 'failed')
        
        logger.info(f"计划执行完成，成功: {success_count}，失败: {failed_count}")
        return results
    
    def _execute_batch(self, batch: List[str], context: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """
        执行一批表处理任务
        
        Args:
            batch: 表名列表
            context: 执行上下文
            
        Returns:
            执行结果，键为表名，值为执行结果字典
        """
        results = {}
        
        # 获取最大并行度
        max_workers = min(len(batch), int(self.pg_connector.get_system_config('max_concurrency', '4')))
        
        # 并行执行任务
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_table = {
                executor.submit(self._process_table, table, context): table
                for table in batch
            }
            
            # 获取结果
            for future in concurrent.futures.as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    result = future.result()
                    results[table] = {
                        'status': 'success',
                        'result': result
                    }
                    logger.info(f"表 {table} 处理成功")
                except Exception as e:
                    error_msg = str(e)
                    error_stack = traceback.format_exc()
                    logger.error(f"表 {table} 处理失败: {error_msg}\n{error_stack}")
                    results[table] = {
                        'status': 'failed',
                        'error': error_msg,
                        'error_stack': error_stack
                    }
                    
                    # 处理失败情况
                    retry_info = self._handle_failure(table, context, error_msg)
                    results[table]['retry_info'] = retry_info
        
        return results
    
    def _process_table(self, table_name: str, context: Dict[str, Any]) -> Any:
        """
        处理单个表
        
        Args:
            table_name: 表名
            context: 执行上下文
            
        Returns:
            处理结果
            
        Raises:
            Exception: 处理过程中的任何异常
        """
        # 记录开始执行
        execution_id = context.get('run_id', str(uuid.uuid4()))
        execution_type = context.get('execution_type', 'manual')
        self._record_execution_start(table_name, execution_id, execution_type)
        
        try:
            # 获取表的处理脚本
            script = self._get_processing_script(table_name)
            
            # 根据脚本类型执行
            if script['script_type'] == 'python':
                result = self._execute_python_script(script['script_content'], script['script_path'], table_name, context)
            elif script['script_type'] == 'sql':
                result = self._execute_sql_script(script['script_content'], script['script_path'], table_name, context)
            else:
                raise ValueError(f"不支持的脚本类型: {script['script_type']}")
            
            # 记录执行成功
            self._record_execution_complete(table_name, execution_id, 'success')
            return result
            
        except Exception as e:
            # 记录执行失败
            self._record_execution_complete(table_name, execution_id, 'failed', str(e))
            raise
    
    def _get_processing_script(self, table_name: str) -> Dict[str, str]:
        """
        获取表的处理脚本
        
        Args:
            table_name: 表名
            
        Returns:
            脚本信息字典
            
        Raises:
            ValueError: 如果找不到表的处理脚本
        """
        try:
            return self.pg_connector.get_processing_script(table_name)
        except Exception as e:
            raise ValueError(f"获取表 {table_name} 的处理脚本失败: {str(e)}")
    
    def _execute_python_script(self, script_content: str, script_path: str, table_name: str, context: Dict[str, Any]) -> Any:
        """
        执行Python脚本
        
        Args:
            script_content: 脚本内容
            script_path: 脚本路径
            table_name: 表名
            context: 执行上下文
            
        Returns:
            脚本执行结果
            
        Raises:
            Exception: 执行过程中的任何异常
        """
        # 判断是内容还是路径
        if script_content:
            # 直接执行脚本内容
            script_to_execute = script_content
            logger.info(f"执行表 {table_name} 的内联Python脚本")
        elif script_path:
            # 从文件读取脚本内容
            try:
                with open(script_path, 'r', encoding='utf-8') as f:
                    script_to_execute = f.read()
                logger.info(f"执行表 {table_name} 的Python脚本文件: {script_path}")
            except Exception as e:
                raise ValueError(f"读取脚本文件 {script_path} 失败: {str(e)}")
        else:
            raise ValueError(f"表 {table_name} 没有提供脚本内容或路径")
        
        # 创建执行环境
        globals_dict = {
            'table_name': table_name,
            'context': context,
            'pg_connector': self.pg_connector,
            'logger': logger,
            'os': os,
            'sys': sys,
            # 其他需要的上下文变量和模块
        }
        
        # 执行脚本
        try:
            exec(script_to_execute, globals_dict)
            # 返回结果变量，如果脚本中定义了result变量
            return globals_dict.get('result', None)
        except Exception as e:
            error_stack = traceback.format_exc()
            raise Exception(f"执行Python脚本失败: {str(e)}\n{error_stack}")
    
    def _execute_sql_script(self, script_content: str, script_path: str, table_name: str, context: Dict[str, Any]) -> Any:
        """
        执行SQL脚本
        
        Args:
            script_content: 脚本内容
            script_path: 脚本路径
            table_name: 表名
            context: 执行上下文
            
        Returns:
            脚本执行结果
            
        Raises:
            Exception: 执行过程中的任何异常
        """
        # 判断是内容还是路径
        if script_content:
            # 直接执行脚本内容
            sql_to_execute = script_content
            logger.info(f"执行表 {table_name} 的内联SQL脚本")
        elif script_path:
            # 从文件读取脚本内容
            try:
                with open(script_path, 'r', encoding='utf-8') as f:
                    sql_to_execute = f.read()
                logger.info(f"执行表 {table_name} 的SQL脚本文件: {script_path}")
            except Exception as e:
                raise ValueError(f"读取脚本文件 {script_path} 失败: {str(e)}")
        else:
            raise ValueError(f"表 {table_name} 没有提供脚本内容或路径")
        
        # 替换变量
        sql_to_execute = sql_to_execute.replace('{table_name}', table_name)
        
        # 替换其他上下文变量
        for key, value in context.items():
            if isinstance(value, str):
                sql_to_execute = sql_to_execute.replace(f'{{{key}}}', value)
        
        # 执行SQL
        try:
            return self.pg_connector.execute_sql(sql_to_execute)
        except Exception as e:
            raise Exception(f"执行SQL脚本失败: {str(e)}")
    
    def _record_execution_start(self, table_name: str, execution_id: str, execution_type: str) -> None:
        """
        记录执行开始
        
        Args:
            table_name: 表名
            execution_id: 执行ID
            execution_type: 执行类型
        """
        try:
            self.pg_connector.record_execution_start(table_name, execution_id, execution_type)
            logger.debug(f"记录表 {table_name} 的执行开始，ID: {execution_id}, 类型: {execution_type}")
        except Exception as e:
            logger.warning(f"记录表 {table_name} 的执行开始失败: {str(e)}")
    
    def _record_execution_complete(self, table_name: str, execution_id: str, status: str, error_message: str = None) -> None:
        """
        记录执行完成
        
        Args:
            table_name: 表名
            execution_id: 执行ID
            status: 执行状态
            error_message: 错误信息
        """
        try:
            self.pg_connector.record_execution_complete(table_name, execution_id, status, error_message)
            if status == 'success':
                logger.debug(f"记录表 {table_name} 的执行成功，ID: {execution_id}")
            else:
                logger.debug(f"记录表 {table_name} 的执行失败，ID: {execution_id}, 错误: {error_message}")
        except Exception as e:
            logger.warning(f"记录表 {table_name} 的执行完成失败: {str(e)}")
    
    def _handle_failure(self, table_name: str, context: Dict[str, Any], error: str) -> Dict[str, Any]:
        """
        处理表处理失败情况
        
        Args:
            table_name: 表名
            context: 执行上下文
            error: 错误信息
            
        Returns:
            重试信息字典
        """
        execution_id = context.get('run_id', '')
        
        try:
            # 获取当前重试次数
            retry_count = self.pg_connector.get_retry_count(table_name, execution_id)
            
            # 获取最大重试次数
            max_retry = self.pg_connector.get_table_max_retry(table_name)
            
            # 判断是否应该重试
            if retry_count < max_retry:
                # 更新重试次数
                new_retry_count = retry_count + 1
                self.pg_connector.update_retry_count(table_name, execution_id, new_retry_count)
                
                logger.info(f"表 {table_name} 处理失败，将进行第 {new_retry_count}/{max_retry} 次重试")
                
                return {
                    'should_retry': True,
                    'retry_count': new_retry_count,
                    'max_retry': max_retry,
                    'message': f"将进行第 {new_retry_count}/{max_retry} 次重试"
                }
            else:
                logger.warning(f"表 {table_name} 处理失败，已达到最大重试次数 {max_retry}，不再重试")
                
                return {
                    'should_retry': False,
                    'retry_count': retry_count,
                    'max_retry': max_retry,
                    'message': f"已达到最大重试次数 {max_retry}，不再重试"
                }
        except Exception as e:
            logger.error(f"处理表 {table_name} 的失败情况时出错: {str(e)}")
            return {
                'should_retry': False,
                'error': str(e),
                'message': "处理失败情况时出错"
            } 