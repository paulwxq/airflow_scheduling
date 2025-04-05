"""
执行计划生成器模块，负责生成和优化表的执行计划
"""
import logging
import networkx as nx
from typing import List, Dict, Any, Optional, Set, Tuple
from collections import defaultdict

from .dependency_manager import DependencyManager
from ..db.connectors import PostgreSQLConnector

# 设置日志
logger = logging.getLogger(__name__)


class ExecutionPlanGenerator:
    """执行计划生成器"""
    
    def __init__(self, dependency_manager: DependencyManager = None, pg_connector: PostgreSQLConnector = None):
        """
        初始化执行计划生成器
        
        Args:
            dependency_manager: 依赖图管理器
            pg_connector: PostgreSQL连接器
        """
        self.dependency_manager = dependency_manager or DependencyManager()
        self.pg_connector = pg_connector or PostgreSQLConnector()
        logger.info("执行计划生成器初始化完成")
    
    def generate_plan(self, tables: List[str], include_dependencies: bool = True) -> List[str]:
        """
        生成执行计划
        
        Args:
            tables: 表名列表
            include_dependencies: 是否包含依赖
            
        Returns:
            执行计划（表的执行顺序列表）
            
        Raises:
            ValueError: 如果存在循环依赖，无法生成执行计划
        """
        if not tables:
            return []
        
        try:
            # 直接使用依赖图管理器生成执行顺序
            execution_order = self.dependency_manager.get_execution_order(tables, include_dependencies)
            
            logger.info(f"生成执行计划: {len(execution_order)} 个表")
            return execution_order
            
        except Exception as e:
            logger.error(f"生成执行计划失败: {str(e)}")
            raise ValueError(f"生成执行计划失败: {str(e)}")
    
    def optimize_plan(self, plan: List[str], max_parallel: int = None) -> List[List[str]]:
        """
        优化执行计划，将表分成多个批次执行，提高并行度
        
        Args:
            plan: 执行计划（表的执行顺序列表）
            max_parallel: 最大并行度，如果为None则从配置中获取
            
        Returns:
            优化后的执行计划（分批次的表列表）
            
        Raises:
            ValueError: 如果无法优化执行计划
        """
        if not plan:
            return []
        
        if max_parallel is None:
            max_parallel = int(self.pg_connector.get_system_config('max_concurrency', '4'))
        
        try:
            # 1. 首先合并重复作业
            merged_plan = self._merge_duplicate_tasks(plan)
            logger.info(f"合并重复作业后的计划包含 {len(merged_plan)} 个表，原计划有 {len(plan)} 个表")
            
            # 2. 应用工作流优化（层次分组）
            # 构建依赖子图
            G = nx.DiGraph()
            
            # 添加所有节点
            for table in merged_plan:
                G.add_node(table)
            
            # 添加所有边
            for table in merged_plan:
                dependencies = self.dependency_manager.get_table_direct_dependencies(table)
                for dep in dependencies:
                    if dep in merged_plan:
                        G.add_edge(dep, table)
            
            # 按层次分组
            optimized_plan = []
            remaining = set(merged_plan)
            
            while remaining:
                # 找出没有未处理依赖的表
                current_layer = []
                for table in list(remaining):
                    if all(pred not in remaining for pred in G.predecessors(table)):
                        current_layer.append(table)
                
                if not current_layer:
                    # 可能存在循环依赖
                    raise ValueError("无法继续优化执行计划，可能存在循环依赖")
                
                # 应用资源亲和性优化：尝试将使用相同资源的表分到不同批次
                current_layer = self._optimize_resource_affinity(current_layer)
                
                # 按优先级排序
                current_layer.sort(key=lambda t: self._get_table_priority(t))
                
                # 分批执行以控制并行度
                for i in range(0, len(current_layer), max_parallel):
                    batch = current_layer[i:i+max_parallel]
                    optimized_plan.append(batch)
                
                # 移除已处理的表
                remaining -= set(current_layer)
            
            logger.info(f"优化执行计划: {len(merged_plan)} 个表分成 {len(optimized_plan)} 个批次")
            return optimized_plan
            
        except Exception as e:
            logger.error(f"优化执行计划失败: {str(e)}")
            raise ValueError(f"优化执行计划失败: {str(e)}")
    
    def _merge_duplicate_tasks(self, plan: List[str]) -> List[str]:
        """
        智能合并重复的作业，减少执行次数
        
        Args:
            plan: 执行计划
            
        Returns:
            合并后的执行计划
        """
        if not plan:
            return []
        
        try:
            # 1. 构建表的属性字典
            table_props = {}
            for table in plan:
                try:
                    # 获取表的配置信息
                    props = self.pg_connector.get_table_properties(table)
                    table_props[table] = props
                except:
                    # 如果获取失败，使用默认属性
                    table_props[table] = {'source': None, 'update_strategy': 'full'}
            
            # 2. 识别相同来源和更新策略的表
            source_groups = defaultdict(list)
            for table, props in table_props.items():
                # 使用源系统+更新策略作为分组key
                source_key = f"{props.get('source')}:{props.get('update_strategy')}"
                source_groups[source_key].append(table)
            
            # 3. 分析重复作业
            merged_plan = []
            processed_tables = set()
            
            # 首先处理所有具有相同来源的表组
            for source_key, tables in source_groups.items():
                if len(tables) <= 1 or None in source_key:
                    # 只有一个表或没有明确来源，不需要合并
                    continue
                
                # 对于每个组，按依赖关系排序
                group_order = self.dependency_manager.get_execution_order(tables, include_dependencies=False)
                
                # 添加到结果中，并标记为已处理
                if group_order:
                    # 仅保留组中第一个表，其他表在这个表处理时会一并处理
                    first_table = group_order[0]
                    merged_plan.append(first_table)
                    processed_tables.update(group_order)
                    
                    # 记录合并信息
                    self._record_task_merge(first_table, group_order[1:])
                    
                    logger.info(f"合并作业: 将表 {group_order[1:]} 合并到 {first_table}")
            
            # 添加未处理的表
            for table in plan:
                if table not in processed_tables:
                    merged_plan.append(table)
            
            # 4. 保持原计划的执行顺序
            # 按原计划顺序对merged_plan进行排序
            plan_order = {table: idx for idx, table in enumerate(plan)}
            merged_plan.sort(key=lambda t: plan_order.get(t, float('inf')))
            
            return merged_plan
            
        except Exception as e:
            logger.warning(f"合并重复作业失败: {str(e)}，使用原计划")
            return plan
    
    def _record_task_merge(self, main_table: str, merged_tables: List[str]) -> None:
        """
        记录任务合并信息
        
        Args:
            main_table: 主表
            merged_tables: 被合并的表
        """
        try:
            # 记录到数据库
            for merged_table in merged_tables:
                query = """
                INSERT INTO task_merge_history 
                (main_table, merged_table, merge_time, merge_reason)
                VALUES (%s, %s, NOW(), %s)
                """
                self.pg_connector.execute_query(
                    query, 
                    (main_table, merged_table, "相同数据源和更新策略")
                )
        except Exception as e:
            logger.warning(f"记录任务合并信息失败: {str(e)}")
    
    def _optimize_resource_affinity(self, tables: List[str]) -> List[str]:
        """
        优化资源亲和性，尽量避免同时执行使用相同资源的表
        
        Args:
            tables: 表列表
            
        Returns:
            优化后的表列表
        """
        if len(tables) <= 1:
            return tables
            
        try:
            # 获取表的资源信息
            resource_map = {}
            for table in tables:
                try:
                    resource = self.pg_connector.get_table_resource(table)
                    resource_map[table] = resource
                except:
                    resource_map[table] = None
            
            # 按资源分组
            resource_groups = defaultdict(list)
            for table, resource in resource_map.items():
                resource_groups[resource].append(table)
            
            # 重新排序表，使相同资源的表尽量分散
            result = []
            
            # 首先按资源组大小排序，优先处理资源冲突最多的组
            sorted_groups = sorted(resource_groups.items(), key=lambda x: len(x[1]), reverse=True)
            
            # 轮流从每个组取表
            while any(group for _, group in sorted_groups):
                for resource, group in sorted_groups:
                    if group:
                        result.append(group.pop(0))
            
            return result
            
        except Exception as e:
            logger.warning(f"优化资源亲和性失败: {str(e)}，使用原表列表")
            return tables
    
    def _get_table_priority(self, table_name: str) -> int:
        """
        获取表的优先级
        
        Args:
            table_name: 表名
            
        Returns:
            表的优先级，值越小优先级越高
        """
        try:
            priority = self.pg_connector.get_table_priority(table_name)
            return priority
        except Exception as e:
            logger.warning(f"获取表 {table_name} 的优先级失败: {str(e)}，使用默认优先级5")
            return 5
    
    def merge_common_dependencies(self, tables: List[str]) -> Dict[str, List[str]]:
        """
        合并共同依赖，找出表之间的共同依赖关系
        
        Args:
            tables: 表名列表
            
        Returns:
            共同依赖表及其被依赖者的字典
        """
        # 获取共同依赖
        common_deps = self.dependency_manager.get_common_dependencies(tables)
        
        # 转换为字典格式
        result = {}
        for dep, dependents in common_deps.items():
            result[dep] = list(dependents)
        
        logger.info(f"找到 {len(result)} 个共同依赖")
        return result
    
    def generate_optimized_plan(self, tables: List[str], max_parallel: int = None) -> List[List[str]]:
        """
        生成并优化执行计划，一步到位
        
        Args:
            tables: 表名列表
            max_parallel: 最大并行度，如果为None则从配置中获取
            
        Returns:
            优化后的执行计划（分批次的表列表）
        """
        # 先生成执行计划
        plan = self.generate_plan(tables)
        
        # 然后优化计划
        return self.optimize_plan(plan, max_parallel)
    
    def estimate_execution_time(self, plan: List[List[str]]) -> float:
        """
        估计执行计划的总执行时间
        
        Args:
            plan: 优化后的执行计划
            
        Returns:
            估计的执行时间（秒）
        """
        total_time = 0
        
        # 对于每个批次，取最长执行时间的表作为批次执行时间
        for batch in plan:
            batch_time = 0
            for table in batch:
                # 查询表的历史平均执行时间
                avg_time = self._get_table_avg_execution_time(table)
                batch_time = max(batch_time, avg_time)
            
            total_time += batch_time
        
        return total_time
    
    def _get_table_avg_execution_time(self, table_name: str) -> float:
        """
        获取表的平均执行时间
        
        Args:
            table_name: 表名
            
        Returns:
            平均执行时间（秒）
        """
        try:
            query = """
            SELECT AVG(EXTRACT(EPOCH FROM (end_time - start_time)))
            FROM execution_history
            WHERE table_name = %s AND status = 'success'
            AND start_time > NOW() - INTERVAL '7 days'
            """
            result = self.pg_connector.execute_query(query, (table_name,))
            
            if result and result[0][0]:
                return float(result[0][0])
            return 60  # 默认1分钟
            
        except Exception as e:
            logger.warning(f"获取表 {table_name} 的平均执行时间失败: {str(e)}，使用默认值60秒")
            return 60  # 默认1分钟 