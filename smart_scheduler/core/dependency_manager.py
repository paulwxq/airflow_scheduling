"""
依赖图管理器模块，负责构建和维护表间依赖关系
"""
import logging
import threading
import time
import networkx as nx
from datetime import datetime
from typing import List, Dict, Any, Optional, Set, Tuple

from ..db.connectors import PostgreSQLConnector, Neo4jConnector

# 设置日志
logger = logging.getLogger(__name__)


class DependencyCache:
    """依赖关系缓存"""
    
    def __init__(self, max_size: int = 1000, ttl: int = 600):
        """
        初始化缓存
        
        Args:
            max_size: 最大缓存条目数
            ttl: 缓存生存时间(秒)
        """
        self.cache = {}  # 缓存字典
        self.max_size = max_size  # 最大缓存条目数
        self.ttl = ttl  # 缓存生存时间(秒)
        self.lock = threading.RLock()  # 缓存锁
    
    def get(self, key: str) -> Optional[Any]:
        """
        获取缓存的值
        
        Args:
            key: 缓存键
            
        Returns:
            缓存值，如果过期或不存在则返回None
        """
        with self.lock:
            if key not in self.cache:
                return None
            
            entry = self.cache[key]
            if time.time() - entry['timestamp'] > self.ttl:
                del self.cache[key]
                return None
            
            return entry['value']
    
    def set(self, key: str, value: Any) -> None:
        """
        设置缓存值
        
        Args:
            key: 缓存键
            value: 缓存值
        """
        with self.lock:
            # 如果缓存已满，移除最旧的条目
            if len(self.cache) >= self.max_size:
                oldest_key = min(self.cache, key=lambda k: self.cache[k]['timestamp'])
                del self.cache[oldest_key]
            
            self.cache[key] = {
                'value': value,
                'timestamp': time.time()
            }
    
    def invalidate(self, key: str = None) -> None:
        """
        使缓存失效
        
        Args:
            key: 缓存键，如果为None则清空整个缓存
        """
        with self.lock:
            if key is None:
                self.cache.clear()
            elif key in self.cache:
                del self.cache[key]


class DependencyManager:
    """依赖图管理器，采用单例模式"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DependencyManager, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self, neo4j_connector: Neo4jConnector = None, pg_connector: PostgreSQLConnector = None,
                refresh_interval: int = 600):
        """
        初始化依赖图管理器
        
        Args:
            neo4j_connector: Neo4j连接器
            pg_connector: PostgreSQL连接器
            refresh_interval: 刷新间隔(秒)
        """
        # 避免重复初始化单例
        if self._initialized:
            return
            
        self.neo4j_connector = neo4j_connector or Neo4jConnector()
        self.pg_connector = pg_connector or PostgreSQLConnector()
        self.refresh_interval = refresh_interval
        
        self.graph = nx.DiGraph()  # 依赖图
        self.cache = DependencyCache()  # 缓存
        self.last_refresh_time = 0  # 上次刷新时间
        self.neo4j_last_update = None  # Neo4j上次更新时间
        self.pg_last_update = None  # PostgreSQL上次更新时间
        self.refresh_lock = threading.RLock()  # 刷新锁，保证并发安全
        
        # 初始化依赖图
        self._refresh_dependency_graph()
        self.last_refresh_time = time.time()
        
        self._initialized = True
        logger.info("依赖图管理器初始化完成")
    
    def get_table_dependencies(self, table_name: str) -> List[str]:
        """
        获取表的所有依赖（直接和间接依赖）
        
        Args:
            table_name: 表名
            
        Returns:
            依赖表名列表
        """
        # 先查询缓存
        cached_result = self.cache.get(f"dep_{table_name}")
        if cached_result is not None:
            return cached_result
        
        # 确保依赖图是最新的
        self._ensure_graph_refreshed()
        
        # 如果表不在图中，返回空列表
        if table_name not in self.graph:
            return []
        
        # 使用反向图进行深度优先搜索，找出所有依赖
        dependencies = []
        
        # 创建反向图
        reverse_graph = self.graph.reverse()
        
        # 从目标表开始，找出所有上游依赖
        for source, target in nx.dfs_edges(reverse_graph, table_name):
            dependencies.append(target)
        
        # 更新缓存
        self.cache.set(f"dep_{table_name}", dependencies)
        
        return dependencies
    
    def get_table_direct_dependencies(self, table_name: str) -> List[str]:
        """
        获取表的直接依赖
        
        Args:
            table_name: 表名
            
        Returns:
            直接依赖表名列表
        """
        # 先查询缓存
        cached_result = self.cache.get(f"direct_dep_{table_name}")
        if cached_result is not None:
            return cached_result
        
        # 确保依赖图是最新的
        self._ensure_graph_refreshed()
        
        # 如果表不在图中，返回空列表
        if table_name not in self.graph:
            return []
        
        # 获取直接依赖
        dependencies = list(self.graph.predecessors(table_name))
        
        # 更新缓存
        self.cache.set(f"direct_dep_{table_name}", dependencies)
        
        return dependencies
    
    def get_table_dependents(self, table_name: str) -> List[str]:
        """
        获取依赖于此表的所有表
        
        Args:
            table_name: 表名
            
        Returns:
            依赖表名列表
        """
        # 先查询缓存
        cached_result = self.cache.get(f"deps_{table_name}")
        if cached_result is not None:
            return cached_result
        
        # 确保依赖图是最新的
        self._ensure_graph_refreshed()
        
        # 如果表不在图中，返回空列表
        if table_name not in self.graph:
            return []
        
        # 使用有向图进行深度优先搜索，找出所有依赖于此表的表
        dependents = []
        for source, target in nx.dfs_edges(self.graph, table_name):
            dependents.append(target)
        
        # 更新缓存
        self.cache.set(f"deps_{table_name}", dependents)
        
        return dependents
    
    def get_all_tables(self) -> List[str]:
        """
        获取所有表名
        
        Returns:
            所有表名列表
        """
        # 确保依赖图是最新的
        self._ensure_graph_refreshed()
        
        return list(self.graph.nodes())
    
    def _ensure_graph_refreshed(self) -> None:
        """确保依赖图是最新的"""
        current_time = time.time()
        if current_time - self.last_refresh_time > self.refresh_interval:
            self.refresh_dependency_graph()
    
    def refresh_dependency_graph(self) -> bool:
        """
        强制刷新依赖图
        
        Returns:
            是否成功刷新
        """
        with self.refresh_lock:
            try:
                self._refresh_dependency_graph()
                self.last_refresh_time = time.time()
                logger.info(f"依赖图已刷新，当前时间: {datetime.now()}")
                return True
            except Exception as e:
                logger.error(f"刷新依赖图失败: {str(e)}")
                return False
    
    def _refresh_dependency_graph(self) -> None:
        """从Neo4j刷新依赖图"""
        try:
            # 获取所有表间依赖关系
            dependencies = self.neo4j_connector.get_all_dependencies()
            
            # 构建新的依赖图
            new_graph = nx.DiGraph()
            
            # 添加所有依赖边
            for relation in dependencies:
                source = relation['source']
                target = relation['target']
                new_graph.add_edge(source, target)
            
            # 更新依赖图
            self.graph = new_graph
            
            # 清空缓存
            self.cache.invalidate()
            
            logger.info(f"依赖图刷新完成，当前节点数: {len(new_graph.nodes())}, 边数: {len(new_graph.edges())}")
            
        except Exception as e:
            logger.error(f"刷新依赖图失败: {str(e)}")
            raise
    
    def check_updates_and_refresh(self) -> bool:
        """
        检查Neo4j和PostgreSQL是否有更新，如有则刷新依赖图
        
        Returns:
            是否执行了刷新
        """
        try:
            needs_update = False
            
            # 检查Neo4j更新
            neo4j_last_update = self.neo4j_connector.get_last_update_time()
            if neo4j_last_update and neo4j_last_update != self.neo4j_last_update:
                logger.info(f"检测到Neo4j更新: {neo4j_last_update}")
                needs_update = True
                self.neo4j_last_update = neo4j_last_update
            
            # 检查PostgreSQL更新
            pg_last_update = self.pg_connector.get_last_schedule_update()
            if pg_last_update and pg_last_update != self.pg_last_update:
                logger.info(f"检测到PostgreSQL调度更新: {pg_last_update}")
                needs_update = True
                self.pg_last_update = pg_last_update
            
            # 如果需要更新，刷新依赖图
            if needs_update:
                return self.refresh_dependency_graph()
            
            return False
            
        except Exception as e:
            logger.error(f"检查更新失败: {str(e)}")
            return False
    
    def has_cycles(self) -> List[List[str]]:
        """
        检查依赖图是否有循环依赖
        
        Returns:
            循环依赖的路径列表，如果没有循环依赖则返回空列表
        """
        # 确保依赖图是最新的
        self._ensure_graph_refreshed()
        
        try:
            cycles = list(nx.simple_cycles(self.graph))
            return cycles
        except Exception as e:
            logger.error(f"检查循环依赖失败: {str(e)}")
            return []
    
    def get_execution_order(self, tables: List[str], include_dependencies: bool = True) -> List[str]:
        """
        获取表的执行顺序
        
        Args:
            tables: 表名列表
            include_dependencies: 是否包含依赖
            
        Returns:
            表的执行顺序
            
        Raises:
            ValueError: 如果存在循环依赖，无法生成执行顺序
        """
        # 确保依赖图是最新的
        self._ensure_graph_refreshed()
        
        # 获取所有相关表
        all_tables = set(tables)
        if include_dependencies:
            for table in tables:
                dependencies = self.get_table_dependencies(table)
                all_tables.update(dependencies)
        
        # 构建子图
        subgraph = self.graph.subgraph(all_tables)
        
        # 检查是否有循环依赖
        cycles = list(nx.simple_cycles(subgraph))
        if cycles:
            raise ValueError(f"检测到循环依赖，无法生成执行顺序: {cycles}")
        
        # 拓扑排序
        try:
            execution_order = list(nx.topological_sort(subgraph))
            return execution_order
        except Exception as e:
            logger.error(f"生成执行顺序失败: {str(e)}")
            raise ValueError(f"生成执行顺序失败: {str(e)}")
    
    def get_common_dependencies(self, tables: List[str]) -> Dict[str, Set[str]]:
        """
        获取表间的共同依赖
        
        Args:
            tables: 表名列表
            
        Returns:
            表间共同依赖的字典，键为表名，值为依赖它的表集合
        """
        # 确保依赖图是最新的
        self._ensure_graph_refreshed()
        
        common_deps = {}
        
        # 获取所有表的依赖
        table_deps = {}
        for table in tables:
            deps = set(self.get_table_dependencies(table))
            table_deps[table] = deps
            
            # 更新共同依赖字典
            for dep in deps:
                if dep not in common_deps:
                    common_deps[dep] = set()
                common_deps[dep].add(table)
        
        # 只保留被多个表依赖的共同依赖
        common_deps = {dep: tables for dep, tables in common_deps.items() if len(tables) > 1}
        
        return common_deps 