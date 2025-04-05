"""
数据库连接器模块，提供PostgreSQL和Neo4j数据库的连接和查询接口
"""
import os
import logging
import psycopg2
import psycopg2.pool
from neo4j import GraphDatabase
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Union

# 尝试导入配置模块
try:
    from ..config import PG_CONN_STRING, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
    HAS_CONFIG = True
except ImportError:
    HAS_CONFIG = False

# 设置日志
logger = logging.getLogger(__name__)


class PostgreSQLConnector:
    """PostgreSQL数据库连接器"""
    
    def __init__(self, conn_string: str = None):
        """
        初始化PostgreSQL连接器
        
        Args:
            conn_string: 数据库连接字符串，如果为None则从配置或环境变量获取
        """
        if conn_string is None:
            # 首先尝试从配置模块获取
            if HAS_CONFIG:
                conn_string = PG_CONN_STRING
            else:
                # 从环境变量获取连接信息
                conn_string = os.getenv('PG_CONN_STRING', 'postgresql://postgres:postgres@localhost:5432/dataops')
        
        self.conn_string = conn_string
        # 创建连接池
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=conn_string
        )
        logger.info(f"初始化PostgreSQL连接池: {conn_string.split('@')[1] if '@' in conn_string else conn_string}")
    
    def close(self):
        """关闭连接池"""
        if self.pool:
            self.pool.closeall()
            logger.info("PostgreSQL连接池已关闭")
    
    def get_connection(self):
        """获取数据库连接"""
        return self.pool.getconn()
    
    def release_connection(self, conn):
        """释放数据库连接"""
        self.pool.putconn(conn)
    
    def execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        """
        执行查询并返回结果
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            查询结果列表
        """
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                return cur.fetchall()
        except Exception as e:
            logger.error(f"执行查询失败: {str(e)}，SQL: {query}")
            raise
        finally:
            if conn:
                self.release_connection(conn)
    
    def execute_update(self, query: str, params: tuple = None) -> int:
        """
        执行更新操作并返回影响行数
        
        Args:
            query: SQL更新语句
            params: 查询参数
            
        Returns:
            影响的行数
        """
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                conn.commit()
                return cur.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"执行更新失败: {str(e)}，SQL: {query}")
            raise
        finally:
            if conn:
                self.release_connection(conn)
    
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """
        批量执行更新操作并返回影响行数
        
        Args:
            query: SQL更新语句
            params_list: 参数列表
            
        Returns:
            影响的行数
        """
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.executemany(query, params_list)
                conn.commit()
                return cur.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"批量执行更新失败: {str(e)}，SQL: {query}")
            raise
        finally:
            if conn:
                self.release_connection(conn)
    
    def execute_sql(self, sql_content: str) -> Union[List[tuple], int]:
        """
        执行SQL脚本并返回结果
        
        Args:
            sql_content: SQL脚本内容
            
        Returns:
            如果是SELECT语句，返回查询结果；否则返回影响行数
        """
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(sql_content)
                conn.commit()
                # 如果是SELECT语句，返回结果
                if sql_content.strip().upper().startswith('SELECT'):
                    return cur.fetchall()
                # 否则返回影响行数
                return cur.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"执行SQL脚本失败: {str(e)}，SQL: {sql_content[:100]}...")
            raise
        finally:
            if conn:
                self.release_connection(conn)
    
    # 表调度信息相关方法
    
    def get_scheduled_tables(self, frequency: str) -> List[str]:
        """
        获取指定频率的所有已订阅表
        
        Args:
            frequency: 调度频率，如'hourly', 'daily', 'weekly', 'monthly'
            
        Returns:
            表名列表
        """
        query = """
        SELECT table_name FROM table_schedule 
        WHERE schedule_frequency = %s AND is_enabled = TRUE
        """
        results = self.execute_query(query, (frequency,))
        return [r[0] for r in results]
    
    def get_table_schedule(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        获取表的调度信息
        
        Args:
            table_name: 表名
            
        Returns:
            调度信息字典，如果表不存在则返回None
        """
        query = """
        SELECT schedule_frequency, schedule_time, schedule_day, execution_mode,
               is_enabled, priority, max_retry, retry_delay_minutes
        FROM table_schedule
        WHERE table_name = %s
        """
        result = self.execute_query(query, (table_name,))
        if not result:
            return None
        
        row = result[0]
        return {
            'schedule_frequency': row[0],
            'schedule_time': row[1],
            'schedule_day': row[2],
            'execution_mode': row[3],
            'is_enabled': row[4],
            'priority': row[5],
            'max_retry': row[6],
            'retry_delay_minutes': row[7]
        }
    
    def get_table_priority(self, table_name: str) -> int:
        """
        获取表的优先级
        
        Args:
            table_name: 表名
            
        Returns:
            表的优先级，默认为5
        """
        query = "SELECT priority FROM table_schedule WHERE table_name = %s"
        result = self.execute_query(query, (table_name,))
        return result[0][0] if result else 5
    
    def get_table_max_retry(self, table_name: str) -> int:
        """
        获取表的最大重试次数
        
        Args:
            table_name: 表名
            
        Returns:
            最大重试次数，默认为5
        """
        query = "SELECT max_retry FROM table_schedule WHERE table_name = %s"
        result = self.execute_query(query, (table_name,))
        return result[0][0] if result else 5
    
    def get_table_retry_delay(self, table_name: str) -> int:
        """
        获取表的重试延迟时间
        
        Args:
            table_name: 表名
            
        Returns:
            重试延迟时间（分钟），默认为1
        """
        query = "SELECT retry_delay_minutes FROM table_schedule WHERE table_name = %s"
        result = self.execute_query(query, (table_name,))
        return result[0][0] if result else 1
    
    def get_last_schedule_update(self) -> Optional[datetime]:
        """
        获取调度表的最后更新时间
        
        Returns:
            最后更新时间，如果没有更新则返回None
        """
        query = "SELECT MAX(updated_at) FROM table_schedule"
        result = self.execute_query(query)
        return result[0][0] if result and result[0][0] else None
    
    # 处理脚本相关方法
    
    def get_processing_script(self, table_name: str) -> Dict[str, Any]:
        """
        获取表的处理脚本
        
        Args:
            table_name: 表名
            
        Returns:
            脚本信息字典，包含script_type, script_content, script_path
            
        Raises:
            ValueError: 如果找不到表的处理脚本
        """
        query = """
        SELECT script_type, script_content, script_path
        FROM processing_script
        WHERE table_name = %s AND is_active = TRUE
        """
        result = self.execute_query(query, (table_name,))
        if not result:
            raise ValueError(f"找不到表 {table_name} 的处理脚本")
        
        row = result[0]
        return {
            'script_type': row[0],
            'script_content': row[1],
            'script_path': row[2]
        }
    
    # 执行记录相关方法
    
    def record_execution_start(self, table_name: str, execution_id: str, execution_type: str) -> None:
        """
        记录执行开始
        
        Args:
            table_name: 表名
            execution_id: 执行ID
            execution_type: 执行类型，如'scheduled', 'manual', 'dependency'
        """
        query = """
        INSERT INTO execution_history 
            (table_name, execution_id, execution_type, start_time, status)
        VALUES (%s, %s, %s, %s, 'running')
        """
        self.execute_update(query, (
            table_name, execution_id, execution_type, datetime.now()
        ))
    
    def record_execution_complete(self, table_name: str, execution_id: str, 
                                 status: str, error_message: str = None) -> None:
        """
        记录执行完成
        
        Args:
            table_name: 表名
            execution_id: 执行ID
            status: 执行状态，如'success', 'failed'
            error_message: 错误信息
        """
        query = """
        UPDATE execution_history
        SET end_time = %s, status = %s, error_message = %s
        WHERE table_name = %s AND execution_id = %s
        """
        self.execute_update(query, (
            datetime.now(), status, error_message, table_name, execution_id
        ))
    
    def get_retry_count(self, table_name: str, execution_id: str) -> int:
        """
        获取当前重试次数
        
        Args:
            table_name: 表名
            execution_id: 执行ID
            
        Returns:
            当前重试次数
        """
        query = """
        SELECT retry_count FROM execution_history
        WHERE table_name = %s AND execution_id = %s
        """
        result = self.execute_query(query, (table_name, execution_id))
        return result[0][0] if result else 0
    
    def update_retry_count(self, table_name: str, execution_id: str, retry_count: int) -> None:
        """
        更新重试次数
        
        Args:
            table_name: 表名
            execution_id: 执行ID
            retry_count: 新的重试次数
        """
        query = """
        UPDATE execution_history
        SET retry_count = %s
        WHERE table_name = %s AND execution_id = %s
        """
        self.execute_update(query, (retry_count, table_name, execution_id))
    
    # 配置相关方法
    
    def get_system_config(self, key: str, default_value: str = None) -> Optional[str]:
        """
        获取系统配置
        
        Args:
            key: 配置键
            default_value: 默认值
            
        Returns:
            配置值，如果不存在则返回默认值
        """
        query = "SELECT config_value FROM system_config WHERE config_key = %s"
        result = self.execute_query(query, (key,))
        return result[0][0] if result else default_value
    
    def update_system_config(self, key: str, value: str) -> None:
        """
        更新系统配置
        
        Args:
            key: 配置键
            value: 配置值
        """
        query = """
        INSERT INTO system_config (config_key, config_value, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (config_key) DO UPDATE
        SET config_value = %s, updated_at = %s
        """
        now = datetime.now()
        self.execute_update(query, (key, value, now, value, now))


class Neo4jConnector:
    """Neo4j数据库连接器"""
    
    def __init__(self, uri: str = None, user: str = None, password: str = None):
        """
        初始化Neo4j连接器
        
        Args:
            uri: Neo4j服务器URI，如果为None则从配置或环境变量获取
            user: 用户名，如果为None则从配置或环境变量获取
            password: 密码，如果为None则从配置或环境变量获取
        """
        # 首先尝试从配置模块获取
        if uri is None and HAS_CONFIG:
            uri = NEO4J_URI
        if user is None and HAS_CONFIG:
            user = NEO4J_USER
        if password is None and HAS_CONFIG:
            password = NEO4J_PASSWORD
            
        # 如果仍为None，从环境变量获取
        self.uri = uri or os.getenv('NEO4J_URI', 'bolt://localhost:7687')
        self.user = user or os.getenv('NEO4J_USER', 'neo4j')
        self.password = password or os.getenv('NEO4J_PASSWORD', 'neo4j')
        
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        logger.info(f"初始化Neo4j连接: {self.uri}")
    
    def close(self):
        """关闭连接"""
        self.driver.close()
    
    def query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        执行查询并返回结果
        
        Args:
            query: Cypher查询语句
            params: 查询参数
            
        Returns:
            查询结果列表
        """
        try:
            with self.driver.session() as session:
                result = session.run(query, params or {})
                return result.data()
        except Exception as e:
            logger.error(f"Neo4j查询失败: {str(e)}，查询: {query}")
            raise
    
    def get_table_dependencies(self, table_name: str) -> List[str]:
        """
        获取表的直接依赖
        
        Args:
            table_name: 表名
            
        Returns:
            依赖表名列表
        """
        query = """
        MATCH (t1:Table {name: $table_name})-[:DERIVED_FROM]->(t2:Table)
        RETURN t2.name AS dependency
        """
        result = self.query(query, {'table_name': table_name})
        return [r['dependency'] for r in result]
    
    def get_table_dependents(self, table_name: str) -> List[str]:
        """
        获取依赖此表的表
        
        Args:
            table_name: 表名
            
        Returns:
            依赖于此表的表名列表
        """
        query = """
        MATCH (t1:Table)-[:DERIVED_FROM]->(t2:Table {name: $table_name})
        RETURN t1.name AS dependent
        """
        result = self.query(query, {'table_name': table_name})
        return [r['dependent'] for r in result]
    
    def get_all_dependencies(self) -> List[Dict[str, str]]:
        """
        获取所有表间依赖关系
        
        Returns:
            依赖关系列表，每项包含source和target字段
        """
        query = """
        MATCH (t1:Table)-[r:DERIVED_FROM]->(t2:Table)
        RETURN t1.name AS target, t2.name AS source
        """
        return self.query(query)
    
    def get_last_update_time(self) -> Optional[datetime]:
        """
        获取Neo4j表的最后更新时间
        
        Returns:
            最后更新时间，如果没有更新则返回None
        """
        query = """
        MATCH (t:Table)
        RETURN max(t.updated_at) AS last_update
        """
        result = self.query(query)
        return result[0]['last_update'] if result and result[0]['last_update'] else None 