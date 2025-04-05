"""
监控服务模块，负责系统监控、性能统计和告警
"""
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union

from .db.connectors import PostgreSQLConnector

# 设置日志
logger = logging.getLogger(__name__)


class MonitoringService:
    """监控服务，负责系统监控和性能统计"""
    
    def __init__(self, pg_connector: PostgreSQLConnector = None):
        """
        初始化监控服务
        
        Args:
            pg_connector: PostgreSQL连接器
        """
        self.pg_connector = pg_connector or PostgreSQLConnector()
        logger.info("监控服务初始化完成")
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        获取系统状态
        
        Returns:
            系统状态信息
        """
        try:
            # 获取最近执行历史
            recent_executions = self.pg_connector.execute_query("""
                SELECT status, COUNT(*) 
                FROM execution_history 
                WHERE start_time > NOW() - INTERVAL '24 hours'
                GROUP BY status
            """)
            
            # 获取当前表数量
            table_counts = self.pg_connector.execute_query("""
                SELECT schedule_frequency, COUNT(*) 
                FROM table_schedule
                WHERE is_enabled = TRUE
                GROUP BY schedule_frequency
            """)
            
            # 获取最近失败任务
            recent_failures = self.pg_connector.execute_query("""
                SELECT table_name, execution_id, start_time, error_message
                FROM execution_history
                WHERE status = 'failed' AND start_time > NOW() - INTERVAL '24 hours'
                ORDER BY start_time DESC
                LIMIT 10
            """)
            
            # 获取系统配置
            system_configs = self.pg_connector.execute_query("""
                SELECT config_key, config_value
                FROM system_config
            """)
            
            return {
                'recent_executions': {row[0]: row[1] for row in recent_executions},
                'table_counts': {row[0]: row[1] for row in table_counts},
                'recent_failures': [
                    {
                        'table_name': row[0],
                        'execution_id': row[1],
                        'start_time': row[2].isoformat() if row[2] else None,
                        'error_message': row[3]
                    }
                    for row in recent_failures
                ],
                'system_configs': {row[0]: row[1] for row in system_configs},
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"获取系统状态失败: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        获取性能指标
        
        Returns:
            性能指标信息
        """
        try:
            # 获取平均执行时间
            avg_execution_times = self.pg_connector.execute_query("""
                SELECT table_name, 
                       AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration
                FROM execution_history
                WHERE status = 'success' AND start_time > NOW() - INTERVAL '7 days'
                GROUP BY table_name
                ORDER BY avg_duration DESC
                LIMIT 10
            """)
            
            # 获取失败率
            failure_rates = self.pg_connector.execute_query("""
                SELECT table_name,
                       COUNT(*) FILTER (WHERE status = 'failed') * 100.0 / COUNT(*) as failure_rate
                FROM execution_history
                WHERE start_time > NOW() - INTERVAL '7 days'
                GROUP BY table_name
                HAVING COUNT(*) > 5
                ORDER BY failure_rate DESC
                LIMIT 10
            """)
            
            # 获取最慢的表
            slowest_tables = self.pg_connector.execute_query("""
                SELECT table_name, EXTRACT(EPOCH FROM (end_time - start_time)) as duration,
                       execution_id, start_time
                FROM execution_history
                WHERE status = 'success' AND start_time > NOW() - INTERVAL '7 days'
                ORDER BY duration DESC
                LIMIT 5
            """)
            
            return {
                'avg_execution_times': [
                    {
                        'table_name': row[0],
                        'avg_duration_seconds': float(row[1]) if row[1] else 0
                    }
                    for row in avg_execution_times
                ],
                'failure_rates': [
                    {
                        'table_name': row[0],
                        'failure_rate_percent': float(row[1]) if row[1] else 0
                    }
                    for row in failure_rates
                ],
                'slowest_executions': [
                    {
                        'table_name': row[0],
                        'duration_seconds': float(row[1]) if row[1] else 0,
                        'execution_id': row[2],
                        'start_time': row[3].isoformat() if row[3] else None
                    }
                    for row in slowest_tables
                ],
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"获取性能指标失败: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def cleanup_old_history(self) -> Dict[str, Any]:
        """
        清理旧的历史记录
        
        Returns:
            清理结果
        """
        try:
            retention_days = int(self.pg_connector.get_system_config('history_retention_days', '180'))
            
            # 删除旧记录
            deleted_count = self.pg_connector.execute_update("""
                DELETE FROM execution_history
                WHERE end_time < NOW() - INTERVAL '%s days'
            """, (retention_days,))
            
            logger.info(f"已清理 {deleted_count} 条历史记录，保留期: {retention_days} 天")
            
            return {
                'status': 'success',
                'deleted_count': deleted_count,
                'retention_days': retention_days,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"清理历史记录失败: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def check_failed_executions(self, hours: int = 24) -> List[Dict[str, Any]]:
        """
        检查近期失败的执行
        
        Args:
            hours: 检查最近多少小时内的失败，默认24小时
            
        Returns:
            失败执行列表
        """
        try:
            failures = self.pg_connector.execute_query("""
                SELECT table_name, execution_id, execution_type, start_time, 
                       end_time, retry_count, error_message
                FROM execution_history
                WHERE status = 'failed' 
                  AND start_time > NOW() - INTERVAL '%s hours'
                ORDER BY end_time DESC
            """, (hours,))
            
            return [
                {
                    'table_name': row[0],
                    'execution_id': row[1],
                    'execution_type': row[2],
                    'start_time': row[3].isoformat() if row[3] else None,
                    'end_time': row[4].isoformat() if row[4] else None,
                    'retry_count': row[5],
                    'error_message': row[6]
                }
                for row in failures
            ]
        except Exception as e:
            logger.error(f"检查失败执行失败: {str(e)}")
            return []


class AlertService:
    """告警服务，负责发送告警和通知"""
    
    def __init__(self, pg_connector: PostgreSQLConnector = None, smtp_config: Dict[str, Any] = None):
        """
        初始化告警服务
        
        Args:
            pg_connector: PostgreSQL连接器
            smtp_config: SMTP配置
        """
        self.pg_connector = pg_connector or PostgreSQLConnector()
        self.smtp_config = smtp_config or {
            'server': self.pg_connector.get_system_config('smtp_server', 'localhost'),
            'port': int(self.pg_connector.get_system_config('smtp_port', '25')),
            'user': self.pg_connector.get_system_config('smtp_user'),
            'password': self.pg_connector.get_system_config('smtp_password'),
            'from': self.pg_connector.get_system_config('smtp_from', 'scheduler@example.com')
        }
        logger.info("告警服务初始化完成")
    
    def send_failure_alert(self, table_name: str, execution_id: str, error_message: str) -> bool:
        """
        发送失败告警
        
        Args:
            table_name: 表名
            execution_id: 执行ID
            error_message: 错误信息
            
        Returns:
            是否发送成功
        """
        try:
            # 获取表的所有者
            owner_query = """
                SELECT owner_id FROM table_schedule WHERE table_name = %s
            """
            owner_result = self.pg_connector.execute_query(owner_query, (table_name,))
            
            if not owner_result:
                logger.warning(f"无法发送告警，找不到表 {table_name} 的所有者")
                return False
            
            owner_id = owner_result[0][0]
            
            # 获取所有者的邮箱
            # 在实际项目中，这可能需要查询用户表或配置
            owner_email = f"{owner_id}@example.com"
            
            # 发送邮件
            subject = f"表 {table_name} 处理失败"
            body = f"""
            表 {table_name} 在执行ID {execution_id} 中处理失败。
            
            错误信息:
            {error_message}
            
            请登录系统查看详细信息。
            """
            
            return self._send_email(owner_email, subject, body)
            
        except Exception as e:
            logger.error(f"发送失败告警失败: {str(e)}")
            return False
    
    def send_system_alert(self, subject: str, message: str, recipients: List[str] = None) -> bool:
        """
        发送系统告警
        
        Args:
            subject: 主题
            message: 消息内容
            recipients: 收件人列表，如果为None则使用默认管理员
            
        Returns:
            是否发送成功
        """
        try:
            if recipients is None:
                # 获取默认管理员
                admin_email = self.pg_connector.get_system_config('admin_email', 'admin@example.com')
                recipients = [admin_email]
            
            # 发送邮件
            for recipient in recipients:
                self._send_email(recipient, subject, message)
            
            return True
            
        except Exception as e:
            logger.error(f"发送系统告警失败: {str(e)}")
            return False
    
    def _send_email(self, recipient: str, subject: str, body: str) -> bool:
        """
        发送邮件
        
        Args:
            recipient: 收件人
            subject: 主题
            body: 内容
            
        Returns:
            是否发送成功
        """
        try:
            # 获取SMTP配置
            smtp_server = self.smtp_config.get('server', 'localhost')
            smtp_port = self.smtp_config.get('port', 25)
            smtp_user = self.smtp_config.get('user')
            smtp_password = self.smtp_config.get('password')
            smtp_from = self.smtp_config.get('from', 'scheduler@example.com')
            
            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = smtp_from
            msg['To'] = recipient
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain'))
            
            # 发送邮件
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                if smtp_user and smtp_password:
                    server.login(smtp_user, smtp_password)
                server.sendmail(smtp_from, recipient, msg.as_string())
            
            logger.info(f"已发送告警邮件到 {recipient}")
            return True
            
        except Exception as e:
            logger.error(f"发送告警邮件失败: {str(e)}")
            return False 