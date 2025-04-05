"""
应用程序入口点，提供命令行接口和初始化功能
"""
import os
import sys
import logging
import argparse
from datetime import datetime

from .api.app import SchedulerAPI
from .db.connectors import PostgreSQLConnector


def init_database():
    """初始化数据库表结构"""
    print("初始化数据库...")
    conn_string = os.getenv('PG_CONN_STRING', 'postgresql://postgres:postgres@localhost:5432/dataops')
    
    pg_connector = PostgreSQLConnector(conn_string)
    
    try:
        # 创建表调度配置表
        pg_connector.execute_update("""
            CREATE TABLE IF NOT EXISTS table_schedule (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                owner_id VARCHAR(100) NOT NULL,
                schedule_frequency VARCHAR(50) NOT NULL,
                schedule_time TIME,
                schedule_day INTEGER,
                execution_mode VARCHAR(50) DEFAULT 'full_refresh',
                is_enabled BOOLEAN DEFAULT TRUE,
                priority INTEGER DEFAULT 5,
                max_retry INTEGER DEFAULT 5,
                retry_delay_minutes INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_scheduled_at TIMESTAMP,
                UNIQUE(table_name)
            )
        """)
        
        # 创建执行记录表
        pg_connector.execute_update("""
            CREATE TABLE IF NOT EXISTS execution_history (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                execution_id VARCHAR(100),
                execution_type VARCHAR(50),
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                status VARCHAR(50),
                retry_count INTEGER DEFAULT 0,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建处理脚本管理表
        pg_connector.execute_update("""
            CREATE TABLE IF NOT EXISTS processing_script (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                script_type VARCHAR(50) NOT NULL,
                script_content TEXT,
                script_path VARCHAR(255),
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(table_name, script_type)
            )
        """)
        
        # 创建系统配置表
        pg_connector.execute_update("""
            CREATE TABLE IF NOT EXISTS system_config (
                config_key VARCHAR(100) PRIMARY KEY,
                config_value VARCHAR(255) NOT NULL,
                description TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建调度变更跟踪表
        pg_connector.execute_update("""
            CREATE TABLE IF NOT EXISTS schedule_change_tracking (
                id SERIAL PRIMARY KEY,
                last_scan_time TIMESTAMP,
                frequency_type VARCHAR(50),
                is_processed BOOLEAN DEFAULT FALSE,
                process_time TIMESTAMP,
                process_status VARCHAR(50),
                error_message TEXT
            )
        """)
        
        # 添加初始系统配置
        pg_connector.execute_update("""
            INSERT INTO system_config (config_key, config_value, description)
            VALUES
                ('max_concurrency', '10', '系统最大并行执行任务数'),
                ('refresh_interval_minutes', '15', '元数据刷新间隔（分钟）'),
                ('default_max_retries', '5', '默认最大重试次数'),
                ('default_retry_delay_minutes', '1', '默认重试间隔（分钟）'),
                ('history_retention_days', '180', '历史记录保留天数')
            ON CONFLICT (config_key) DO NOTHING
        """)
        
        print("数据库初始化完成")
        
    except Exception as e:
        print(f"数据库初始化失败: {str(e)}")
        raise


def run_api_server(host, port, debug):
    """运行API服务器"""
    # 设置日志
    logging.basicConfig(
        level=logging.INFO if not debug else logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建API并运行
    api = SchedulerAPI()
    api.run(host=host, port=port, debug=debug)


def main():
    """主函数，处理命令行参数并运行相应功能"""
    parser = argparse.ArgumentParser(description='智能调度系统')
    parser.add_argument('--init-db', action='store_true', help='初始化数据库')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='API服务器主机')
    parser.add_argument('--port', type=int, default=5000, help='API服务器端口')
    parser.add_argument('--debug', action='store_true', help='启用调试模式')
    
    args = parser.parse_args()
    
    if args.init_db:
        init_database()
        return
    
    run_api_server(args.host, args.port, args.debug)


if __name__ == '__main__':
    main() 