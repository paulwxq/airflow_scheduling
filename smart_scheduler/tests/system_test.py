"""
系统功能验证脚本
用于验证智能调度系统的基本功能
"""
import os
import sys
import time
import unittest
import requests
import uuid
import json
from datetime import datetime

# 添加父目录到路径，以便导入smart_scheduler模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from smart_scheduler.db.connectors import PostgreSQLConnector

# 测试配置
API_BASE_URL = "http://localhost:5000"
DB_CONN_STRING = "postgresql://postgres:postgres@localhost:5432/dataops"
TEST_TABLE_NAME = f"test_table_{uuid.uuid4().hex[:8]}"


class SystemTest(unittest.TestCase):
    """系统功能测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试前的准备工作"""
        cls.pg_connector = PostgreSQLConnector(DB_CONN_STRING)
        
        # 创建测试表
        try:
            # 创建表调度配置
            cls.pg_connector.execute_update(f"""
                INSERT INTO table_schedule (
                    table_name, owner_id, schedule_frequency, priority
                ) VALUES (
                    '{TEST_TABLE_NAME}', 'test_user', 'daily', 3
                )
            """)
            
            # 创建处理脚本
            test_script = f"""
                print("处理 {TEST_TABLE_NAME}")
                # 模拟数据处理
                import time
                time.sleep(2)
                print("处理完成")
            """
            
            cls.pg_connector.execute_update(f"""
                INSERT INTO processing_script (
                    table_name, script_type, script_content
                ) VALUES (
                    '{TEST_TABLE_NAME}', 'python', '{test_script}'
                )
            """)
            
            print(f"测试表 {TEST_TABLE_NAME} 创建完成")
            
        except Exception as e:
            print(f"设置测试环境失败: {str(e)}")
            raise
    
    @classmethod
    def tearDownClass(cls):
        """测试后的清理工作"""
        try:
            # 删除测试表配置
            cls.pg_connector.execute_update(f"""
                DELETE FROM table_schedule WHERE table_name = '{TEST_TABLE_NAME}'
            """)
            
            # 删除处理脚本
            cls.pg_connector.execute_update(f"""
                DELETE FROM processing_script WHERE table_name = '{TEST_TABLE_NAME}'
            """)
            
            # 删除执行记录
            cls.pg_connector.execute_update(f"""
                DELETE FROM execution_history WHERE table_name = '{TEST_TABLE_NAME}'
            """)
            
            print(f"测试表 {TEST_TABLE_NAME} 清理完成")
            
        except Exception as e:
            print(f"清理测试环境失败: {str(e)}")
    
    def test_01_api_health(self):
        """测试API健康状态"""
        try:
            response = requests.get(f"{API_BASE_URL}/health")
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data['status'], 'healthy')
            print("API健康状态检查通过")
        except Exception as e:
            self.fail(f"API健康检查失败: {str(e)}")
    
    def test_02_refresh_dependency(self):
        """测试刷新依赖图"""
        try:
            response = requests.get(f"{API_BASE_URL}/api/v1/dependency/refresh")
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertTrue('success' in data)
            self.assertTrue(data['success'])
            print("依赖图刷新测试通过")
        except Exception as e:
            self.fail(f"依赖图刷新测试失败: {str(e)}")
    
    def test_03_get_tables(self):
        """测试获取表列表"""
        try:
            response = requests.get(f"{API_BASE_URL}/api/v1/tables")
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertTrue('tables' in data)
            
            # 检查我们的测试表是否在列表中
            found = False
            for table in data['tables']:
                if table['table_name'] == TEST_TABLE_NAME:
                    found = True
                    break
            
            self.assertTrue(found, f"未找到测试表 {TEST_TABLE_NAME}")
            print("获取表列表测试通过")
        except Exception as e:
            self.fail(f"获取表列表测试失败: {str(e)}")
    
    def test_04_execute_immediate(self):
        """测试立即执行功能"""
        try:
            # 发送立即执行请求
            response = requests.post(
                f"{API_BASE_URL}/api/v1/execute/immediate",
                json={"table_name": TEST_TABLE_NAME}
            )
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertTrue('execution_id' in data)
            execution_id = data['execution_id']
            
            # 等待执行完成并检查状态
            max_wait = 30  # 最多等待30秒
            wait_time = 0
            status = None
            
            while wait_time < max_wait:
                status_response = requests.get(
                    f"{API_BASE_URL}/api/v1/status/{execution_id}"
                )
                self.assertEqual(status_response.status_code, 200)
                status_data = status_response.json()
                status = status_data.get('status')
                
                if status in ['COMPLETED', 'FAILED']:
                    break
                
                time.sleep(2)
                wait_time += 2
            
            self.assertEqual(status, 'COMPLETED', f"执行状态异常: {status}")
            print("立即执行测试通过")
        except Exception as e:
            self.fail(f"立即执行测试失败: {str(e)}")
    
    def test_05_get_config(self):
        """测试获取配置"""
        try:
            response = requests.get(f"{API_BASE_URL}/api/v1/config/max_concurrency")
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertTrue('key' in data)
            self.assertTrue('value' in data)
            self.assertEqual(data['key'], 'max_concurrency')
            print("获取配置测试通过")
        except Exception as e:
            self.fail(f"获取配置测试失败: {str(e)}")


if __name__ == "__main__":
    print(f"开始系统功能测试，测试表: {TEST_TABLE_NAME}")
    print("-" * 50)
    
    # 运行测试
    unittest.main() 