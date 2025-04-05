# 智能调度系统 (Smart Scheduler)

一个基于Airflow的分布式智能调度系统，用于管理数据处理任务和优化资源使用。

## 系统概述

智能调度系统是一个专为数据处理和表依赖管理设计的解决方案。它使用Apache Airflow作为基础调度引擎，同时提供了智能化的额外功能：

- **依赖关系管理**: 自动识别和处理表之间的依赖关系
- **执行计划优化**: 生成最优执行计划以减少执行时间
- **资源使用优化**: 根据系统负载智能分配资源
- **REST API接口**: 提供灵活的接口进行交互
- **支持多种处理方式**: 可执行Python脚本或SQL语句

## 技术栈

- Python 3.12+
- Apache Airflow 2.10.x
- PostgreSQL 17 (元数据存储)
- Neo4j 3.5.x (依赖图存储)
- Flask (API层)

## 项目结构

```
airflow_scheduling/
├── .venv/                # Python虚拟环境
├── dags/                 # Airflow DAG定义
│   └── smart_scheduler_dag.py  # 示例DAG
├── smart_scheduler/      # 核心调度系统
│   ├── __init__.py
│   ├── __main__.py       # 程序入口点
│   ├── api/              # API接口层
│   │   ├── __init__.py
│   │   └── app.py        # Flask API应用
│   ├── core/             # 核心功能
│   │   ├── __init__.py
│   │   ├── dependency_manager.py  # 依赖管理器
│   │   └── execution_strategy.py  # 执行策略
│   ├── db/               # 数据库连接器
│   │   ├── __init__.py
│   │   └── connectors.py # 数据库连接器
│   ├── tests/            # 单元测试
│   │   └── __init__.py
│   └── utils/            # 辅助工具
│       └── __init__.py
└── requirements.txt      # 依赖项
```

## 安装步骤

1. 克隆仓库
```bash
git clone <repository-url>
cd airflow_scheduling
```

2. 创建并激活虚拟环境
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# 或
.venv\Scripts\activate     # Windows
```

3. 安装依赖
```bash
pip install -r requirements.txt
```

4. 初始化数据库
```bash
python -m smart_scheduler --init-db
```

5. 启动API服务
```bash
python -m smart_scheduler
```

## 配置Airflow

1. 确保Airflow已正确安装并配置
2. 将`dags`目录设置为Airflow的DAG目录，或将其中的DAG文件复制到Airflow的DAG目录
3. 在Airflow中设置变量`scheduler_api_url`，指向智能调度系统API地址
4. 重启Airflow Web Server和Scheduler

## API接口

智能调度系统提供以下REST API接口：

| 接口 | 方法 | 描述 |
|------|------|------|
| `/api/v1/dependency/refresh` | GET | 刷新依赖图 |
| `/api/v1/dependency/status` | GET | 获取依赖图状态 |
| `/api/v1/execute/immediate` | POST | 立即执行任务 |
| `/api/v1/status/<execution_id>` | GET | 获取执行状态 |
| `/api/v1/config/<key>` | GET/PUT | 获取/设置配置项 |
| `/api/v1/tables` | GET | 获取所有表及其调度信息 |
| `/api/v1/tables/<table_name>/dependencies` | GET | 获取特定表的依赖关系 |
| `/health` | GET | 健康检查 |

## 使用示例

### 通过API执行任务

```python
import requests

# 立即执行特定表的处理
response = requests.post(
    "http://localhost:5000/api/v1/execute/immediate",
    json={"table_name": "my_data_table"}
)

execution_id = response.json()["execution_id"]

# 检查执行状态
status_response = requests.get(
    f"http://localhost:5000/api/v1/status/{execution_id}"
)
print(status_response.json())
```

### 在Airflow中使用

系统提供了一个示例DAG `smart_scheduler_dag.py`，展示了如何在Airflow中与智能调度系统集成。该DAG将：

1. 从智能调度系统获取当天需要执行的表及其依赖关系
2. 根据依赖关系动态创建任务
3. 执行这些任务并监控其状态

## 贡献指南

1. Fork项目
2. 创建功能分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建Pull Request

## 许可证

[MIT](LICENSE) 