# NFA-Forgotten-Archieve
以chatbot为基础的群聊成员自动备份程序

## 安装依赖

### 运行依赖（API 服务）

```bash
pip install -r requirements.txt
```

### 测试依赖（含运行依赖）

```bash
pip install -r requirements-dev.txt
```

## 运行测试

```bash
# API 测试（无需数据库）
python -m pytest tests/test_api.py -v

# 分类测试
python -m pytest tests/test_classification.py -v

# 全套测试
python -m pytest tests/ -v
```

## 启动 API 服务

```bash
# 开发模式（自动重载）
python scripts/run_api.py --reload

# 生产模式
python scripts/run_api.py --host 0.0.0.0 --port 8000

# 交互文档
# http://localhost:8000/docs   (Swagger UI)
# http://localhost:8000/redoc  (ReDoc)
```
