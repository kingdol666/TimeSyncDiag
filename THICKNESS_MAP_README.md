# 膜厚温度云图功能说明

## 功能概述

本功能实现了每隔1分钟自动生成膜厚数据温度云图，并将结果保存到数据库中。温度云图展示了当前时间前一分钟内的膜厚数据分布情况。

## 组件说明

### 1. 数据库模型 (ThicknessMap)

位置: `fastapi/logic/models/models.py`

字段说明:
- `id`: 主键
- `start_time`: 温度云图数据起始时间
- `end_time`: 温度云图数据结束时间
- `map_image`: 温度云图图像数据 (LargeBinary类型)
- `data_points_count`: 数据点数量
- `created_at`: 创建时间

### 2. 膜厚数据处理器 (ThicknessMapProcessor)

位置: `fastapi/logic/processors/thickness_map_processor.py`

主要方法:
- `generate_thickness_map(start_time, end_time)`: 生成指定时间范围的温度云图
- `save_thickness_map(start_time, end_time)`: 生成并保存温度云图到数据库
- `generate_and_save_latest_map()`: 生成并保存最近一分钟的温度云图

### 3. 温度云图消费者 (ThicknessMapConsumer)

位置: `fastapi/logic/consumers/thickness_map_consumer.py`

功能:
- 每分钟自动生成温度云图
- 提供启动/停止控制
- 提供状态查询功能

### 4. API端点

位置: `fastapi/routers/thickness_map.py`

端点列表:
- `GET /api/thickness-map/status`: 获取温度云图消费者状态
- `POST /api/thickness-map/generate`: 立即生成一次温度云图
- `POST /api/thickness-map/start`: 启动温度云图消费者
- `POST /api/thickness-map/stop`: 停止温度云图消费者

## 使用方法

### 1. 启动应用

```bash
cd fastapi
python main.py
```

应用启动后会自动启动温度云图消费者，开始每分钟生成一次温度云图。

### 2. 通过API控制

#### 获取消费者状态
```bash
curl -X GET "http://localhost:8000/api/thickness-map/status"
```

#### 立即生成温度云图
```bash
curl -X POST "http://localhost:8000/api/thickness-map/generate"
```

#### 启动消费者
```bash
curl -X POST "http://localhost:8000/api/thickness-map/start"
```

#### 停止消费者
```bash
curl -X POST "http://localhost:8000/api/thickness-map/stop"
```

### 3. 测试功能

运行测试脚本:
```bash
cd TimeSyncDiag
python test_thickness_map.py
```

选择测试模式:
1. 测试膜厚温度云图生成功能
2. 测试API端点
3. 运行所有测试

## 数据查看

温度云图以二进制形式存储在数据库的`thickness_map`表中。可以通过以下方式查看:

1. 使用数据库客户端直接查询
2. 通过API获取温度云图数据并解码显示

## 注意事项

1. 确保数据库中有足够的膜厚数据用于生成温度云图
2. 温度云图生成需要一定时间，建议在低峰期进行大量测试
3. 消费者默认每分钟执行一次，可通过修改代码调整间隔时间
4. 确保matplotlib库已正确安装，用于生成温度云图

## 故障排除

1. 如果温度云图生成失败，检查数据库连接是否正常
2. 如果没有数据，确认膜厚数据是否正常收集
3. 如果API无法访问，确认FastAPI应用是否正常运行
4. 查看日志文件获取详细错误信息