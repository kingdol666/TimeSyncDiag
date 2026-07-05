"""
API 冒烟测试：验证主服务核心端点可用
"""
import requests
import json

BASE = "http://127.0.0.1:8002"
CNN_BASE = "http://127.0.0.1:8003"

results = []

def check(name, method, path, expected_status=200, payload=None, files=None, data=None):
    url = BASE + path
    try:
        if method == "GET":
            r = requests.get(url, timeout=30)
        elif method == "POST":
            r = requests.post(url, json=payload, files=files, data=data, timeout=30)
        elif method == "PATCH":
            r = requests.patch(url, json=payload, timeout=30)
        else:
            raise ValueError(f"Unsupported method: {method}")
        ok = r.status_code == expected_status
        results.append((name, ok, r.status_code, r.text[:200] if not ok else ""))
        print(f"{'✅' if ok else '❌'} {name}: {r.status_code}")
        return r
    except Exception as e:
        results.append((name, False, 0, str(e)))
        print(f"❌ {name}: {e}")
        return None

# 健康检查
check("主服务健康", "GET", "/health")

# 配置
check("获取完整配置", "GET", "/api/config")
check("获取database配置", "GET", "/api/config/database")
check("修改kafka传感器间隔", "PATCH", "/api/config", payload={"updates": {"kafka.sensor_producer_interval": 1.5}})
check("恢复kafka传感器间隔", "PATCH", "/api/config", payload={"updates": {"kafka.sensor_producer_interval": 1.0}})
check("重载配置", "POST", "/api/config/reload")

# 生产者
check("获取生产者状态", "GET", "/api/producer/status")
check("停止检测生产者", "POST", "/api/producer/detection/stop")
check("启动检测生产者", "POST", "/api/producer/detection/start")
check("停止传感器生产者", "POST", "/api/producer/sensor/stop")
check("启动传感器生产者", "POST", "/api/producer/sensor/start")

# 消费者
check("获取消费者状态", "GET", "/api/consumer/status")
check("停止消费者", "POST", "/api/consumer/stop")
check("启动消费者", "POST", "/api/consumer/start")

# 膜厚云图
check("获取云图管道状态", "GET", "/api/thickness-map/status")
check("获取最新云图", "GET", "/api/thickness-map/map/latest")
check("生成云图", "POST", "/api/thickness-map/generate")

# 图片分析
check("分页获取图片分析", "POST", "/api/image-analysis/paginated", payload={"page": 1, "page_size": 5})

# CNN服务健康
r = requests.get(CNN_BASE + "/health", timeout=10)
ok = r.status_code == 200
results.append(("CNN服务健康", ok, r.status_code, r.text[:200] if not ok else ""))
print(f"{'✅' if ok else '❌'} CNN服务健康: {r.status_code}")

passed = sum(1 for _, ok, _, _ in results if ok)
print(f"\n结果: {passed}/{len(results)} 通过")
if passed != len(results):
    raise SystemExit(1)
