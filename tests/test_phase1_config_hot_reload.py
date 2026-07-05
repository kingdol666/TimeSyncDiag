"""
第一阶段配置热更新测试
验证 Kafka 生产/消费间隔参数可通过 API 修改并触发回调
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from backend.config.config_loader import config
from backend.logic.producers.producer import SensorDataProducer, DetectionDataProducer
from backend.logic.consumers.kafka_consumer import KafkaMessageConsumer


def test_sensor_interval_hot_reload():
    producer = SensorDataProducer()
    assert producer.interval == config.kafka.sensor_producer_interval

    config.update({"kafka.sensor_producer_interval": 2.5}, save=False)
    assert producer.interval == 2.5
    print("✅ 传感器生产间隔热更新回调正确")

    # 恢复默认值
    config.update({"kafka.sensor_producer_interval": 1.0}, save=False)


def test_detection_interval_hot_reload():
    producer = DetectionDataProducer()
    original = producer.interval
    assert original == config.kafka.detection_producer_interval

    config.update({"kafka.detection_producer_interval": 10.0}, save=False)
    assert producer.interval == 10.0
    print("✅ 检测生产间隔热更新回调正确")

    # 恢复默认值
    config.update({"kafka.detection_producer_interval": original}, save=False)


def test_consumer_poll_timeout_hot_reload():
    consumer = KafkaMessageConsumer()
    assert consumer.poll_timeout_ms == config.kafka.consumer_poll_timeout_ms

    config.update({"kafka.consumer_poll_timeout_ms": 2000}, save=False)
    assert consumer.poll_timeout_ms == 2000
    print("✅ Kafka 消费者 poll 超时热更新回调正确")

    # 恢复默认值
    config.update({"kafka.consumer_poll_timeout_ms": 1000}, save=False)


def test_config_persistence():
    """验证更新配置后会持久化到 config.yml（使用临时值测试）"""
    original = config.kafka.sensor_producer_interval
    config.update({"kafka.sensor_producer_interval": 3.0}, save=True)

    # 重新加载验证
    config.reload()
    assert config.kafka.sensor_producer_interval == 3.0
    print("✅ 配置更新已持久化到 config.yml")

    # 恢复
    config.update({"kafka.sensor_producer_interval": original}, save=True)


if __name__ == "__main__":
    test_sensor_interval_hot_reload()
    test_detection_interval_hot_reload()
    test_consumer_poll_timeout_hot_reload()
    test_config_persistence()
    print("\n🎉 第一阶段配置热更新测试全部通过")
