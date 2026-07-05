"""
第一阶段 Kafka 配置读取测试
验证 producer、consumer、kafka_consumer 从 config.yml 读取配置
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from backend.config.config_loader import config
from backend.logic.producers.producer import SensorDataProducer, DetectionDataProducer
from backend.logic.consumers.kafka_consumer import KafkaMessageConsumer
from backend.logic.consumers.consumer import SensorDataPipeline, DetectionDataPipeline, build_pipeline_config


def test_config_loads_kafka_fields():
    assert config.kafka.bootstrap_servers == "localhost:9092"
    assert config.kafka.sensor_topic == "sensor_data"
    assert config.kafka.detection_topic == "detection_data"
    assert config.kafka.sensor_producer_interval == 1.0
    assert config.kafka.detection_producer_interval == 5.0
    assert config.kafka.consumer_poll_timeout_ms == 1000
    print("✅ config.yml Kafka 字段加载正确")


def test_sensor_producer_defaults():
    p = SensorDataProducer()
    assert p.bootstrap_servers == config.kafka.bootstrap_servers
    assert p.topic == config.kafka.sensor_topic
    assert p.interval == config.kafka.sensor_producer_interval
    print("✅ SensorDataProducer 默认参数从配置读取正确")


def test_detection_producer_defaults():
    p = DetectionDataProducer()
    assert p.bootstrap_servers == config.kafka.bootstrap_servers
    assert p.topic == config.kafka.detection_topic
    assert p.interval == config.kafka.detection_producer_interval
    print("✅ DetectionDataProducer 默认参数从配置读取正确")


def test_kafka_consumer_defaults():
    c = KafkaMessageConsumer()
    assert c.bootstrap_servers == config.kafka.bootstrap_servers
    assert c.topic == config.kafka.sensor_topic
    assert c.group_id == "sensor_data_consumer"
    assert c.poll_timeout_ms == config.kafka.consumer_poll_timeout_ms
    print("✅ KafkaMessageConsumer 默认参数从配置读取正确")


def test_pipeline_config_builder():
    cfg = build_pipeline_config(topic=config.kafka.detection_topic, group_id="detection_data_consumer")
    assert cfg["kafka"]["bootstrap_servers"] == config.kafka.bootstrap_servers
    assert cfg["kafka"]["topic"] == config.kafka.detection_topic
    assert cfg["kafka"]["group_id"] == "detection_data_consumer"
    assert cfg["kafka"]["poll_timeout_ms"] == config.kafka.consumer_poll_timeout_ms
    assert "postgresql://" in cfg["database"]["url"]
    print("✅ Pipeline 配置构建器正确")


def test_pipeline_defaults():
    sensor_pipe = SensorDataPipeline()
    assert sensor_pipe.config["kafka"]["topic"] == config.kafka.sensor_topic
    assert sensor_pipe.config["kafka"]["group_id"] == "sensor_data_consumer"

    detection_pipe = DetectionDataPipeline()
    assert detection_pipe.config["kafka"]["topic"] == config.kafka.detection_topic
    assert detection_pipe.config["kafka"]["group_id"] == "detection_data_consumer"
    print("✅ Pipeline 默认配置正确")


if __name__ == "__main__":
    test_config_loads_kafka_fields()
    test_sensor_producer_defaults()
    test_detection_producer_defaults()
    test_kafka_consumer_defaults()
    test_pipeline_config_builder()
    test_pipeline_defaults()
    print("\n🎉 第一阶段 Kafka 配置改造测试全部通过")
