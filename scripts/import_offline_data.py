"""
产线离线 CSV 数据导入脚本

将 /data/realtime/ 下的膜厚检测 CSV 数据导入到 TimescaleDB 的 detection_device_data 表。

数据来源：转置后的产线膜厚检测数据文件（trip_transposed_*.csv）
数据格式：每个 CSV 包含时间列 + 3000个探头位置列
导入目标：detection_device_data 表（device_id=THK-00x, detection_type=thickness_measurement）
"""

import os
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

# ── 路径配置 ──────────────────────────────────────────
# 将项目根目录加入 sys.path，以便导入 logic 模块
BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))

from fastapi.logic.models.db_connection import DatabaseConnection
from fastapi.logic.models.models import DetectionDeviceData
from fastapi.logic.utils.paths import get_realtime_dir, get_motou_data_dir, get_other_data_dir

# CSV 数据文件夹（从统一路径管理读取）
REALTIME_DIR = get_realtime_dir()
MOTOU_DATA_DIR = get_motou_data_dir()
OTHER_DATA_DIR = get_other_data_dir()
DEVICE_IDS = ["THK-001", "THK-002", "THK-003", "THK-004", "THK-005"]
BATCH_SIZE = 100  # 每批写入的行数

# ── 导入函数 ──────────────────────────────────────────


def import_realtime_csv(db_conn):
    """
    导入 realtime/ 下的所有 trip_transposed_*.csv 文件到 detection_device_data 表。
    """
    csv_files = sorted(REALTIME_DIR.glob("trip_transposed_*.csv"))
    if not csv_files:
        print(f"[WARN] 在 {REALTIME_DIR} 中未找到 trip_transposed_*.csv 文件")
        return 0

    print(f"[INFO] 找到 {len(csv_files)} 个 CSV 文件")
    total_imported = 0

    for csv_file in csv_files:
        print(f"\n[INFO] 处理文件: {csv_file.name}")
        try:
            df = pd.read_csv(csv_file, encoding="utf-8")
            print(f"       形状: {df.shape[0]} 行 × {df.shape[1]} 列")
        except Exception as e:
            print(f"  [ERROR] 读取 CSV 失败: {e}")
            continue

        device_id = _infer_device_id(csv_file.name)
        session = db_conn.get_session()
        batch = []
        row_count = 0

        for idx, (_, row) in enumerate(df.iterrows()):
            msg = dict(row)

            # 提取 time 列作为时间戳
            timestamp_str = str(msg.pop("time", "")) if "time" in msg else ""

            # 剩余列视为探头检测值，转换为 float 列表
            values = []
            for col_name, val in msg.items():
                try:
                    values.append(float(val))
                except (ValueError, TypeError):
                    continue

            # 构建 DetectionDeviceData.from_kafka_message 可识别的消息结构
            kafka_msg = {
                "device_id": device_id,
                "detection_type": "thickness_measurement",
                "values": values[:3000],  # 最多 3000 个探头位置
                "timestamp": timestamp_str,
            }

            detection_data = DetectionDeviceData.from_kafka_message(kafka_msg)
            batch.append(detection_data)

            if len(batch) >= BATCH_SIZE:
                session.add_all(batch)
                session.commit()
                row_count += len(batch)
                total_imported += len(batch)
                print(f"  已导入 {row_count} 行 ...", end="\r")
                batch = []

        # 剩余批次
        if batch:
            session.add_all(batch)
            session.commit()
            row_count += len(batch)
            total_imported += len(batch)

        session.close()
        print(f"  [OK] 文件完成: 共 {row_count} 行")

    return total_imported


def import_motou_data(db_conn):
    """
    导入 motouData/ 下的 CSV 到 motou_data 表。
    """
    from fastapi.logic.models.models import MotouData

    data_dir = get_motou_data_dir()
    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        print(f"[WARN] motouData 目录无 CSV 文件")
        return 0

    return _import_generic(db_conn, csv_files, "motou", MotouData.from_csv_row)


def import_other_data(db_conn):
    """
    导入 otherData/ 下的 CSV 到 other_data 表。
    """
    from fastapi.logic.models.models import OtherData

    data_dir = get_other_data_dir()
    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        print(f"[WARN] otherData 目录无 CSV 文件")
        return 0

    return _import_generic(db_conn, csv_files, "其他设备数据", OtherData.from_csv_row)


def _import_generic(db_conn, csv_files, label, from_row_func):
    """通用 CSV 导入函数"""
    total = 0
    for csv_file in csv_files:
        print(f"  处理 {csv_file.name} ...")
        df = pd.read_csv(csv_file, encoding="utf-8")
        session = db_conn.get_session()
        batch = []
        for _, row in df.iterrows():
            obj = from_row_func(row.to_dict())
            batch.append(obj)
            if len(batch) >= 1000:
                session.bulk_save_objects(batch)
                session.commit()
                total += len(batch)
                batch = []
        if batch:
            session.bulk_save_objects(batch)
            session.commit()
            total += len(batch)
        session.close()
        print(f"    [OK] 导入 {total}")
    return total


def _infer_device_id(filename: str) -> str:
    """从文件名推断设备 ID"""
    import re

    # 提取 trip_transposed_N_ 中的 N
    m = re.search(r"trip_transposed_(\d+)_", filename)
    if m:
        idx = int(m.group(1))
        if 1 <= idx <= len(DEVICE_IDS):
            return DEVICE_IDS[idx - 1]
    # 默认轮流分配
    return DEVICE_IDS[hash(filename) % len(DEVICE_IDS)]


def ensure_hypertable(db_conn, table_name: str):
    """确保表已创建为 TimescaleDB 超表"""
    try:
        with db_conn.engine.connect() as conn:
            r = conn.execute(
                text(
                    "SELECT EXISTS (SELECT 1 FROM timescaledb_information.hypertables "
                    f"WHERE hypertable_name = '{table_name}')"
                )
            )
            if not r.scalar():
                conn.execute(text(f"SELECT create_hypertable('{table_name}', 'timestamp', if_not_exists => true)"))
                conn.commit()
                print(f"[INFO] {table_name} 超表创建成功")
            else:
                print(f"[INFO] {table_name} 已是超表")
    except Exception as e:
        print(f"[WARN] {table_name} 超表创建跳过: {e}")


# ── 主入口 ──────────────────────────────────────────────


def main():
    print("=" * 60)
    print("  产线离线 CSV 数据导入工具")
    print("=" * 60)

    db_conn = DatabaseConnection()
    if not db_conn.connect():
        print("[ERROR] 数据库连接失败，请确认 Docker 容器已启动")
        sys.exit(1)

    print(f"[INFO] 数据库已连接 (tsdb)")
    print(f"[INFO] realtime 数据目录: {REALTIME_DIR}")

    # 创建超表（已有数据时会跳过）
    for t in ["detection_device_data", "motou_data", "other_data"]:
        ensure_hypertable(db_conn, t)

    # ── 导入 detect_device_data （realtime 膜厚 CSV）──
    print("\n── 1. 膜厚检测数据 ──")
    count1 = import_realtime_csv(db_conn)

    # ── 导入 motou_data ──
    print("\n── 2. 模头数据 ──")
    count2 = import_motou_data(db_conn)

    # ── 导入 other_data ──
    print("\n── 3. 其他设备数据 ──")
    count3 = import_other_data(db_conn)

    db_conn.close()


    db_conn.close()

    total = count1 + count2 + count3
    print("")
    print("=" * 60)
    print("  Import Summary")
    print("    membrane thickness: " + str(count1) + " rows -> detection_device_data")
    print("    motou data:        " + str(count2) + " rows -> motou_data")
    print("    other data:        " + str(count3) + " rows -> other_data")
    print("    ------------")
    print("    TOTAL:             " + str(total) + " rows")
    print("=" * 60)



if __name__ == "__main__":
    main()
