"""
CNN 模型权重下载脚本

模型来自产线膜厚数据的 ResNet50 二分类训练（正常/异常），大小约 128MB。

使用方式：
    uv run python cnn_image_classification/download_model.py

或手动下载后放入 cnn_image_classification/ 目录。
"""

import os
import sys
from pathlib import Path

RELEASE_URL = "https://github.com/kingdol666/TimeSyncDiag/releases/download/v1.0/best_resnet_model.keras"
SCRIPT_DIR = Path(__file__).parent
MODEL_PATH = SCRIPT_DIR / "best_resnet_model.keras"


def main():
    print("=" * 60)
    print("  TimeSyncDiag - CNN 模型权重下载")
    print("=" * 60)
    print(f"\n  目标路径: {MODEL_PATH}\n")

    # 检查是否已存在
    if MODEL_PATH.exists():
        size = MODEL_PATH.stat().st_size / (1024 * 1024)
        print(f"  模型已存在: {size:.1f} MB")
        print("  如需重新下载请先删除文件。")
        return

    # 尝试通过 requests 下载
    try:
        import requests
        print("  [1/2] 正在从 GitHub Releases 下载模型...")
        print(f"        源: {RELEASE_URL}\n")
        response = requests.get(RELEASE_URL, stream=True, timeout=300)
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))
        downloaded = 0
        with open(MODEL_PATH, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total > 0:
                    pct = downloaded / total * 100
                    sys.stdout.write(f"\r  下载进度: {pct:.1f}% ({downloaded/1024/1024:.1f} MB)")
                    sys.stdout.flush()
        print()
        if MODEL_PATH.exists():
            size = MODEL_PATH.stat().st_size / (1024 * 1024)
            print(f"\n  ✅ 下载成功! ({size:.1f} MB)")
            return
    except ImportError:
        print("  [1/2] requests 库不可用，尝试 urllib...")
    except Exception as e:
        print(f"  [1/2] 下载失败: {e}")

    # 备用方式: urllib
    try:
        from urllib.request import urlopen
        print("  [2/2] 正在通过 urllib 下载模型...\n")
        with urlopen(RELEASE_URL, timeout=300) as response:
            total = int(response.headers.get("content-length", 0))
            downloaded = 0
            with open(MODEL_PATH, "wb") as f:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        pct = downloaded / total * 100
                        sys.stdout.write(f"\r  下载进度: {pct:.1f}% ({downloaded/1024/1024:.1f} MB)")
                        sys.stdout.flush()
        print()
        if MODEL_PATH.exists():
            size = MODEL_PATH.stat().st_size / (1024 * 1024)
            print(f"\n  ✅ 下载成功! ({size:.1f} MB)")
            return
    except Exception as e:
        print(f"  [2/2] 下载失败: {e}")

    print()
    print("=" * 60)
    print("  所有自动下载方式均失败，请手动下载：")
    print()
    print("    1. 访问: https://github.com/kingdol666/TimeSyncDiag/releases")
    print("    2. 下载 best_resnet_model.keras")
    print(f"    3. 放入: {SCRIPT_DIR}/")
    print("=" * 60)
    sys.exit(1)


if __name__ == "__main__":
    main()
