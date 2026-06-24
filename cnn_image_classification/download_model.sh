#!/bin/bash
# ============================================================
# CNN 模型权重下载脚本
# 模型来自产线膜厚数据的 ResNet50 二分类训练（正常/异常）
# 大小约 128MB
#
# 使用方式：
#   chmod +x cnn_image_classification/download_model.sh
#   ./cnn_image_classification/download_model.sh
#
# 或手动下载后放入 cnn_image_classification/ 目录
# ============================================================

MODEL_URL="https://github.com/kingdol666/TimeSyncDiag/releases/download/v1.0/best_resnet_model.keras"
MODEL_DIR="$(cd "$(dirname "$0")/.." && pwd)/cnn_image_classification"
MODEL_PATH="$MODEL_DIR/best_resnet_model.keras"

echo "============================================"
echo "TimeSyncDiag - CNN 模型权重下载"
echo "============================================"
echo ""
echo "目标路径: $MODEL_PATH"
echo ""

# 检查是否已存在
if [ -f "$MODEL_PATH" ]; then
    FILESIZE=$(ls -lh "$MODEL_PATH" | awk '{print $5}')
    echo "模型已存在: $FILESIZE"
    echo "如需重新下载请先删除文件。"
    exit 0
fi

# 方法1: 从 GitHub Releases 下载（优先）
if command -v curl &> /dev/null; then
    echo "[1/2] 正在从 GitHub Releases 下载模型..."
    echo "      源: $MODEL_URL"
    curl -L -o "$MODEL_PATH" "$MODEL_URL" --progress-bar
    
    if [ $? -eq 0 ] && [ -f "$MODEL_PATH" ]; then
        FILESIZE=$(ls -lh "$MODEL_PATH" | awk '{print $5}')
        echo "✅ 下载成功! ($FILESIZE)"
        exit 0
    else
        echo "   GitHub Releases 下载失败，尝试备用方案..."
    fi
fi

# 方法2: 使用 Python + requests 下载
if command -v python3 &> /dev/null || command -v python &> /dev/null; then
    PYTHON=$(command -v python3 || command -v python)
    echo "[2/2] 正在通过 Python 下载模型..."
    echo "      这可能较慢，请耐心等待..."
    
    $PYTHON -c "
import requests, os, sys
url = '$MODEL_URL'
path = '$MODEL_PATH'
os.makedirs(os.path.dirname(path), exist_ok=True)
print(f'下载中: {url}')
response = requests.get(url, stream=True, timeout=300)
total = int(response.headers.get('content-length', 0))
block = 8192
with open(path, 'wb') as f:
    for chunk in response.iter_content(chunk_size=block):
        f.write(chunk)
if os.path.exists(path):
    size = os.path.getsize(path)
    print(f'✅ 下载成功! ({size/1024/1024:.1f} MB)')
else:
    print('❌ 下载失败')
    sys.exit(1)
"
    if [ $? -eq 0 ]; then
        exit 0
    fi
fi

echo ""
echo "============================================"
echo "所有自动下载方式均失败，请手动下载："
echo ""
echo "  1. 访问: https://github.com/kingdol666/TimeSyncDiag/releases"
echo "  2. 下载 best_resnet_model.keras"
echo "  3. 放入: $MODEL_DIR/"
echo "============================================"
exit 1
