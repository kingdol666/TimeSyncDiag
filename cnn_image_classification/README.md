# CNN图像分类模型

本项目使用ResNet50卷积神经网络对图像数据进行二分类，区分正常(0)和异常(1)图像。

## 项目结构

```
cnn_image_classification/
├── cnn_image_classifier.py       # 模型训练脚本
├── cnnfast.py                     # FastAPI 服务 (port 8001)
├── predict_image.py               # 图像预测脚本
├── best_resnet_model.keras        # 训练好的模型（需下载，~128MB）
├── training_history.png           # 训练历史曲线（自动生成）
├── evaluation_results.txt         # 模型评估结果（自动生成）
├── download_model.sh              # 模型下载脚本 (bash)
├── download_model.py              # 模型下载脚本 (Python)
├── requirements.txt               # 依赖项列表
└── README.md                      # 项目说明文档
```

## 数据准备

数据应按照以下结构组织：

```
imageData/
├── 0/  # 正常图像文件夹
│   ├── image1.png
│   ├── image2.png
│   └── ...
└── 1/  # 异常图像文件夹
    ├── image1.png
    ├── image2.png
    └── ...
```

## 环境要求

- Python 3.8+
- TensorFlow 2.8+
- NumPy
- Matplotlib
- scikit-learn

## 安装依赖

```bash
pip install -r requirements.txt
```

## 获取模型权重

训练好的模型文件 `best_resnet_model.keras`（~128MB）因体积过大未包含在 git 仓库中。

### 方式一：自动下载（推荐）

```bash
# Bash
bash cnn_image_classification/download_model.sh

# Python (跨平台)
uv run python cnn_image_classification/download_model.py
```

### 方式二：GitHub Releases

1. 访问 https://github.com/kingdol666/TimeSyncDiag/releases
2. 下载 `best_resnet_model.keras`
3. 放入 `cnn_image_classification/` 目录

### 方式三：自行训练

```bash
uv run python cnn_image_classification/cnn_image_classifier.py
```

需要自行准备训练数据。

## 使用说明

### 1. 训练模型

运行训练脚本：

```bash
python cnn_image_classifier.py
```

训练过程包括：
- 创建数据生成器（训练集和验证集）
- 构建ResNet50模型（冻结预训练层）
- 训练模型（20个epoch）
- 解冻顶层网络进行微调
- 再次训练模型
- 评估模型性能
- 保存最佳模型

### 2. 预测图像

运行预测脚本：

```bash
python predict_image.py
```

根据提示选择操作：
- 1: 预测单个图像
- 2: 预测文件夹中所有图像
- 3: 退出

## 模型说明

- **基础模型**: ResNet50（预训练于ImageNet数据集）
- **输入尺寸**: 224x224x3
- **分类类型**: 二分类（正常/异常）
- **损失函数**: 二元交叉熵
- **优化器**: Adam
- **学习率**: 1e-4（微调时为1e-5）

## 结果输出

- **模型文件**: `best_resnet_model.keras`
- **训练历史**: `training_history.png`
- **评估结果**: `evaluation_results.txt`（包含准确率、混淆矩阵、分类报告）

## 注意事项

1. 首次训练时，会自动下载ResNet50预训练权重，需要网络连接
2. 训练时间取决于硬件配置，建议使用GPU加速
3. 可以通过修改脚本中的参数调整训练配置
4. 预测时需要确保模型文件存在

## 调整参数

可以在`cnn_image_classifier.py`中调整以下参数：

- `IMG_HEIGHT, IMG_WIDTH`: 图像尺寸
- `BATCH_SIZE`: 批次大小
- `EPOCHS`: 训练轮数
- `LEARNING_RATE`: 学习率

## 示例输出

### 训练输出
```
=== CNN图像分类模型训练 ===
数据路径: e:\codes\wanweiData2\data\data\2hour_interval_heatmaps\imageData
模型保存路径: e:\codes\wanweiData2\cnn_image_classification\best_resnet_model.keras
图像尺寸: 224x224
批次大小: 32
训练轮数: 20
学习率: 0.0001

1. 创建数据生成器...
训练集样本数: 1200
验证集样本数: 300
类别映射: {'0': 0, '1': 1}

2. 构建ResNet50模型...
Model: "model"
...

3. 开始训练模型（冻结预训练层）...
Epoch 1/20
...
```

### 预测输出
```
=== CNN图像分类预测工具 ===
成功加载模型: e:\codes\wanweiData2\cnn_image_classification\best_resnet_model.keras

请选择操作:
1. 预测单个图像
2. 预测文件夹中所有图像
3. 退出
请输入选择 (1-3): 1
请输入图像文件路径: test_image.png

预测结果:
文件: test_image.png
预测类别: 正常(0)
置信度: 0.9876
```
