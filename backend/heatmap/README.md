# 膜厚热力图绘制工具

## 功能说明

本工具专门用于绘制薄膜加工产线的两小时间隔膜厚热力图，支持两种模式：

### 1. 两小时间隔热力图（按正常/异常分类）
- 将数据按两小时间隔分割
- 根据膜厚均值判断是否异常（目标值0.045μm，容差±0.00225μm）
- 正常热力图保存在子文件夹`0`中
- 异常热力图保存在子文件夹`1`中
- 支持鼠标悬停查看详细信息

### 2. 带坐标数值信息的两小时热力图
- 生成带有详细坐标信息的热力图
- 鼠标悬停时显示：
  - 检测点位置
  - 具体时间
  - 厚度值
  - 与目标值的偏差
  - 数据统计信息

## 使用方法

### 基本使用

```bash
python creatmap.py
```

运行后会提示选择操作模式：
- `1`: 生成两小时间隔热力图（按正常/异常分类）
- `2`: 生成带坐标数值信息的两小时热力图
- `3`: 同时生成两种热力图

### 代码中使用

```python
from heatmap.creatmap import HeatmapGenerator

# 创建热力图生成器
generator = HeatmapGenerator(data_dir="e:\\codes\\wanweiData2\\data\\realtime")

# 加载数据
generator.load_data()

# 生成两小时间隔热力图
generator.plot_2hour_interval_heatmaps(
    output_dir="e:\\codes\\wanweiData2\\output\\2hour_heatmaps"
)

# 生成带坐标数值信息的两小时热力图
generator.plot_2hour_heatmap_with_coordinates(
    output_dir="e:\\codes\\wanweiData2\\output\\2hour_heatmaps_with_coords",
    show_plot=False  # 是否显示图表
)
```

## 输出说明

### 文件结构

```
2hour_interval_heatmaps/
├── trip_transposed_10_20251014_155657_merged/
│   ├── 0/  # 正常热力图
│   │   ├── trip_transposed_10_20251014_155657_merged_interval_01_20251014_0800-1000.png
│   │   └── ...
│   └── 1/  # 异常热力图
│       └── ...
└── trip_transposed_11_20251014_155702_merged/
    ├── 0/
    └── 1/
```

### 文件命名规则

- **两小时间隔热力图**: `[文件名]_interval_[序号]_[开始时间]-[结束时间].png`
- **带坐标信息热力图**: `[文件名]_2hour_[序号]_[开始时间]-[结束时间]_with_coords.png`

时间格式：`YYYYMMDD_HHMM`

## 参数说明

### HeatmapGenerator类

#### 初始化参数
- `data_dir`: transposed数据文件夹路径

#### load_data()
- 加载所有transposed CSV文件
- 只取一半列（探头单程检测）
- 自动清理NaN数据

#### plot_2hour_interval_heatmaps()
- `output_dir`: 输出目录（默认：`data/2hour_interval_heatmaps`）
- 返回：字典（文件名 -> 生成的文件路径列表）

#### plot_2hour_heatmap_with_coordinates()
- `output_dir`: 输出目录（默认：`data/2hour_heatmaps_with_coords`）
- `show_plot`: 是否显示图表（默认：False）
- 返回：字典（文件名 -> 生成的文件路径列表）

## 数据要求

- 数据文件格式：`trip_transposed_*_merged.csv`
- 数据列结构：
  - 第一列：`time`（时间戳）
  - 后续列：检测点数据（探头往返数据，只取前一半）

## 异常判断标准

- 目标厚度：0.045 μm
- 容差范围：±0.00225 μm（5%）
- 异常条件：`abs(均值 - 目标值) > 容差`

## 可视化特性

### 热力图设置
- 颜色映射：`turbo`
- 插值方式：`nearest`
- 分辨率：300 DPI
- 颜色范围：0.04 - 0.05 μm

### 交互功能
- 鼠标悬停显示详细信息
- 网格线辅助定位
- 清晰的坐标轴标签
- 数据统计信息展示

## 依赖项

```txt
pandas
numpy
matplotlib
```

## 注意事项

1. 确保数据文件路径正确
2. 输出目录会自动创建
3. 大量数据时可能需要较长时间
4. 建议定期清理生成的图片文件
5. 内存占用较高，建议分批处理大量文件

## 示例输出

```
正在加载数据文件...
找到 2 个数据文件
正在处理文件: trip_transposed_10_20251014_155657_merged.csv
  - 数据形状: (5000, 50)
  - 时间范围: 2025-10-14 08:00:00 到 2025-10-14 18:00:00
  - 检测点数: 49 个点位

开始生成两小时间隔热力图...
============================================================

正在处理文件: trip_transposed_10_20251014_155657_merged.csv
  文件输出目录: e:\codes\wanweiData2\data\2hour_interval_heatmaps\trip_transposed_10_20251014_155657_merged
  正常云图保存到: e:\codes\wanweiData2\data\2hour_interval_heatmaps\trip_transposed_10_20251014_155657_merged\0
  异常云图保存到: e:\codes\wanweiData2\data\2hour_interval_heatmaps\trip_transposed_10_20251014_155657_merged\1
  数据时间范围: 2025-10-14 08:00:00 到 2025-10-14 18:00:00
  分割为 5 个两小时间隔
    间隔 1: 2025-10-14 08:00 至 10:00 -> trip_transposed_10_20251014_155657_merged_interval_01_20251014_0800-1000.png (均值: 0.0451, 状态: 正常)
    ...
```

## 更新日志

### v1.0.0 (2025-12-25)
- 初始版本
- 实现两小时间隔热力图绘制功能
- 支持正常/异常分类
- 支持鼠标悬停查看详细信息
