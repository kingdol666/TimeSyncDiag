import os
import random
import sys
import numpy as np
import tensorflow as tf
from tensorflow.keras.preprocessing import image
import matplotlib.pyplot as plt

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# 模型路径
MODEL_PATH = r'e:\codes\wanweiData2\cnn_image_classification\best_resnet_model.keras'

# 图像尺寸
IMG_HEIGHT, IMG_WIDTH = 224, 224

# 数据路径
DATA_PATH = r'e:\codes\wanweiData2\data\data\2hour_interval_heatmaps\imageData'


def load_model():
    """
    加载训练好的模型
    """
    try:
        model = tf.keras.models.load_model(MODEL_PATH)
        print(f"成功加载模型: {MODEL_PATH}")
        return model
    except Exception as e:
        print(f"加载模型失败: {e}")
        return None


def preprocess_image(img_path):
    """
    预处理图像以适应模型输入
    """
    # 加载图像
    img = image.load_img(img_path, target_size=(IMG_HEIGHT, IMG_WIDTH))
    
    # 转换为数组
    img_array = image.img_to_array(img)
    
    # 扩展维度以匹配模型输入形状 (1, 224, 224, 3)
    img_array = np.expand_dims(img_array, axis=0)
    
    # 归一化
    img_array = img_array / 255.0
    
    return img_array


def predict_image(model, img_path):
    """
    使用模型预测图像类别
    """
    # 预处理图像
    img_array = preprocess_image(img_path)
    
    # 预测
    prediction = model.predict(img_array, verbose=0)
    
    # 转换为类别
    class_idx = int(prediction[0] > 0.5)
    confidence = float(prediction[0]) if class_idx == 1 else float(1 - prediction[0])
    
    # 类别名称
    class_names = {0: '正常(0)', 1: '异常(1)'}
    
    return class_names[class_idx], confidence


def get_random_images(num_images=3):
    """
    从0和1文件夹中随机抽取图片
    """
    random_images = []
    
    # 从正常(0)文件夹中随机抽取
    normal_folder = os.path.join(DATA_PATH, '0')
    normal_images = [f for f in os.listdir(normal_folder) if f.endswith('.png')]
    random_normal = random.sample(normal_images, min(num_images, len(normal_images)))
    for img in random_normal:
        random_images.append((os.path.join(normal_folder, img), 0))
    
    # 从异常(1)文件夹中随机抽取
    abnormal_folder = os.path.join(DATA_PATH, '1')
    abnormal_images = [f for f in os.listdir(abnormal_folder) if f.endswith('.png')]
    random_abnormal = random.sample(abnormal_images, min(num_images, len(abnormal_images)))
    for img in random_abnormal:
        random_images.append((os.path.join(abnormal_folder, img), 1))
    
    # 打乱顺序
    random.shuffle(random_images)
    
    return random_images


def visualize_predictions(random_images, model):
    """
    可视化预测结果
    """
    # 计算网格大小
    num_images = len(random_images)
    rows = (num_images + 1) // 2  # 每行2张图像
    cols = 2
    
    # 创建画布
    fig, axes = plt.subplots(rows, cols, figsize=(15, rows * 7))
    axes = axes.flatten()  # 转换为一维数组，方便遍历
    
    # 预测每张图像并绘制
    correct_count = 0
    for i, (img_path, true_label) in enumerate(random_images):
        # 加载原始图像
        img = image.load_img(img_path, target_size=(IMG_HEIGHT, IMG_WIDTH))
        
        # 预测
        predicted_class, confidence = predict_image(model, img_path)
        
        # 计算是否预测正确
        predicted_label = 0 if predicted_class == '正常(0)' else 1
        is_correct = predicted_label == true_label
        if is_correct:
            correct_count += 1
        
        # 绘制图像
        axes[i].imshow(img)
        axes[i].axis('off')
        
        # 设置标题
        true_label_str = '正常(0)' if true_label == 0 else '异常(1)'
        title_color = 'green' if is_correct else 'red'
        axes[i].set_title(
            f"文件名: {os.path.basename(img_path)}\n" +
            f"真实标签: {true_label_str}\n" +
            f"预测结果: {predicted_class}\n" +
            f"置信度: {confidence:.4f}",
            color=title_color, fontsize=10
        )
    
    # 隐藏多余的子图
    for i in range(num_images, len(axes)):
        axes[i].axis('off')
    
    # 调整布局
    plt.tight_layout()
    
    # 保存图像
    output_path = r'e:\codes\wanweiData2\cnn_image_classification\prediction_visualization.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\n可视化结果已保存到: {output_path}")
    
    # 显示图像
    plt.show()
    plt.close()
    
    return correct_count, num_images

def main():
    """
    主函数
    """
    print("=== 随机测试图像分类 ===")
    
    # 加载模型
    model = load_model()
    if model is None:
        return
    
    # 获取随机图像
    random_images = get_random_images(num_images=5)  # 6张图像，3行2列
    
    print(f"\n随机抽取了 {len(random_images)} 张图像进行测试:")
    print("-" * 80)
    
    # 可视化预测结果
    correct_count, num_images = visualize_predictions(random_images, model)
    
    # 输出总体结果
    accuracy = correct_count / num_images
    print(f"\n测试结果总结:")
    print(f"测试图像总数: {num_images}")
    print(f"正确预测数: {correct_count}")
    print(f"预测准确率: {accuracy:.4f}")


if __name__ == "__main__":
    main()
