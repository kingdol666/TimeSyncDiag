import os
import tensorflow as tf
from tensorflow.keras.preprocessing import image
import numpy as np

# 模型路径
MODEL_PATH = r'e:\codes\wanweiData2\cnn_image_classification\best_resnet_model.keras'

# 图像尺寸
IMG_HEIGHT, IMG_WIDTH = 224, 224


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


def predict_folder(model, folder_path):
    """
    预测文件夹中所有图像
    """
    results = []
    
    # 获取文件夹中所有图像文件
    for filename in os.listdir(folder_path):
        if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif')):
            img_path = os.path.join(folder_path, filename)
            
            # 预测
            predicted_class, confidence = predict_image(model, img_path)
            
            # 保存结果
            results.append({
                'filename': filename,
                'predicted_class': predicted_class,
                'confidence': confidence
            })
            
            print(f"文件: {filename} -> 预测: {predicted_class}, 置信度: {confidence:.4f}")
    
    return results


def main():
    """
    主函数
    """
    print("=== CNN图像分类预测工具 ===")
    
    # 加载模型
    model = load_model()
    if model is None:
        return
    
    while True:
        print("\n请选择操作:")
        print("1. 预测单个图像")
        print("2. 预测文件夹中所有图像")
        print("3. 退出")
        
        choice = input("请输入选择 (1-3): ")
        
        if choice == '1':
            # 预测单个图像
            img_path = input("请输入图像文件路径: ")
            if os.path.exists(img_path):
                predicted_class, confidence = predict_image(model, img_path)
                print(f"\n预测结果:")
                print(f"文件: {img_path}")
                print(f"预测类别: {predicted_class}")
                print(f"置信度: {confidence:.4f}")
            else:
                print(f"错误: 文件不存在: {img_path}")
        
        elif choice == '2':
            # 预测文件夹中所有图像
            folder_path = input("请输入文件夹路径: ")
            if os.path.exists(folder_path) and os.path.isdir(folder_path):
                print(f"\n正在预测文件夹: {folder_path} 中的所有图像...")
                results = predict_folder(model, folder_path)
                
                # 统计结果
                normal_count = sum(1 for r in results if r['predicted_class'] == '正常(0)')
                abnormal_count = sum(1 for r in results if r['predicted_class'] == '异常(1)')
                total_count = len(results)
                
                print(f"\n预测统计:")
                print(f"总图像数: {total_count}")
                print(f"正常图像数: {normal_count}")
                print(f"异常图像数: {abnormal_count}")
                print(f"异常比例: {abnormal_count / total_count:.2%}")
            else:
                print(f"错误: 文件夹不存在: {folder_path}")
        
        elif choice == '3':
            # 退出
            print("退出程序...")
            break
        
        else:
            print("错误: 无效的选择，请重新输入")


if __name__ == "__main__":
    main()
