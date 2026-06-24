import os
import numpy as np
import tensorflow as tf
from tensorflow.keras.preprocessing.image import load_img, img_to_array
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from tensorflow.keras.applications import ResNet50
from tensorflow.keras.layers import Dense, GlobalAveragePooling2D
from tensorflow.keras.models import Model
from tensorflow.keras.optimizers import Adam
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split

# 设置随机种子以保证结果可复现
tf.random.set_seed(42)
np.random.seed(42)

# 数据路径
DATA_PATH = r'e:\codes\wanweiData2\data\data\2hour_interval_heatmaps\imageData'

# 模型保存路径
MODEL_SAVE_PATH = r'e:\codes\wanweiData2\cnn_image_classification\best_resnet_model.keras'

# 训练参数
IMG_HEIGHT, IMG_WIDTH = 224, 224  # ResNet50输入尺寸
BATCH_SIZE = 32
EPOCHS = 20
LEARNING_RATE = 1e-4


def load_images_and_labels():
    """
    手动加载所有图片和标签
    """
    images = []
    labels = []
    file_paths = []
    
    # 遍历两个类别文件夹
    for class_label in ['0', '1']:
        class_path = os.path.join(DATA_PATH, class_label)
        
        # 遍历文件夹中的所有图片文件
        for filename in os.listdir(class_path):
            if filename.lower().endswith('.png'):
                # 构建完整的文件路径
                img_path = os.path.join(class_path, filename)
                
                # 加载并预处理图片
                img = load_img(img_path, target_size=(IMG_HEIGHT, IMG_WIDTH))
                img_array = img_to_array(img)
                
                # 添加到列表
                images.append(img_array)
                labels.append(int(class_label))
                file_paths.append(img_path)
    
    # 转换为numpy数组
    images = np.array(images)
    labels = np.array(labels)
    
    return images, labels, file_paths


def create_data_generators():
    """
    创建训练集和验证集的数据生成器
    """
    # 加载所有图片和标签
    images, labels, _ = load_images_and_labels()
    
    # 划分训练集和验证集
    x_train, x_val, y_train, y_val = train_test_split(
        images, labels, test_size=0.2, random_state=42, stratify=labels
    )
    
    # 数据增强配置
    train_datagen = ImageDataGenerator(
        rescale=1./255,
        rotation_range=20,
        width_shift_range=0.2,
        height_shift_range=0.2,
        shear_range=0.2,
        zoom_range=0.2,
        horizontal_flip=True,
        vertical_flip=True
    )

    # 验证集数据生成器（仅 rescale）
    val_datagen = ImageDataGenerator(
        rescale=1./255
    )

    # 训练集生成器
    train_generator = train_datagen.flow(
        x_train,
        y_train,
        batch_size=BATCH_SIZE,
        shuffle=True
    )

    # 验证集生成器
    val_generator = val_datagen.flow(
        x_val,
        y_val,
        batch_size=BATCH_SIZE,
        shuffle=False
    )

    return train_generator, val_generator, x_val, y_val


def build_simple_cnn_model():
    """
    构建简单的CNN模型用于二分类
    """
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import Conv2D, MaxPooling2D, Flatten, Dense, Dropout
    
    # 创建序贯模型
    model = Sequential()
    
    # 卷积层1
    model.add(Conv2D(32, (3, 3), activation='relu', input_shape=(IMG_HEIGHT, IMG_WIDTH, 3)))
    model.add(MaxPooling2D((2, 2)))
    
    # 卷积层2
    model.add(Conv2D(64, (3, 3), activation='relu'))
    model.add(MaxPooling2D((2, 2)))
    
    # 卷积层3
    model.add(Conv2D(128, (3, 3), activation='relu'))
    model.add(MaxPooling2D((2, 2)))
    
    # 展平层
    model.add(Flatten())
    
    # 全连接层1
    model.add(Dense(128, activation='relu'))
    model.add(Dropout(0.5))  # Dropout层防止过拟合
    
    # 输出层
    model.add(Dense(1, activation='sigmoid'))  # 二分类使用sigmoid激活
    
    # 编译模型
    model.compile(
        optimizer=Adam(learning_rate=LEARNING_RATE),
        loss='binary_crossentropy',
        metrics=['accuracy']
    )
    
    return model


def unfreeze_top_layers(model):
    """
    简单CNN不需要解冻，直接返回模型
    """
    return model


def train_model(model, train_generator, val_generator):
    """
    训练模型
    """
    # 回调函数
    callbacks = [
        tf.keras.callbacks.ModelCheckpoint(
            filepath=MODEL_SAVE_PATH,
            monitor='val_accuracy',
            save_best_only=True,
            mode='max',
            verbose=1
        ),
        tf.keras.callbacks.EarlyStopping(
            monitor='val_loss',
            patience=5,
            restore_best_weights=True,
            verbose=1
        ),
        tf.keras.callbacks.ReduceLROnPlateau(
            monitor='val_loss',
            factor=0.2,
            patience=3,
            min_lr=1e-6,
            verbose=1
        )
    ]

    # 训练模型
    history = model.fit(
        train_generator,
        steps_per_epoch=train_generator.n // BATCH_SIZE,
        validation_data=val_generator,
        validation_steps=val_generator.n // BATCH_SIZE,
        epochs=EPOCHS,
        callbacks=callbacks,
        verbose=1
    )

    return history


def plot_training_history(history):
    """
    绘制训练历史
    """
    acc = history.history['accuracy']
    val_acc = history.history['val_accuracy']
    loss = history.history['loss']
    val_loss = history.history['val_loss']

    epochs_range = range(len(acc))

    plt.figure(figsize=(12, 8))

    # 绘制准确率曲线
    plt.subplot(1, 2, 1)
    plt.plot(epochs_range, acc, label='Training Accuracy')
    plt.plot(epochs_range, val_acc, label='Validation Accuracy')
    plt.legend(loc='lower right')
    plt.title('Training and Validation Accuracy')

    # 绘制损失曲线
    plt.subplot(1, 2, 2)
    plt.plot(epochs_range, loss, label='Training Loss')
    plt.plot(epochs_range, val_loss, label='Validation Loss')
    plt.legend(loc='upper right')
    plt.title('Training and Validation Loss')

    plt.savefig(r'e:\codes\wanweiData2\cnn_image_classification\training_history.png')
    plt.close()


def evaluate_model(model, val_generator, y_val):
    """
    评估模型性能
    """
    # 加载最佳模型
    best_model = tf.keras.models.load_model(MODEL_SAVE_PATH)

    # 在验证集上评估
    loss, accuracy = best_model.evaluate(val_generator, verbose=1)
    print(f"\nValidation Loss: {loss:.4f}")
    print(f"Validation Accuracy: {accuracy:.4f}")

    # 生成预测结果
    y_pred = best_model.predict(val_generator, verbose=1)
    y_pred = (y_pred > 0.5).astype(int)
    y_true = y_val

    # 计算混淆矩阵
    from sklearn.metrics import confusion_matrix, classification_report
    cm = confusion_matrix(y_true, y_pred)
    report = classification_report(y_true, y_pred, target_names=['正常(0)', '异常(1)'], zero_division=1)

    print("\nConfusion Matrix:")
    print(cm)
    print("\nClassification Report:")
    print(report)

    # 保存评估结果到文件
    with open(r'e:\codes\wanweiData2\cnn_image_classification\evaluation_results.txt', 'w') as f:
        f.write(f"Validation Loss: {loss:.4f}\n")
        f.write(f"Validation Accuracy: {accuracy:.4f}\n\n")
        f.write("Confusion Matrix:\n")
        f.write(str(cm) + "\n\n")
        f.write("Classification Report:\n")
        f.write(report)


def main():
    """
    主函数
    """
    print("=== CNN图像分类模型训练 ===")
    print(f"数据路径: {DATA_PATH}")
    print(f"模型保存路径: {MODEL_SAVE_PATH}")
    print(f"图像尺寸: {IMG_HEIGHT}x{IMG_WIDTH}")
    print(f"批次大小: {BATCH_SIZE}")
    print(f"训练轮数: {EPOCHS}")
    print(f"学习率: {LEARNING_RATE}")
    print("\n")

    # 创建数据生成器
    print("1. 加载图片和创建数据生成器...")
    train_generator, val_generator, x_val, y_val = create_data_generators()
    print(f"训练集样本数: {train_generator.n}")
    print(f"验证集样本数: {val_generator.n}")
    print(f"正常样本数: {sum(y_val == 0)}")
    print(f"异常样本数: {sum(y_val == 1)}")
    print("\n")

    # 构建模型
    print("2. 构建简单CNN模型...")
    model = build_simple_cnn_model()
    model.summary()
    print("\n")

    # 训练模型（冻结层）
    print("3. 开始训练模型（冻结预训练层）...")
    history = train_model(model, train_generator, val_generator)
    print("\n")

    # 绘制训练历史
    print("4. 绘制训练历史...")
    plot_training_history(history)
    print("\n")

    # 解冻顶层进行微调
    print("5. 解冻顶层网络，进行微调...")
    model = unfreeze_top_layers(model)
    print("\n")

    # 继续训练（微调）
    print("6. 开始微调模型...")
    history_fine = train_model(model, train_generator, val_generator)
    print("\n")

    # 评估模型
    print("7. 评估模型性能...")
    evaluate_model(model, val_generator, y_val)
    print("\n")

    print("=== 训练完成 ===")


if __name__ == "__main__":
    main()
