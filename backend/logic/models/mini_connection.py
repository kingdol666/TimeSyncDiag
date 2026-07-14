from minio import Minio
from minio.error import S3Error
import os
import io
import logging

logger = logging.getLogger(__name__)

class MinioConnector:
    """MinIO Python连接工具类（Connector）"""
    def __init__(self, endpoint, access_key, secret_key, secure=False):
        """
        初始化MinIO连接
        :param endpoint: MinIO服务地址（格式：IP:端口，如 127.0.0.1:9000）
        :param access_key: MinIO管理员用户名（对应 MINIO_ROOT_USER）
        :param secret_key: MinIO管理员密码（对应 MINIO_ROOT_PASSWORD）
        :param secure: 是否使用HTTPS连接（默认False，开发环境用HTTP；生产环境建议True）
        """
        # 创建MinIO客户端实例（核心连接对象）
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )

    def bucket_exists(self, bucket_name):
        """检查存储桶是否存在"""
        try:
            return self.client.bucket_exists(bucket_name)
        except S3Error as e:
            logger.error(f"检查存储桶失败：{e}")
            return False

    def create_bucket(self, bucket_name):
        """创建存储桶（若不存在则创建）"""
        try:
            if not self.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"存储桶 {bucket_name} 创建成功")
            else:
                logger.info(f"存储桶 {bucket_name} 已存在")
        except S3Error as e:
            logger.error(f"创建存储桶失败：{e}")

    def upload_file(self, bucket_name, object_name, file_path):
        """
        上传本地文件到MinIO
        :param bucket_name: 存储桶名称
        :param object_name: 上传后在MinIO中的对象名称（可带路径，如 test/avatar.jpg）
        :param file_path: 本地文件路径（如 D:/test/xxx.jpg 或 /tmp/xxx.jpg）
        """
        try:
            # 检查文件是否存在
            if not os.path.exists(file_path):
                logger.error(f"本地文件 {file_path} 不存在")
                return False
            # 上传文件（自动识别文件类型）
            self.client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=file_path
            )
            logger.info(f"文件 {file_path} 上传成功，对象名称：{object_name}")
            return True
        except S3Error as e:
            logger.error(f"上传文件失败：{e}")
            return False

    def download_file(self, bucket_name, object_name, save_path):
        """
        从MinIO下载文件到本地
        :param bucket_name: 存储桶名称
        :param object_name: MinIO中的对象名称（如 test/avatar.jpg）
        :param save_path: 本地保存路径（如 D:/download/xxx.jpg 或 /tmp/xxx.jpg）
        """
        try:
            # 下载文件
            self.client.fget_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=save_path
            )
            logger.info(f"对象 {object_name} 下载成功，保存路径：{save_path}")
            return True
        except S3Error as e:
            logger.error(f"下载文件失败：{e}")
            return False

    def download_file_to_bytes(self, bucket_name, object_name):
        """
        从MinIO下载文件到内存（返回字节流）
        :param bucket_name: 存储桶名称
        :param object_name: MinIO中的对象名称（如 test/avatar.jpg）
        :return: 字节流数据，失败返回None
        """
        try:
            # 下载文件到内存
            response = self.client.get_object(
                bucket_name=bucket_name,
                object_name=object_name
            )
            # 读取数据到内存
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            logger.error(f"下载文件到内存失败：{e}")
            return None

    def list_objects(self, bucket_name, prefix=""):
        """
        列出存储桶中的对象
        :param bucket_name: 存储桶名称
        :param prefix: 过滤前缀（如 test/ 可列出test目录下的所有对象）
        :return: 对象列表
        """
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
            object_list = []
            for obj in objects:
                object_info = {
                    "name": obj.object_name,
                    "size": obj.size,
                    "last_modified": obj.last_modified
                }
                object_list.append(object_info)
                logger.info(f"对象名称：{obj.object_name}，大小：{obj.size} 字节，最后修改时间：{obj.last_modified}")
            return object_list
        except S3Error as e:
            logger.error(f"列出对象失败：{e}")
            return []

if __name__ == "__main__":
    from backend.config.config_loader import config

    # 从配置文件读取MinIO连接参数
    MINIO_ENDPOINT = config.minio.endpoint
    MINIO_ACCESS_KEY = config.minio.access_key
    MINIO_SECRET_KEY = config.minio.secret_key
    BUCKET_NAME = config.minio.bucket_name

    # 1. 创建MinIO连接实例
    minio_connector = MinioConnector(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=config.minio.secure
    )

    # 2. 创建存储桶
    minio_connector.create_bucket(BUCKET_NAME)

    # 3. 上传本地文件（使用项目路径工具获取图片路径）
    from backend.logic.utils.paths import get_images_dir
    local_file_path = str(get_images_dir() / "1.png")
    minio_object_name = "test/1.png"
    minio_connector.upload_file(BUCKET_NAME, minio_object_name, local_file_path)

    # 4. 列出存储桶中的所有对象
    logger.info("存储桶中的对象列表：")
    minio_connector.list_objects(BUCKET_NAME)

    # 5. 下载MinIO对象到本地
    save_local_path = "./download/1.png"
    minio_connector.download_file(BUCKET_NAME, minio_object_name, save_local_path)