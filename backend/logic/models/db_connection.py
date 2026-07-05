import logging
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# 使用绝对导入，避免与 state.py 的 path 注入方式产生相对导入层级冲突
from config.config_loader import config

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建基类
Base = declarative_base()


def build_db_url() -> str:
    """根据全局配置组装数据库连接 URL"""
    db = config.database
    return f"postgresql://{db.user}:{db.password}@{db.host}:{db.port}/{db.name}"


class DatabaseConnection:
    """
    TimescaleDB数据库连接类，用于管理数据库连接和会话
    """
    def __init__(self, db_url: str = None):
        """
        初始化数据库连接
        
        参数:
            db_url: 数据库连接URL，默认从全局配置读取
        """
        self.db_url = db_url or build_db_url()
        self.engine = None
        self.SessionLocal = None
        self.connected = False
    
    def connect(self):
        """
        建立数据库连接
        
        返回:
            bool: 连接是否成功
        """
        try:
            # 从全局配置读取连接池参数
            pool_size = config.database.pool_size
            max_overflow = config.database.max_overflow
            pool_recycle = config.database.pool_recycle
            
            # 创建数据库引擎
            self.engine = create_engine(
                self.db_url,
                pool_pre_ping=True,  # 连接池预检
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_recycle=pool_recycle,
                connect_args={"options": f"-c timezone={config.system.timezone}"}
            )
            
            # 创建会话工厂
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            
            # 测试连接并设置时区
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                conn.execute(text("SET TIME ZONE 'Asia/Shanghai'"))
                conn.commit()
            
            self.connected = True
            logger.info(f"成功连接到TimescaleDB: {self.db_url}")
            logger.info("数据库时区已设置为: Asia/Shanghai")
            
            # 创建所有表
            self.create_tables()
            
            return True
            
        except Exception as e:
            logger.error(f"连接TimescaleDB失败: {e}")
            self.connected = False
            return False
    
    def get_session(self) -> Session:
        """
        获取数据库会话
        
        返回:
            Session: 数据库会话对象
        
        异常:
            RuntimeError: 如果数据库未连接
        """
        if not self.connected:
            raise RuntimeError("数据库未连接，请先调用connect()方法")
        
        return self.SessionLocal()
    
    def create_tables(self):
        """
        创建所有数据表
        """
        try:
            # 导入所有模型类，确保它们在创建表之前被注册
            from .models import SensorData, DetectionDeviceData, ThicknessMap, ImageAnalysisResult
            
            # 创建所有表
            Base.metadata.create_all(bind=self.engine)
            
            # 检查是否需要为SensorData表添加TimescaleDB超表
            self._create_hypertable()
            
            logger.info("数据库表创建/更新成功")
            
        except Exception as e:
            logger.error(f"创建数据库表失败: {e}")
    
    def _create_hypertable(self):
        """
        为传感器数据表创建TimescaleDB超表
        """
        try:
            with self.engine.connect() as conn:
                # 检查是否已经是超表
                result = conn.execute(
                    text("SELECT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'sensor_data')")
                )
                is_hypertable = result.scalar()
                
                if not is_hypertable:
                    # 创建超表
                    conn.execute(
                        text("SELECT create_hypertable('sensor_data', 'timestamp')")
                    )
                    conn.commit()
                    logger.info("成功为sensor_data表创建TimescaleDB超表")
                else:
                    logger.info("sensor_data表已经是TimescaleDB超表")
                    
        except Exception as e:
            # 如果TimescaleDB扩展未安装，只记录警告，不影响程序运行
            if "timescaledb_information" in str(e) or "does not exist" in str(e):
                logger.warning("TimescaleDB扩展未安装，sensor_data表将作为普通表使用")
            else:
                logger.warning(f"创建TimescaleDB超表失败: {e}")
    
    def close(self):
        """
        关闭数据库连接
        """
        if self.engine:
            try:
                self.engine.dispose()
                self.connected = False
                logger.info("数据库连接已关闭")
            except Exception as e:
                logger.error(f"关闭数据库连接时发生错误: {e}")
    
    def __enter__(self):
        """
        上下文管理器入口，用于with语句
        """
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        上下文管理器出口，用于with语句
        """
        self.close()

# 创建全局数据库连接实例
db = DatabaseConnection()

if __name__ == "__main__":
    # 测试数据库连接
    try:
        db.connect()
        logger.info("数据库连接测试成功")
    finally:
        db.close()