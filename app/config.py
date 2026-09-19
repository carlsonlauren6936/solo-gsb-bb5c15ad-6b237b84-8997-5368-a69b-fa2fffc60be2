"""应用配置。"""
import os
from pathlib import Path

# 数据库文件路径（容器内默认 /data/handoff.db，可用环境变量覆盖）
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DB_PATH = Path(os.getenv("DB_PATH", str(DATA_DIR / "handoff.db")))

# 服务监听
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
