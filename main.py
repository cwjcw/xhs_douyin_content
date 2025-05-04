import sys
import os
from pathlib import Path

# 手动注入项目根目录到 sys.path
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
    print(f"✅ 添加项目根路径: {project_root}")
else:
    print(f"✅ 项目根路径已存在: {project_root}")

import logging
from datetime import datetime
# from spiders.douyin import Douyin
from spiders.xhs import Xhs

# ========== 日志配置 ==========
log_dir = "logs"
log_file = f"{log_dir}/run_{datetime.now().strftime('%Y-%m-%d')}.log"

import os
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# ========== 主程序入口 ==========
if __name__ == "__main__":
    logging.info("📦 程序启动")

    # try:
    #     logging.info("▶ 开始处理 Douyin 数据")
    #     Douyin.run_all()
    #     logging.info("✅ Douyin 处理完成")
    # except Exception as e:
    #     logging.error(f"❌ Douyin 出错: {e}")

    try:
        logging.info("▶ 开始处理 XHS 数据")
        Xhs.run_all()
        logging.info("✅ XHS 处理完成")
    except Exception as e:
        logging.error(f"❌ XHS 出错: {e}")

    logging.info("🏁 程序结束")
