import jdy
import os, sys
from datetime import datetime, timedelta
import asyncio
import aiohttp

# 获取当前脚本所在目录 (data_processing目录)
current_dir = os.path.dirname(os.path.abspath(__file__))

# 获取项目根目录（即当前目录的上一级）
project_root = os.path.abspath(os.path.join(current_dir, ".."))

# 将项目根目录添加到sys.path中
if project_root not in sys.path:
    sys.path.append(project_root)

from data_processing.video_base import DailyDataProcessor

processor = DailyDataProcessor()
daily_data = processor.get_daily_data()

jdy = jdy.JDY()

appid, entryid = "67c280b7c6387c4f4afd50ae", "67c69341ea7d25979a4d9e8b"

# # 设置事件循环策略（在windows环境下必须添加）
# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# # 调用并发库asyncio执行批量上传
asyncio.run(jdy.batch_create(app_id=appid, entry_id=entryid, source_data=daily_data))










