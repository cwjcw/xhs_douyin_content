import jdy
import asyncio
from data_processing.video_base import DailyDataProcessor
from spiders import douyin

dy_spider = douyin.Douyin("https://creator.douyin.com/creator-micro/home")
dy_spider.run()

processor = DailyDataProcessor()
daily_data = processor.get_daily_data()
processor.update_yesterday_data()


jdy = jdy.JDY()

appid, entryid = "67c280b7c6387c4f4afd50ae", "67c69341ea7d25979a4d9e8b"

# 调用并发库asyncio执行批量上传
asyncio.run(jdy.batch_create(app_id=appid, entry_id=entryid, source_data=daily_data))












