import jdy
import asyncio
from data_processing.video_analysis import DailyDataProcessor
from spiders import douyin
from data_processing import dividend_processor as dy_bjlp

# 创建简道云对象
jdy = jdy.JDY()

# 创建浏览器对象，并下载data.xlsx
dy_spider = douyin.Douyin("https://creator.douyin.com/creator-micro/home")
dy_spider.run()

#############################################################################################
# 获取新增的视频质量数量，包括播放，点赞，收藏，评论，分享，收藏等
processor = DailyDataProcessor()
daily_data = processor.get_daily_data()
print(daily_data.head)
daily_data.to_excel(r'C:\Users\Administrator\Desktop\临时文件\daily.xlsx',index=False)
# 上传当天的视频质量数据
appid, entryid = "67c280b7c6387c4f4afd50ae", "67c69341ea7d25979a4d9e8b"
# 调用并发库asyncio执行批量上传
asyncio.run(jdy.batch_create(app_id=appid, entry_id=entryid, source_data=daily_data))

#############################################################################################
# 上传当天的人员分红
appId1 = "67c280b7c6387c4f4afd50ae"
entryId1 = "67d7097d08e5f607c4cfd028"
# 调用并发库asyncio执行批量上传
dy_bjlp_1 = dy_bjlp.Dividend()
people_money = dy_bjlp_1.everyone_money()
asyncio.run(jdy.batch_create(app_id=appId1, entry_id=entryId1, source_data=people_money))


#############################################################################################
# 上传当天的视频分红
appId2 ="67c280b7c6387c4f4afd50ae"
entryId2 = "67d7ead7492e5041b57fb891"
# 调用并发库asyncio执行批量上传
video_money = dy_bjlp_1.video_dividend()
asyncio.run(jdy.batch_create(app_id=appId2, entry_id=entryId2, source_data=video_money))

#############################################################################################
# 对数据进行转化处理
processor.update_yesterday_data()












