from data_processing.dy_video_analysis import DailyDataProcessor
from spiders import douyin, xhs
from data_processing import dy_video_analysis, xhs_video_analysis

# 创建浏览器对象，并下载data.xlsx
dy_spider = douyin.Douyin("https://creator.douyin.com/creator-micro/home")
dy_spider.run()

#############################################################################################
# 获取抖音新增的视频质量数量，包括播放，点赞，收藏，评论，分享，收藏等
processor = DailyDataProcessor()
daily_data = processor.get_daily_data()
print(daily_data.head)
daily_data.to_excel(r'C:\Users\Administrator\Desktop\临时文件\daily.xlsx',index=False)

#############################################################################################



#############################################################################################
# 上传当天的视频分红


#############################################################################################
# 对数据进行转化处理
processor.update_yesterday_data()












