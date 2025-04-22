import os
import time
import jdy
import asyncio
from data_processing.dy_video_analysis import DailyDataProcessor
from spiders import douyin, xhs
from spiders.douyin import merge_xlsx_files
from data_processing.dy_money import Dividend
from project_config.project import dy_file_path, xhs_cookie_list
from spiders.xhs import Xhs
from data_processing.xhs_money import XhsDividend


# 创建简道云对象
jdy = jdy.JDY()
#############################################################################################
# # 下载抖音数据

# # 获取项目根目录
# project_root = os.path.dirname(os.path.abspath(__file__))

# # 指向 spiders 目录
# spiders_dir = os.path.join(project_root, 'spiders')

# # 自动获取所有包含 'dy' 且以 .pkl 结尾的文件
# cookie_list = [f for f in os.listdir(spiders_dir) if 'douyin' in f and f.endswith('.pkl')]

# if not cookie_list:
#     print("⚠️ 未找到符合条件的 dy*.pkl 文件！")
# else:
#     for cookie_file in cookie_list:
#         full_cookie_path = os.path.join(spiders_dir, cookie_file)
#         print(f"\n🌐 当前账号: {cookie_file}")

#         douyin_instance = douyin.Douyin("https://creator.douyin.com/creator-micro/home", full_cookie_path)
#         douyin_instance.run()

#         print("⏳ 等待下载完成...")
#         time.sleep(15)  # 视网络情况可调整

#     print("\n📁 开始合并所有Excel文件...")
#     merge_xlsx_files(dy_file_path)

#############################################################################################
# 获取抖音新增的视频质量数量，包括播放，点赞，收藏，评论，分享，收藏等
# dividend = Dividend()
# print(dividend.total_money_dy())
# print(dividend.get_custom_count()['客资数'].sum())
# video_people = dividend.get_video_people()
# video_people.to_excel('抖音视频管理.xlsx', index=False)
# people_money = dividend.everyone_money()  # 每人应分金额
# people_money.to_excel('抖音每人分红金额.xlsx', index=False)
# data = dividend.video_dividend()
# data.to_excel('抖音视频分红.xlsx', index=False)
# dividend.upload_to_jdy()


# #############################################################################################
# # 上传当天的视频分红
# appId2 ="67c280b7c6387c4f4afd50ae"
# entryId2 = "67d7ead7492e5041b57fb891"
# # 调用并发库asyncio执行批量上传
# video_money = dy_bjlp_1.video_dividend()
# asyncio.run(jdy.batch_create(app_id=appId2, entry_id=entryId2, source_data=video_money))

# #############################################################################################
# 获取小红书数据
# 调用 process_all_accounts 方法处理所有账号并返回合并后的 DataFrame
# final_df = Xhs.process_all_accounts(xhs_cookie_list)

# if final_df is not None:
#     print("✅ 最终合并的 DataFrame：")
#     print(final_df.head())
# else:
#     print("⚠️ 未能生成合并的 DataFrame")

#############################################################################################
# 获取小红书新增的视频质量数量，包括播放，点赞，收藏，评论，分享，收藏等

xhsdividend = XhsDividend()
print(xhsdividend.total_money_dy())
print(xhsdividend.get_custom_count()['客资数'].sum())
video_people = xhsdividend.get_video_people()
video_people.to_excel('小红书视频管理.xlsx', index=False)
people_money = xhsdividend.everyone_money()
people_money.to_excel('小红书每人分红金额.xlsx', index=False)
data = xhsdividend.video_dividend()
data.to_excel('小红书视频分红.xlsx', index=False)
xhsdividend.upload_to_jdy()








