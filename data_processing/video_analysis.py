'''
处理下载的抖音视频质量数据，将其转换为当天的数据，并在处理后将其重命名，为下一天继续处理做准备
'''

import pandas as pd
from datetime import datetime, timedelta
import os
import sys

class DailyDataProcessor:
    def __init__(self):
        # 获取当前脚本所在目录 (data_processing目录)
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # 获取项目根目录（即当前目录的上一级）
        project_root = os.path.abspath(os.path.join(current_dir, ".."))

        # 将项目根目录添加到sys.path中
        if project_root not in sys.path:
            sys.path.append(project_root)

        from project_config.project import data_path, yesterday_data_path, file_path

        self.data_path = data_path
        self.yesterday_data_path = yesterday_data_path
        self.file_path = file_path
        self.compare_columns = ['播放量', '点赞量', '分享量', '评论量', '收藏量']

    def get_daily_data(self):
        # 读取当天数据和昨天的数据
        data_df = pd.read_excel(self.data_path)
        yesterday_df = pd.read_excel(self.yesterday_data_path)

        # 确认发布时间字段格式为日期格式
        data_df['发布时间'] = pd.to_datetime(data_df['发布时间'])
        yesterday_df['发布时间'] = pd.to_datetime(yesterday_df['发布时间'])

        # 日期过滤条件
        min_date = datetime(2025, 3, 4)

        # 筛选出符合条件的数据（开始日期≥2025-03-04）
        filtered_data_df = data_df[data_df['发布时间'] >= min_date].copy()

        # 使用明确的字段（比如：作品名称）合并今天和昨天的数据
        daily_data = pd.merge(
            filtered_data_df,
            yesterday_df[['作品名称'] + self.compare_columns],
            on='作品名称',
            how='left',
            suffixes=('', '_昨日')
        )

        # 处理昨日无数据的情况
        for col in self.compare_columns:
            yesterday_col = f"{col}_昨日"
            daily_data[yesterday_col] = daily_data[yesterday_col].fillna(0)

            # 计算绝对值差值
            daily_data[col] = (daily_data[col] - daily_data[yesterday_col]).abs()

            # 删除昨日数据列，保留最终计算结果
            daily_data.drop(columns=[yesterday_col], inplace=True)

        # 筛选发布时间满足条件的数据
        daily_data = daily_data[daily_data['发布时间'] >= min_date].reset_index(drop=True)     
        
        # 获取昨天日期，格式为YYYY-MM-DD
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 在daily_data第一列插入日期字段
        daily_data.insert(0, '日期', yesterday_str)
        
        # 返回处理后的daily_data
        return daily_data
    
    def update_yesterday_data(self):
        """
        删除yesterday_data_path文件，并将data_path重命名为yesterday_data.xlsx
        :param file_path: 存放yesterday_data.xlsx的目标目录
        """
        # 确保 yesterday_data_path 文件存在再删除
        if os.path.exists(self.yesterday_data_path):
            os.remove(self.yesterday_data_path)
            print(f"✅ 已删除旧的昨日数据文件: {self.yesterday_data_path}")
        else:
            print("⚠️ 旧的昨日数据文件不存在，无需删除。")
        
        # 目标文件路径
        new_yesterday_path = os.path.join(self.file_path, "yesterday_data.xlsx")
        
        # 重命名 data_path 文件
        if os.path.exists(self.data_path):
            os.rename(self.data_path, new_yesterday_path)
            print(f"✅ 已将 {self.data_path} 重命名为 {new_yesterday_path}")
        else:
            print("❌ 无法重命名，data_path 文件不存在。")

# 示例调用
if __name__ == "__main__":
    processor = DailyDataProcessor()
    daily_data = processor.get_daily_data()
    daily_data.to_excel('daily_data.xlsx', index=False)
    print(daily_data)
