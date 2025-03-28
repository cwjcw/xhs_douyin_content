'''
处理下载的小红书内容质量数据，将其转换为当天的数据，并在处理后将其重命名，为下一天继续处理做准备
'''

import pandas as pd
from datetime import datetime, timedelta
import os
import sys

class XhsDailyDataProcessor:
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
        self.compare_columns = ['观看量', '点赞', '分享', '评论', '收藏']

    def get_daily_data(self):
    # 读取当天数据和昨天的数据
        data_df = pd.read_excel(self.data_path)
        yesterday_df = pd.read_excel(self.yesterday_data_path)

        # 把“2025年03月25日15时06分58秒” 字符串格式转换为 datetime 对象（不转成字符串）
        def parse_date(date_str):
            return datetime.strptime(date_str, "%Y年%m月%d日%H时%M分%S秒")

        # 应用于两个数据集
        data_df['首次发布时间'] = data_df['首次发布时间'].astype(str).apply(parse_date)
        yesterday_df['首次发布时间'] = yesterday_df['首次发布时间'].astype(str).apply(parse_date)

        # 日期过滤条件
        min_date = datetime(2025, 3, 14)

        # 筛选出符合条件的数据（开始日期≥2025-03-04）
        filtered_data_df = data_df[data_df['首次发布时间'] >= min_date].copy()

        # 使用明确的字段（比如：笔记标题）合并今天和昨天的数据
        daily_data = pd.merge(
            filtered_data_df,
            yesterday_df[['笔记标题'] + self.compare_columns],
            on='笔记标题',
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

        # 筛选首次发布时间满足条件的数据
        daily_data = daily_data[daily_data['首次发布时间'] >= min_date].reset_index(drop=True)     
        
        # 获取昨天日期，格式为YYYY-MM-DD
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 在daily_data第一列插入日期字段
        daily_data.insert(0, '日期', yesterday_str)

        # 在daily_data第一列插入平台字段
        daily_data.insert(0, '平台', '小红书')
        
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
    processor = XhsDailyDataProcessor()
    daily_data = processor.get_daily_data()
    daily_data.to_excel('daily_data.xlsx', index=False)
    print(daily_data)
