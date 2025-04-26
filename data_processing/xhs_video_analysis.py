import pandas as pd
from datetime import datetime, timedelta
import os
import sys
import jdy
import asyncio

jdy = jdy.JDY()

class XhsDailyDataProcessor:
    def __init__(self):
        # 获取当前脚本所在目录 (data_processing目录)
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # 获取项目根目录（即当前目录的上一级）
        project_root = os.path.abspath(os.path.join(current_dir, ".."))

        # 将项目根目录添加到sys.path中
        if project_root not in sys.path:
            sys.path.append(project_root)

        from project_config.project import xhs_data_path, xhs_yesterday_path, xhs_file_path

        self.xhs_data_path = xhs_data_path
        self.xhs_yesterday_path = xhs_yesterday_path
        self.xhs_file_path = xhs_file_path

        # 改为小红书使用的字段
        self.compare_columns = ['观看量', '点赞', '收藏', '评论', '分享']

        # 视频质量表模板字段顺序（固定）
        self.template_columns = [
            '所属平台', '数据日期', '作品名称', '发布时间', '体裁', '审核状态', '播放量', '完播率',
            '5s完播率', '封面点击率', '2s跳出率', '平均播放时长', '点赞量', '分享量',
            '评论量', '收藏量', '主页访问量', '粉丝增量'
        ]

        # 定义字段映射关系
        self.column_mapping = {
            '所属平台': '平台',
            '数据日期': '日期',
            '作品名称': '笔记标题',
            '发布时间': '首次发布时间',
            '体裁': '体裁',
            '审核状态': None,
            '播放量': '观看量',
            '完播率': None,
            '5s完播率': None,
            '封面点击率': None,
            '2s跳出率': None,
            '平均播放时长': '人均观看时长',
            '点赞量': '点赞',
            '分享量': '分享',
            '评论量': '评论',
            '收藏量': '收藏',
            '主页访问量': None,
            '粉丝增量': '涨粉'
        }

    def get_daily_data(self):
        # —— 1. 读取并计算 daily_data —— #
        data_df = pd.read_excel(self.xhs_data_path)
        yesterday_df = pd.read_excel(self.xhs_yesterday_path)

        # 确保时间列为 datetime
        data_df['首次发布时间'] = pd.to_datetime(data_df['首次发布时间'])
        yesterday_df['首次发布时间'] = pd.to_datetime(yesterday_df['首次发布时间'])

        # 筛选
        min_date = datetime(2025, 3, 4)
        filtered = data_df[data_df['首次发布时间'] >= min_date].copy()

        # 合并并计算差值
        daily_data = pd.merge(
            filtered,
            yesterday_df[['笔记标题'] + self.compare_columns],
            on='笔记标题',
            how='left',
            suffixes=('', '_昨日')
        )
        for col in self.compare_columns:
            prev = f"{col}_昨日"
            daily_data[prev] = daily_data[prev].fillna(0)
            daily_data[col] = (daily_data[col] - daily_data[prev]).abs()
            daily_data.drop(columns=[prev], inplace=True)

        daily_data = daily_data[daily_data['首次发布时间'] >= min_date].reset_index(drop=True)

        # 插入“平台”“日期”
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        daily_data.insert(0, '日期', yesterday_str)
        daily_data.insert(0, '平台', '小红书')

        # —— 2. 构造 converted_df 用于上传 —— #
        converted_df = pd.DataFrame(index=daily_data.index, columns=self.template_columns)
        for tgt in self.template_columns:
            src = self.column_mapping.get(tgt)
            converted_df[tgt] = daily_data[src] if src in daily_data.columns else None
        converted_df['所属平台'] = '小红书'

        # —— 3. 上传到简道云 —— #
        appid, entryid = "67c280b7c6387c4f4afd50ae", "67c69341ea7d25979a4d9e8b"
        
        asyncio.run(jdy.batch_create(app_id=appid, entry_id=entryid, source_data=converted_df))

        # —— 4. 返回 daily_data —— #
        return daily_data

    def update_yesterday_data(self):
        if os.path.exists(self.xhs_yesterday_path):
            os.remove(self.xhs_yesterday_path)
            print(f"✅ 已删除旧的昨日数据文件: {self.xhs_yesterday_path}")
        else:
            print("⚠️ 旧的昨日数据文件不存在，无需删除。")

        new_yesterday_path = os.path.join(self.xhs_file_path, "yesterday.xlsx")

        if os.path.exists(self.xhs_data_path):
            os.rename(self.xhs_data_path, new_yesterday_path)
            print(f"✅ 已将 {self.xhs_data_path} 重命名为 {new_yesterday_path}")
        else:
            print("❌ 无法重命名，xhs_data_path 文件不存在。")

    # def convert_to_video_quality_format(self):
    #     """
    #     获取 daily_data，并将其转换为 视频质量数据 模板格式。
    #     返回：格式统一的新 DataFrame
    #     """
    #     df = self.get_daily_data()
    #     converted_df = pd.DataFrame(columns=self.template_columns)
    #     for target_col in self.template_columns:
    #         source_col = self.column_mapping.get(target_col)
    #         if source_col in df.columns:
    #             converted_df[target_col] = df[source_col]
    #         else:
    #             converted_df[target_col] = None
    #     converted_df['所属平台'] = '小红书'
    #     # 上传视频质量内容        
    #     appid, entryid = "67c280b7c6387c4f4afd50ae", "67c69341ea7d25979a4d9e8b"

    #     # 设置事件循环策略（在windows环境下必须添加）
    #     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    #     # # 调用并发库asyncio执行批量上传
    #     asyncio.run(jdy.batch_create(app_id=appid, entry_id=entryid, source_data=converted_df))
    #     return converted_df

# 示例调用
if __name__ == "__main__":
    processor = XhsDailyDataProcessor()
    # processor.update_yesterday_data()
    formatted_df = processor.get_daily_data()
    # formatted_df.to_excel('小红书视频质量数据daily.xlsx', index=False)
    # print("✅ 已保存转换后的视频质量数据为 '转换后的视频质量数据.xlsx'")
