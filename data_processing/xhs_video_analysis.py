import pandas as pd
from datetime import datetime, timedelta
import os
import sys
import warnings

# 忽略 openpyxl 样式警告
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

class XhsDailyDataProcessor:
    def __init__(self):
        # 获取当前脚本所在目录 (data_processing目录)
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # 获取项目根目录（即当前目录的上一级）
        project_root = os.path.abspath(os.path.join(current_dir, ".."))

        # 将项目根目录添加到sys.path中
        if project_root not in sys.path:
            sys.path.append(project_root)

        from project_config.project import xhs_file_path, xhs_yesterday_path, xhs_data_path

        self.data_path = xhs_data_path
        self.yesterday_data_path = xhs_yesterday_path
        self.compare_columns = ['观看量', '点赞', '分享', '评论', '收藏']

    def load_data(self):
        """
        查找路径 xhs_file_path 中所有文件名包含 “笔记列表明细表” 的xlsx文件，
        每个文件跳过第一行，第二行作为标题，合并成一个DataFrame返回。
        """
        if os.path.isfile(self.data_path):
            data_dir = os.path.dirname(self.data_path)
        else:
            data_dir = self.data_path

        target_files = [f for f in os.listdir(data_dir) if "笔记列表明细表" in f and f.endswith(".xlsx")]
        if not target_files:
            print("没有找到文件名包含 '笔记列表明细表' 的xlsx文件。")
            return pd.DataFrame()

        df_list = []
        for file in target_files:
            file_path = os.path.join(data_dir, file)
            df = pd.read_excel(file_path, header=1)  # 跳过第一行
            df_list.append(df)

        combined_df = pd.concat(df_list, ignore_index=True)
        return combined_df

    def get_daily_data(self):
        data_df = self.load_data()
        if data_df.empty:
            print("❌ 未能加载主数据，停止处理。")
            return pd.DataFrame()

        yesterday_df = pd.read_excel(self.yesterday_data_path)

        # def parse_date(date_str):
        #     return datetime.strptime(date_str, "%Y年%m月%d日%H时%M分%S秒")

        # data_df['首次发布时间'] = data_df['首次发布时间'].astype(str).apply(parse_date)
        # # yesterday_df['首次发布时间'] = yesterday_df['首次发布时间'].astype(str).apply(parse_date)

        # min_date = datetime(2025, 3, 14)
        # data_df = data_df[data_df['首次发布时间'] >= min_date].copy()
        # yesterday_df = yesterday_df[yesterday_df['首次发布时间'] >= min_date].copy()

        # 以笔记标题为关键字进行合并，并进行数据字段相减：今日 - 昨日
        daily_data = pd.merge(
            data_df,
            yesterday_df[['笔记标题'] + self.compare_columns],
            on='笔记标题',
            how='left',
            suffixes=('', '_昨日')
        )

        for col in self.compare_columns:
            yesterday_col = f"{col}_昨日"
            daily_data[yesterday_col] = daily_data[yesterday_col].fillna(0)
            daily_data[col] = daily_data[col] - daily_data[yesterday_col]
            daily_data.drop(columns=[yesterday_col], inplace=True)

        daily_data = daily_data.reset_index(drop=True)
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        daily_data.insert(0, '日期', yesterday_str)
        daily_data.insert(0, '平台', '小红书')

        return daily_data

    def update_yesterday_data(self):
        if os.path.exists(self.yesterday_data_path):
            os.remove(self.yesterday_data_path)
            print(f"✅ 已删除旧的昨日数据文件: {self.yesterday_data_path}")
        else:
            print("⚠️ 旧的昨日数据文件不存在，无需删除。")

        data_dir = self.data_path if os.path.isdir(self.data_path) else os.path.dirname(self.data_path)
        new_yesterday_path = os.path.join(data_dir, "yesterday_data.xlsx")

        if os.path.exists(self.data_path):
            os.rename(self.data_path, new_yesterday_path)
            print(f"✅ 已将 {self.data_path} 重命名为 {new_yesterday_path}")
        else:
            print("❌ 无法重命名，data_path 文件不存在。")

# 示例调用
if __name__ == "__main__":
    processor = XhsDailyDataProcessor()
    daily_data = processor.get_daily_data()
    daily_data.to_excel(os.path.join(processor.data_path, 'combined_note_list_details.xlsx'), index=False)
    print("每日数据：")
    print(daily_data)

    # 保存原始合并数据（load_data的结果）为 yesterday.xlsx
    raw_data = processor.load_data()
    raw_data.to_excel(os.path.join(processor.data_path, 'yesterday.xlsx'), index=False)
