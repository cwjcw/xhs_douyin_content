import read_sql as rs
import os
import re
import sys
import jdy
import pandas as pd
import asyncio
from datetime import datetime, timedelta

# 模块级路径配置
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from project_config.project import xhs_custom_count_sql
from data_processing.dy_video_analysis import DailyDataProcessor

class Dividend:
    def __init__(self):
        """初始化数据库、简道云接口、数据处理路径等配置"""
        self.sql = rs.MSSQLDatabase()
        self.custom_count_path = xhs_custom_count_sql
        self.jdy = jdy.JDY()
        self.daily_process = DailyDataProcessor()
        self.metrics = ['观看量', '点赞', '收藏', '评论', '分享']
        self._cached_jdy_data = None

    def get_jdy_data_cached(self):
        """缓存简道云数据，避免重复请求"""
        if self._cached_jdy_data is None:
            appId = "67c280b7c6387c4f4afd50ae"
            entryId = "67c2816ffa795e84a8fe45b9"
            self._cached_jdy_data = self.jdy.get_jdy_data(app_id=appId, entry_id=entryId)
        return self._cached_jdy_data

    def get_custom_count(self):
        """从SQL文件中获取客资数据"""
        try:
            print(f"Loading SQL from: {self.custom_count_path}")
            return self.sql.get_from_sqlfile(self.custom_count_path)
        except FileNotFoundError as e:
            print(f"SQL文件未找到: {e}")
            return None
        except Exception as e:
            print(f"数据库操作失败: {e}")
            return None

    def get_daily_video_data(self):
        """获取每日视频数据"""
        return self.daily_process.get_daily_data()

    def total_money_dy(self):
        """计算奖励总金额 = 客资总和 * 50"""
        total_custom = self.get_custom_count()
        return total_custom['客资数'].sum() * 50

    def video_dividend(self):
        """
        处理简道云视频数据与每日视频表现数据，计算每条作品应得的分成金额
        返回包含 [作品名称, 总分成, 日期] 的 DataFrame
        """
        video_df = self.get_daily_video_data().copy()
        jdy_data = self.get_jdy_data_cached()
        content_df = pd.DataFrame(jdy_data)

        content_df['作品名称'] = content_df['_widget_1740646149825'].astype(str).apply(lambda x: re.sub(r'\s*#.*', '', x))
        video_df['作品名称'] = video_df['作品名称'].astype(str).apply(lambda x: re.sub(r'\s*#.*', '', x))

        merged_df = content_df.merge(video_df, on='作品名称', how='left')

        for metric in self.metrics:
            if metric in merged_df.columns:
                merged_df[metric] = merged_df[metric].fillna(0)

        total_money = self.total_money_dy()
        metric_weights = {'观看量': 0.05, '点赞': 0.05, '收藏': 0.3, '评论': 0.3, '分享': 0.3}

        for metric, weight in metric_weights.items():
            max_val = merged_df[metric].max()
            merged_df[f'{metric}_标准化'] = merged_df[metric].apply(lambda x: (x / max_val) * weight if max_val > 0 else 0)

        standardized_cols = [f'{m}_标准化' for m in metric_weights]
        merged_df['总表现分'] = merged_df[standardized_cols].sum(axis=1)

        video_scores = merged_df.groupby('作品名称', as_index=False)['总表现分'].sum()
        video_scores = video_scores[video_scores['总表现分'] > 0]

        total_customers = total_money // 50
        total_scores = video_scores['总表现分'].sum()
        video_scores['客户数'] = ((video_scores['总表现分'] / total_scores) * total_customers).round().astype(int)

        discrepancy = total_customers - video_scores['客户数'].sum()
        if discrepancy != 0:
            idx_max = video_scores['总表现分'].idxmax()
            video_scores.at[idx_max, '客户数'] += discrepancy

        video_scores['总分成'] = video_scores['客户数'] * 50
        video_scores['日期'] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        video_scores = video_scores[video_scores['总分成'] > 0]
        return video_scores[['作品名称', '总分成', '日期']]

    def get_video_people(self):
        """提取简道云中每条作品对应的人员信息"""
        jdy_data = self.get_jdy_data_cached()
        rows = []
        for doc in jdy_data:
            title_raw = doc.get("_widget_1740646149825", "")
            title_cleaned = re.sub(r'\s*#.*', '', title_raw)
            base_fields = {
                "账号名称": doc.get("_widget_1741257105163", ""),
                "账号ID": doc.get("_widget_1741257105165", ""),
                "是否完整内容": doc.get("_widget_1740798082550", ""),
                "作品名称": title_cleaned,
                "提交日期": doc.get("_widget_1740646149826", ""),
                "来源门店/部门": doc.get("_widget_1741934971937", {}).get("name", "")
            }
            user_groups = {
                "完整内容提供": [u.get("username") for u in doc.get("_widget_1740798082567", [])],
                "半成品内容提供": [u.get("username") for u in doc.get("_widget_1740798082568", [])],
                "剪辑": [u.get("username") for u in doc.get("_widget_1740798082569", [])],
                "发布运营": [u.get("username") for u in doc.get("_widget_1740798082570", [])]
            }
            max_len = max(len(g) for g in user_groups.values()) or 1
            for group, users in user_groups.items():
                for user in users + [None] * (max_len - len(users)):
                    row = {**base_fields, "人员类别": group, "人员": user}
                    rows.append(row)
        df = pd.DataFrame(rows)
        return df.dropna(subset=["人员"])

    def everyone_money(self):
        """根据规则分配分成金额给人员"""
        video_people = self.get_video_people()
        video_money = self.video_dividend()

        merged = video_people.merge(video_money, on="作品名称", how="left")
        merged["总分成"] = merged["总分成"].fillna(0)

        RULES = {
            ("是", "完整内容提供"): 0.6,
            ("是", "发布运营"): 0.4,
            ("否", "半成品内容提供"): 0.4,
            ("否", "剪辑"): 0.2,
            ("否", "发布运营"): 0.4
        }

        merged["分成比例"] = merged.apply(lambda row: RULES.get((row["是否完整内容"], row["人员类别"]), 0.2), axis=1)
        merged = merged.dropna(subset=["分成比例"])
        merged["人数"] = merged.groupby(["作品名称", "人员类别"])["人员"].transform("count")
        merged["分成金额"] = (merged["总分成"] * merged["分成比例"] / merged["人数"]).round(2)

        result = merged[["人员", "分成金额"]].groupby("人员", as_index=False).sum()

        before = video_money["总分成"].sum()
        after = result["分成金额"].sum()
        diff = round(before - after, 2)
        if diff != 0:
            idx = result["分成金额"].idxmax()
            result.loc[idx, "分成金额"] = round(result.loc[idx, "分成金额"] + diff, 2)

        result["日期"] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        return result[result["分成金额"] > 0].reset_index(drop=True)

    def upload_to_jdy(self):
        """上传结果数据到简道云"""
        appId = "67c280b7c6387c4f4afd50ae"
        entryId = "67d7097d08e5f607c4cfd028"
        final_data = self.everyone_money()
        asyncio.run(self.jdy.batch_create(app_id=appId, entry_id=entryId, source_data=final_data))

if __name__ == '__main__':
    dividend = Dividend()
    print(dividend.total_money_dy())
    print(dividend.get_custom_count()['客资数'].sum())

    video_people = dividend.get_video_people()
    video_people.to_excel('小红书_视频管理.xlsx', index=False)

    people_money = dividend.everyone_money()
    people_money.to_excel('小红书_每人分红金额.xlsx', index=False)

    data = dividend.video_dividend()
    data.to_excel('小红书_视频分红.xlsx', index=False)

    # dividend.upload_to_jdy()
