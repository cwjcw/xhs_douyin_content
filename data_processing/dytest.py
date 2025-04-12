import read_sql as rs
import os
import re
import sys
import jdy
import pandas as pd
import asyncio
from datetime import datetime, timedelta

# 配置模块级路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from project_config.project import xhs_custom_count_sql
from data_processing.xhs_video_analysis import DailyDataProcessor

class Dividend:
    """
    视频内容分红管理类：用于读取视频数据、客资数据和简道云信息，
    进行内容表现分析、分红计算并可上传结果至简道云。
    """

    def __init__(self):
        self.sql = rs.MSSQLDatabase()
        self.custom_count_path = xhs_custom_count_sql
        self.jdy = jdy.JDY()
        self.daily_process = DailyDataProcessor()
        self.metrics = ['观看量', '点赞', '收藏', '评论', '分享']
        self._cached_jdy_data = None

    def get_jdy_data_cached(self):
        """
        从简道云获取并缓存数据，避免重复调用。
        """
        if self._cached_jdy_data is None:
            appId = "67c280b7c6387c4f4afd50ae"
            entryId = "67c2816ffa795e84a8fe45b9"
            self._cached_jdy_data = self.jdy.get_jdy_data(app_id=appId, entry_id=entryId)
        return self._cached_jdy_data

    def get_custom_count(self):
        """
        从SQL文件读取客资数量。
        """
        try:
            return self.sql.get_from_sqlfile(self.custom_count_path)
        except Exception as e:
            print(f"读取客资数失败: {e}")
            return pd.DataFrame()

    def get_daily_video_data(self):
        """
        获取每日视频数据，字段自动标准化。
        """
        try:
            df = self.daily_process.get_daily_data()
            rename_map = {
                '播放量': '观看量',
                '播放次数': '观看量'
            }
            df.rename(columns=rename_map, inplace=True)
            return df
        except Exception as e:
            print("❌ get_daily_data 报错：", e)
            return pd.DataFrame()

    def video_dividend(self):
        """
        计算每条视频根据表现应得的分成金额。
        返回包含 [作品名称, 总分成, 日期] 的 DataFrame。
        """
        video_df = self.get_daily_video_data()
        print("🎬 video_df 字段名：", video_df.columns.tolist())

        jdy_data = self.get_jdy_data_cached()
        content_df = pd.DataFrame(jdy_data)
        print("📄 content_df 字段名：", content_df.columns.tolist())

        content_df['正片标题'] = content_df['_widget_1740646149825'].astype(str).apply(lambda x: re.sub(r'\s*#.*', '', x))
        video_df['笔记标题'] = video_df['笔记标题'].astype(str).apply(lambda x: re.sub(r'\s*#.*', '', x))

        merged_df = content_df.merge(video_df, left_on='正片标题', right_on='笔记标题', how='left')
        print("🧩 merged_df 字段名：", merged_df.columns.tolist())

        for metric in self.metrics:
            if metric not in merged_df.columns:
                print(f"⚠️ 缺失字段 {metric}，自动填充 0")
                merged_df[metric] = 0

        metric_weights = {
            '观看量': 0.05,
            '点赞': 0.05,
            '收藏': 0.3,
            '评论': 0.3,
            '分享': 0.3
        }

        for metric, weight in metric_weights.items():
            max_val = merged_df[metric].max()
            merged_df[f'{metric}_标准化'] = merged_df[metric].apply(lambda x: (x / max_val) * weight if max_val > 0 else 0)

        merged_df['总表现分'] = merged_df[[f'{m}_标准化' for m in metric_weights]].sum(axis=1)
        video_scores = merged_df.groupby('正片标题', as_index=False)['总表现分'].sum()
        video_scores = video_scores[video_scores['总表现分'] > 0]

        total_money = self.total_money_dy()
        total_customers = total_money // 50
        total_scores = video_scores['总表现分'].sum()
        video_scores['客户数'] = ((video_scores['总表现分'] / total_scores) * total_customers).round().astype(int)

        diff = total_customers - video_scores['客户数'].sum()
        if diff != 0:
            idx = video_scores['总表现分'].idxmax()
            video_scores.at[idx, '客户数'] += diff

        video_scores['总分成'] = video_scores['客户数'] * 50
        video_scores['日期'] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        video_scores.rename(columns={'正片标题': '作品名称'}, inplace=True)
        return video_scores[['作品名称', '总分成', '日期']]

    def total_money_dy(self):
        """
        总分红池金额（= 客资总数 × 50）。
        """
        df = self.get_custom_count()
        return df['客资数'].sum() * 50 if '客资数' in df.columns else 0

    def get_video_people(self):
        """
        获取视频人员参与信息，返回每条视频对应人员及角色。
        """
        jdy_data = self.get_jdy_data_cached()
        rows = []
        for doc in jdy_data:
            title_raw = doc.get("_widget_1740646149825", "")
            title_cleaned = re.sub(r'\s*#.*', '', title_raw)
            base_fields = {
                "账号名称": doc.get("_widget_1741257105163", ""),
                "账号ID": doc.get("_widget_1741257105165", ""),
                "是否完整内容": doc.get("_widget_1740798082550", ""),
                "正片标题": title_cleaned,
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
            aligned_groups = {}
            for field, users in user_groups.items():
                aligned_groups[field] = users + [None] * (max_len - len(users))
            row = {**base_fields, **aligned_groups}
            rows.append(row)

        df = pd.DataFrame(rows)
        exploded_dfs = []
        for group in ["完整内容提供", "半成品内容提供", "剪辑", "发布运营"]:
            temp_df = df[["正片标题", group]].explode(group)
            temp_df = temp_df.rename(columns={group: "人员"})
            temp_df["人员类别"] = group
            exploded_dfs.append(temp_df)

        final_df = pd.concat(exploded_dfs, ignore_index=True)
        base_df = df[["正片标题", "账号名称", "账号ID", "是否完整内容", "提交日期", "来源门店/部门"]]
        final_df = final_df.merge(base_df, on="正片标题", how="left")
        final_df = final_df.dropna(subset=["人员"])

        return final_df[["正片标题", "账号名称", "账号ID", "是否完整内容", "人员类别", "人员", "提交日期", "来源门店/部门"]].reset_index(drop=True)

    def everyone_money(self):
        """
        根据参与人及角色计算每人应得的分红金额。
        """
        video_people = self.get_video_people()
        video_money = self.video_dividend()
        video_people = video_people.rename(columns={"正片标题": "作品名称"})
        merged = video_people.merge(video_money, on="作品名称", how="left")
        merged["总分成"] = merged["总分成"].fillna(0)
        total_dividend_before = video_money["总分成"].sum()
        print(f"🔍 合并前 总分成金额: {total_dividend_before}")

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

        result = merged.loc[(merged["人员"].notnull()), ["作品名称", "人员", "分成金额"]]
        total_dividend_after = result["分成金额"].sum()
        diff = round(total_dividend_before - total_dividend_after, 2)
        if diff != 0:
            idx = result["分成金额"].idxmax()
            result.loc[idx, "分成金额"] = round(result.loc[idx, "分成金额"] + diff, 2)

        total_dividend_after = result["分成金额"].sum()
        print(f"✅ 分配后 总分成金额: {total_dividend_after}")
        if round(total_dividend_before, 2) != round(total_dividend_after, 2):
            print(f"⚠️ 警告: 总金额有损失！缺少 {round(total_dividend_before - total_dividend_after, 2)}")

        summary = result.groupby("人员", as_index=False)["分成金额"].sum()
        summary["日期"] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        return summary[summary["分成金额"] > 0].reset_index(drop=True)

    def upload_to_jdy(self):
        """
        将分红结果上传至简道云。
        """
        appId = "67c280b7c6387c4f4afd50ae"
        entryId = "67d7097d08e5f607c4cfd028"
        final_data = self.everyone_money()
        asyncio.run(self.jdy.batch_create(app_id=appId, entry_id=entryId, source_data=final_data))

if __name__ == '__main__':
    dividend = Dividend()
    print(dividend.total_money_dy())
    print(dividend.get_custom_count()['客资数'].sum())
    video_people = dividend.get_video_people()
    video_people.to_excel('小红书视频管理.xlsx', index=False)
    people_money = dividend.everyone_money()
    people_money.to_excel('小红书每人分红金额.xlsx', index=False)
    data = dividend.video_dividend()
    data.to_excel('小红书视频分红.xlsx', index=False)
    # dividend.upload_to_jdy()