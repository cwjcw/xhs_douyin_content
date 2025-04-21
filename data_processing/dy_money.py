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

from project_config.project import custom_count_sql
from data_processing.dy_video_analysis import DailyDataProcessor

class Dividend:
    def __init__(self):
        self.sql = rs.MSSQLDatabase()
        self.custom_count_path = custom_count_sql
        self.jdy = jdy.JDY()  # 调用简道云接口
        self.daily_process = DailyDataProcessor()
        self.metrics = ['播放量', '点赞量', '收藏量', '评论量', '分享量']
        self._cached_jdy_data = None   # 用于缓存简道云数据，避免重复调用

    def get_jdy_data_cached(self):
        """
        缓存获取简道云数据，避免重复调用接口。
        使用固定的 appId 与 entryId（分别为 "67c280b7c6387c4f4afd50ae" 和 "67c2816ffa795e84a8fe45b9"）来获取数据。
        """
        if self._cached_jdy_data is None:
            appId = "67c280b7c6387c4f4afd50ae"
            entryId = "67c2816ffa795e84a8fe45b9"
            self._cached_jdy_data = self.jdy.get_jdy_data(app_id=appId, entry_id=entryId)
        return self._cached_jdy_data

    def get_custom_count(self):
        '''
        获取客资数量
        '''
        try:
            print(f"Loading SQL from: {self.custom_count_path}")
            custom_count = self.sql.get_from_sqlfile(self.custom_count_path)
            return custom_count
        except FileNotFoundError as e:
            print(f"SQL文件未找到: {e}")
            return None
        except Exception as e:
            print(f"数据库操作失败: {e}")
            return None
    
    def get_daily_video_data(self):
        '''
        获取视频的点赞、评论、分享、收藏、转发数据
        '''
        daily_data = self.daily_process.get_daily_data()
        return daily_data
    
    def total_money_dy(self):
        '''
        计算总金额（客资数总和 × 50）
        '''
        total_custom = self.get_custom_count()
        total_money = total_custom['客资数'].sum()
        return total_money * 50

    def video_dividend(self):
        """
        根据作品表现，以50元的倍数分配奖励金额给每个作品。
        此函数首先对每日视频数据中的“作品名称”字段和简道云数据中的“正片标题”字段进行清洗，
        去掉第一个 "#" 前的空格及 "#" 及其后续所有字符，然后以简道云数据为主表（左连接）与每日数据匹配，
        只保留简道云数据中存在的记录，再根据作品表现计算奖励分成。
        返回包含['作品名称', '总分成', '日期']的 DataFrame。
        """
        # 获取每日视频数据，并复制
        video_df = self.get_daily_video_data().copy()
        
        # 获取缓存的简道云数据并转换为 DataFrame
        jdy_data = self.get_jdy_data_cached()
        content_df = pd.DataFrame(jdy_data)
        
        # 清洗简道云数据中“正片标题”字段：去掉第一个 "#" 前的空格及 "#" 后所有字符
        content_df['正片标题'] = content_df['_widget_1740646149825'].astype(str).apply(
            lambda x: re.sub(r'\s*#.*', '', x))
        
        # 清洗每日视频数据中“作品名称”字段：去掉第一个 "#" 前的空格及 "#" 后所有字符
        video_df['作品名称'] = video_df['作品名称'].astype(str).apply(
            lambda x: re.sub(r'\s*#.*', '', x))
        
        # 使用简道云数据作为主表，与每日视频数据进行左连接，只保留简道云数据中存在的记录
        merged_df = content_df.merge(video_df, left_on='正片标题', right_on='作品名称', how='left')
        
        # 对于未匹配到视频数据的指标填充为 0
        for metric in self.metrics:
            if metric in merged_df.columns:
                merged_df[metric] = merged_df[metric].fillna(0)
        
        # 计算总奖励金额
        total_money = self.total_money_dy()
        
        # 设置各指标的权重
        metric_weights = {
            '播放量': 0.05,
            '点赞量': 0.05,
            '收藏量': 0.3,
            '评论量': 0.3,
            '分享量': 0.3
        }
        
        # 对每个指标进行标准化（除以最大值）后再乘以对应的权重
        for metric, weight in metric_weights.items():
            max_val = merged_df[metric].max()
            standardized_col = f'{metric}_标准化'
            merged_df[standardized_col] = merged_df[metric].apply(
                lambda x: (x / max_val) * weight if max_val > 0 else 0)
        
        # 计算各作品的总表现分
        standardized_cols = [f'{metric}_标准化' for metric in metric_weights.keys()]
        merged_df['总表现分'] = merged_df[standardized_cols].sum(axis=1)
        
        # 按“正片标题”分组汇总总表现分
        video_scores = merged_df.groupby('正片标题', as_index=False)['总表现分'].sum()
        
        # 过滤掉总表现分为0的记录（无奖励）
        video_scores = video_scores[video_scores['总表现分'] > 0]
        
        # 计算对应的客户数（总奖励金额以50为单位，然后按比例分配）
        total_customers = total_money // 50
        total_scores = video_scores['总表现分'].sum()
        video_scores['客户数'] = ((video_scores['总表现分'] / total_scores) * total_customers).round().astype(int)
        
        # 调整误差，确保分配的客户数总和正确
        discrepancy = total_customers - video_scores['客户数'].sum()
        if discrepancy != 0:
            idx_max = video_scores['总表现分'].idxmax()
            video_scores.at[idx_max, '客户数'] += discrepancy
        
        # 计算每个作品的最终奖励分成（客户数 × 50）
        video_scores['总分成'] = video_scores['客户数'] * 50
        
        # 添加日期字段（使用昨天的日期）
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        video_scores['日期'] = yesterday_str
        
        # 过滤分成金额为 0 的记录
        video_scores = video_scores[video_scores["总分成"] > 0]
        
        # 将字段“正片标题”重命名为“作品名称”，保持名称统一
        video_scores.rename(columns={'正片标题': '作品名称'}, inplace=True)
        return video_scores[['作品名称', '总分成', '日期']]
                  
    def get_video_people(self):
        """
        获取每条视频对应的员工信息。
        返回包含以下字段的 DataFrame：
            正片标题 | 账号名称 | 账号ID | 是否完整内容 | 人员类别 | 人员 | 提交日期 | 来源门店/部门
        其中“人员类别”包括：完整内容提供、半成品内容提供、剪辑、发布运营。
        """
        # 使用缓存的简道云数据，避免重复调用接口
        jdy_data = self.get_jdy_data_cached()
        rows = []
        for doc in jdy_data:
            # 获取原始标题并清洗：去掉第一个 "#" 前的空格及 "#" 后所有字符
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
            temp_df = temp_df.rename(columns={group: "人员", "_id": "人员类别"})
            temp_df["人员类别"] = group
            exploded_dfs.append(temp_df)
        final_df = pd.concat(exploded_dfs, ignore_index=True)
        base_df = df[["正片标题", "账号名称", "账号ID", "是否完整内容", "提交日期", "来源门店/部门"]]
        final_df = final_df.merge(base_df, on="正片标题", how="left")
        final_df = final_df.dropna(subset=["人员"])
        column_order = [
            "正片标题", "账号名称", "账号ID", "是否完整内容", 
            "人员类别", "人员", "提交日期", "来源门店/部门"
        ]
        return final_df[column_order].reset_index(drop=True)
    
    def everyone_money(self):
        """
        计算视频内容分成金额，并确保所有总分成金额完整分配。
        最终返回包含 [人员, 分成金额, 日期] 的 DataFrame。
        
        解决方案：由于对每条记录的分成金额单独四舍五入导致累积误差，
        在计算完成后，我们根据累计误差对金额最大的记录进行补偿调整，确保
        分配后的总金额与原始视频分成金额相匹配。
        """
        # 数据预处理：获取视频对应的人员信息和视频分成数据
        video_people = self.get_video_people()
        video_money = self.video_dividend()
        # 统一关键字段名称，便于合并
        video_people = video_people.rename(columns={"正片标题": "作品名称"})
        # 数据合并：将视频人员信息与视频分成数据匹配
        merged = video_people.merge(video_money, on="作品名称", how="left")
        merged["总分成"] = merged["总分成"].fillna(0)
        total_dividend_before = video_money["总分成"].sum()
        print(f"🔍 合并前 总分成金额: {total_dividend_before}")
        # 定义分成规则，根据是否完整内容及人员类别确定分成比例
        RULES = {
            ("是", "完整内容提供"): 0.6,
            ("是", "发布运营"): 0.4,
            ("否", "半成品内容提供"): 0.4,
            ("否", "剪辑"): 0.2,
            ("否", "发布运营"): 0.4
        }
        merged["分成比例"] = merged.apply(lambda row: RULES.get((row["是否完整内容"], row["人员类别"]), 0.2), axis=1)
        # 检查是否有数据未匹配到分成规则
        missing_rules = merged[merged["分成比例"].isna()]
        if not missing_rules.empty:
            print("⚠️ 以下数据未匹配到分成规则（请检查 RULES 是否缺失）:")
            print(missing_rules[["作品名称", "人员类别", "是否完整内容"]])
        merged = merged.dropna(subset=["分成比例"])
        # 计算每组中相同（作品名称, 人员类别）下的人员数量
        merged["人数"] = merged.groupby(["作品名称", "人员类别"])["人员"].transform("count")
        # 按每条记录计算分成金额，并四舍五入保留2位小数
        merged["分成金额"] = (merged["总分成"] * merged["分成比例"] / merged["人数"]).round(2)
        result = merged.loc[(merged["人员"].notnull()), ["作品名称", "人员", "分成金额"]]
        
        # 调整累计舍入误差
        total_dividend_after = result["分成金额"].sum()
        diff = round(total_dividend_before - total_dividend_after, 2)
        if diff != 0:
            # 将累计误差补偿到分成金额最高的记录上
            idx = result["分成金额"].idxmax()
            result.loc[idx, "分成金额"] = round(result.loc[idx, "分成金额"] + diff, 2)
            # 重新计算调整后的总额
            total_dividend_after = result["分成金额"].sum()
        print(f"✅ 分配后 总分成金额: {total_dividend_after}")
        if round(total_dividend_before, 2) != round(total_dividend_after, 2):
            print(f"⚠️ 警告: 总金额调整后仍不匹配！差值 {round(total_dividend_before - total_dividend_after, 2)}")
        summary = result.groupby("人员", as_index=False)["分成金额"].sum()
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        summary["日期"] = yesterday_str
        summary = summary[summary["分成金额"] > 0]
        return summary.reset_index(drop=True)
       
    def upload_to_jdy(self):
        """
        上传分成数据到简道云系统。
        使用异步方式批量上传数据，上传数据包含 [人员, 分成金额, 日期] 字段。
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
    video_people.to_excel('视频管理.xlsx', index=False)
    people_money = dividend.everyone_money()  # 每人应分金额
    people_money.to_excel('每人分红金额.xlsx', index=False)
    data = dividend.video_dividend()
    data.to_excel('视频分红.xlsx', index=False)
    dividend.upload_to_jdy()
