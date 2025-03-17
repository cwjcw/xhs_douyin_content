import read_sql as rs
import os
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

from project_config.project import custom_count_sql, video_content
from data_processing.video_analysis import DailyDataProcessor

class Dividend:
    def __init__(self):
        self.sql = rs.MSSQLDatabase()
        self.custom_count_path = custom_count_sql
        self.jdy = jdy.JDY()
        self.daily_process = DailyDataProcessor()
        self.metrics = ['播放量', '点赞量', '收藏量', '评论量', '分享量']

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
        获取视频的点赞，评论，分享，收藏，转发数据
        '''
        daily_data = self.daily_process.get_daily_data()
        return daily_data
    
    def total_money_dy(self):
        '''
        计算总金额
        '''
        total_custom = self.get_custom_count()
        total_money = total_custom['numbers'].sum()
        return total_money*50

    def video_dividend(self):
        """
        根据作品表现，以50元的倍数分配奖励金额给每个作品。

        参数:
        self.video_df: 包含['作品名称', '播放量', '点赞量', '收藏量', '评论量', '分享量']的数据
        self.total_money_dy(): 奖励总金额，必须为50的整数倍
        """
        video_df = self.get_daily_video_data().copy()
        total_money = self.total_money_dy()

        # 避免播放量为 0 的情况
        video_df['播放量'] = video_df['播放量'].replace(0, 1)  # 防止除零错误

        # 计算互动率
        video_df['点赞率'] = video_df['点赞量'] / video_df['播放量']
        video_df['收藏率'] = video_df['收藏量'] / video_df['播放量']
        video_df['评论率'] = video_df['评论量'] / video_df['播放量']
        video_df['分享率'] = video_df['分享量'] / video_df['播放量']

        # 计算总表现分，每个维度占 25%
        video_df['总表现分'] = (
            0.25 * video_df['点赞率'] +
            0.25 * video_df['收藏率'] +
            0.25 * video_df['评论率'] +
            0.25 * video_df['分享率']
        )

        # 按作品名称汇总总表现分
        video_scores = video_df.groupby('作品名称', as_index=False)['总表现分'].sum()

        # 过滤表现分为0的作品（无奖励）
        video_scores = video_scores[video_scores['总表现分'] > 0]

        # 计算每个作品的奖励客户数（以50元为单位）
        total_customers = total_money // 50
        total_scores = video_scores['总表现分'].sum()

        video_scores['客户数'] = ((video_scores['总表现分'] / total_scores) * total_customers).round().astype(int)

        # 调整误差确保客户数总和正确
        discrepancy = total_customers - video_scores['客户数'].sum()
        if discrepancy != 0:
            idx_max = video_scores['总表现分'].idxmax()
            video_scores.at[idx_max, '客户数'] += discrepancy

        # 计算最终奖励金额（客户数 × 50）
        video_scores['总分成'] = video_scores['客户数'] * 50

        # 添加日期字段
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        video_scores['日期'] = yesterday_str

        return video_scores[['作品名称', '总分成', '日期']]
                  
    def get_video_people(self):
        """
        获取每条视频对应的员工
        返回包含以下字段的DataFrame：
        正片标题 | 账号名称 | 账号ID | 是否完整内容 | 完整内容提供 
        半成品内容提供 | 剪辑 | 发布运营 | 提交日期 | 来源门店/部门
        """
        appId = "67c280b7c6387c4f4afd50ae"
        entryId = "67c2816ffa795e84a8fe45b9"
        video_people = self.jdy.get_jdy_data(app_id=appId, entry_id=entryId)
        
        rows = []
        for doc in video_people:
            # 基础字段处理
            base_fields = {
                "账号名称": doc.get("_widget_1741257105163", ""),
                "账号ID": doc.get("_widget_1741257105165", ""),
                "是否完整内容": doc.get("_widget_1740798082550", ""),
                "正片标题": doc.get("_widget_1740646149825", ""),
                "提交日期": doc.get("_widget_1740646149826", ""),
                "来源门店/部门": doc.get("_widget_1741934971937", {}).get("name", "")
            }
            
            # 用户组字段处理（带长度对齐）
            user_groups = {
                "完整内容提供": [u.get("username") for u in doc.get("_widget_1740798082567", [])],
                "半成品内容提供": [u.get("username") for u in doc.get("_widget_1740798082568", [])],
                "剪辑": [u.get("username") for u in doc.get("_widget_1740798082569", [])],
                "发布运营": [u.get("username") for u in doc.get("_widget_1740798082570", [])]
            }
            
            # 计算最大用户组长度
            max_len = max(len(g) for g in user_groups.values()) or 1
            
            # 填充所有用户组到相同长度
            aligned_groups = {}
            for field, users in user_groups.items():
                aligned_groups[field] = users + [None]*(max_len - len(users))  # 用None填充
                
            # 合并字段
            row = {**base_fields, **aligned_groups}
            rows.append(row)
        
        # 生成DataFrame并展开
        df = pd.DataFrame(rows)
        
        # 分步展开每组字段（解决长度不一致问题）
        exploded_dfs = []
        for group in ["完整内容提供", "半成品内容提供", "剪辑", "发布运营"]:
            temp_df = df[["正片标题"] + [group]].explode(group)
            temp_df = temp_df.rename(columns={group: "人员", "_id": "人员类别"})
            temp_df["人员类别"] = group  # 添加类别标识
            exploded_dfs.append(temp_df)
        
        # 合并所有展开结果
        final_df = pd.concat(exploded_dfs, ignore_index=True)
        
        # 合并基础字段
        base_df = df[["正片标题", "账号名称", "账号ID", "是否完整内容", "提交日期", "来源门店/部门"]]
        final_df = final_df.merge(base_df, on="正片标题", how="left")
        
        # 清理空值
        final_df = final_df.dropna(subset=["人员"])
        
        # 字段排序
        column_order = [
            "正片标题", "账号名称", "账号ID", "是否完整内容", 
            "人员类别", "人员", "提交日期", "来源门店/部门"
        ]
        return final_df[column_order].reset_index(drop=True)

    
    def everyone_money(self):
        """
        计算视频内容分成金额
        
        参数：
        video_money : 包含[作品名称, 总分成]的DataFrame
        video_people : 包含[正片标题, 人员类别, 人员, 是否完整内容]的DataFrame
        
        返回：
        包含[作品名称, 人员类别, 人员, 分成金额]的DataFrame
        """
        # 数据预处理
        video_people = self.get_video_people()
        video_money = self.video_dividend()

        # 统一关键字段名称
        video_people = video_people.rename(columns={"正片标题": "作品名称"})
        
        # 数据合并
        merged = video_people.merge(video_money, on="作品名称", how="inner")
        
        # 定义分成规则
        RULES = {
            ("是", "完整内容提供"): 0.6,
            ("是", "发布运营"): 0.4,
            ("否", "半成品内容提供"): 0.4,
            ("否", "剪辑"): 0.2,
            ("否", "发布运营"): 0.4
        }
        
        # 计算每个角色的人数
        merged["人数"] = merged.groupby(["作品名称", "人员类别"])["人员"].transform("count")
        
        # 计算分成比例
        merged["分成比例"] = merged.apply(lambda row: RULES.get((row["是否完整内容"], row["人员类别"]), 0), axis=1)
        
        # 计算分成金额
        merged["分成金额"] = (merged["总分成"] * merged["分成比例"] / merged["人数"]).round(2)
        
        # 过滤有效数据
        result = merged.loc[(merged["分成金额"] > 0) & merged["人员"].notnull(), ["人员", "分成金额"]]
        
        # 按人员汇总分成金额
        summary = result.groupby("人员", as_index=False)["分成金额"].sum()
        
        # 获取昨天的日期，格式为YYYY-MM-DD
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 添加日期字段
        summary["日期"] = yesterday_str
        
        return summary.reset_index(drop=True)
    
    def upload_to_jdy(self):
        appId = "67c280b7c6387c4f4afd50ae"
        entryId = "67d7097d08e5f607c4cfd028"
        # 调用并发库asyncio执行批量上传
        final_data = self.everyone_money()
        asyncio.run(self.jdy.batch_create(app_id=appId, entry_id=entryId, source_data=final_data))

if __name__ == '__main__':
    dividend = Dividend()
    data = dividend.video_dividend()
    data.to_excel('视频分红.xlsx',index=False)
    # dividend.upload_to_jdy()