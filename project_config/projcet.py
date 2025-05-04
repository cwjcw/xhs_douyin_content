from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# 小红书路径
xhs_file_path = BASE_DIR / "xlsx_file" / "xhs"
xhs_data_path = xhs_file_path / "汇总笔记列表明细表.xlsx"
xhs_yesterday_path = xhs_file_path / "yesterday.xlsx"

# 抖音路径
dy_file_path = BASE_DIR / "xlsx_file" / "douyin"
dy_data_path = dy_file_path / "douyin_汇总数据.xlsx"
dy_yesterday_path = dy_file_path / "yesterday.xlsx"

# 驱动路径
driver_path = BASE_DIR / "project_config" / "msedgedriver.exe"

# Cookie 路径
pkl_path = BASE_DIR / "pkl"


# 字段映射关系（name到label）
fields = [
        {"label": "所属平台", "type": "combo"},
        {"label": "数据日期", "type": "datetime"},
        {"label": "作品名称", "type": "text"},
        {"label": "发布时间", "type": "datetime"},
        {"label": "体裁", "type": "text"},
        {"label": "审核状态", "type": "text"},
        {"label": "播放量", "type": "number"},
        {"label": "完播率", "type": "number"},
        {"label": "5s完播率", "type": "number"},
        {"label": "封面点击率", "type": "number"},
        {"label": "2s跳出率", "type": "number"},
        {"label": "平均播放时长", "type": "number"},
        {"label": "点赞量", "type": "number"},
        {"label": "分享量", "type": "number"},
        {"label": "评论量", "type": "number"},
        {"label": "收藏量", "type": "number"},
        {"label": "主页访问量", "type": "number"},
        {"label": "粉丝增量", "type": "number"},
    ]


xhs_cookie_list = [
        "xhs_336283533.pkl",
        "xhs_345630498.pkl",
        "xhs_348492471.pkl",
        "xhs_348499654.pkl",
        "xhs_485899710.pkl",
        # "xhs_672578639.pkl",
        "xhs_713752297I.pkl",
        "xhs_1159005953.pkl",
        "xhs_2690270173.pkl",
        "xhs_4235229252.pkl",
        "xhs_26501332556.pkl",
        "xhs_yayun92.pkl"
    ]

dy_cookie_list = [
    "douyin_44698605892.pkl",
    "douyin_bojuegz.pkl",
    "douyin_bojuexiamen.pkl",
    "douyin_NCHQYX520.pkl",
    "douyin_53693141223.pkl",
    "douyin_BJ_520.pkl"
]

if __name__ == '__main__':
    print(dy_file_path)

