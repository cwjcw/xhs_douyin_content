import os

# 存放excel下载的路径
file_path = r'E:\\'
driver_path = r'G:\New Code\douyin_video\project_config\msedgedriver.exe'

xhs_file_path = r'E:\douyin_xhs_data\xhs'
xhs_data_path = os.path.join(xhs_file_path,'汇总笔记列表明细表.xlsx')
xhs_yesterday_path = r'E:\douyin_xhs_data\xhs\yesterday.xlsx'

dy_file_path = r'E:\douyin_xhs_data\douyin'
dy_data_path = os.path.join(dy_file_path,'douyin_汇总数据.xlsx')
dy_yesterday_path = os.path.join(dy_file_path,'yesterday.xlsx')

# 存放sql文件的路径
custom_count_sql = r'G:\New Code\douyin_video\sql\douyin_customer.sql'
xhs_custom_count_sql = r'G:\New Code\douyin_video\sql\xhs.sql'

# 字段映射关系（name到label）
video_content = {
    "_widget_1741257105163": "账号名称",
    "_widget_1741257105165": "账号ID",
    "_widget_1740798082550": "是否完整内容",
    "_widget_1740798082567": "完整内容提供",
    "_widget_1740798082568": "半成品内容提供",
    "_widget_1740798082569": "剪辑",
    "_widget_1740798082570": "发布运营",
    "_widget_1740646149825": "正片标题",
    "_widget_1740798082556": "正片链接",
    "_widget_1740646149824": "正片ID",
    "_widget_1740646149826": "提交日期",
    "_widget_1741934971937": "来源门店/部门",
    "_widget_1740655279753": "正片说明",
    "_widget_1740655279752": "正片封面",
    "_widget_1740656251325": "数量"
}

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

if __name__ == 'main':
    print(dy_file_path)

