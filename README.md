# douyin_video
# 作用
自动抓取抖音，小红书创作者中心里的每条视频的播放，完播，点击，2s跳出，播放时长，点赞，分享，评论，收藏，主页访问，粉丝增量等数据

# 创建项目参数文件夹和文件
- 新建project_config文件夹，在文件夹内创建project.py文件，输入以下内容：
```python
import os

# 存放excel下载的路径
file_path = r'E:\\'

xhs_file_path = r'E:\douyin_xhs_data\xhs'
xhs_data_path = os.path.join(xhs_file_path,'汇总笔记列表明细表.xlsx')
xhs_yesterday_path = r'E:\douyin_xhs_data\xhs\yesterday.xlsx'

dy_file_path = r'E:\douyin_xhs_data\douyin'
dy_data_path = os.path.join(dy_file_path,'douyin_汇总数据.xlsx')
dy_yesterday_path = os.path.join(dy_file_path,'yesterday.xlsx')

# 存放sql文件的路径
custom_count_sql = r'G:\New Code\douyin_video\sql\douyin_customer.sql'
xhs_custom_count_sql = r'G:\New Code\douyin_video\sql\xhs.sql'

# 字段映射关系（name到label）,这个是我存入数据库时用的，可忽略
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

xhs_cookie_files = [
        "xhs_xxx.pkl",
        "xhs_xxxx.pkl",        
    ]

dy_cookie_list = [
    "douyin_05892.pkl",    
    "douyin_5223.pkl",
]
```
# 用法
## 爬虫部分，在spiders文件夹中
- 如果只是仅仅对抓取抖音和小红书后台内容有兴趣，直接运行spiders文件夹下的douyin.py和xhs.py即可。
- 第一次需要扫码登录，登陆后回到代码界面输入回车，即可继续。
## 数据处理部分，在data_processing文件夹中
- 可以先从后台下载对应的excel文件，清空标题以外的内容，命名为yesterday.xlsx
- 系统会自动下载data.xlsx,并在处理完后，自动将data.xlsx命名为yesterday.xlsx
