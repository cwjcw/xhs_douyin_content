# douyin_video
# 作用
自动抓取抖音，小红书创作者中心里的每条视频的播放，完播，点击，2s跳出，播放时长，点赞，分享，评论，收藏，主页访问，粉丝增量等数据

# 创建项目参数文件夹和文件
- 新建project_config文件夹，在文件夹内创建project.py文件，输入以下内容（请把文件夹路径替换为你自己的）：
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

# 请把生成的pkl文件放到spiders文件夹中，并将其文件名放到以下列表中
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
