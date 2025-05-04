# douyin_video
# 作用
自动抓取抖音，小红书创作者中心里的每条视频的播放，完播，点击，2s跳出，播放时长，点赞，分享，评论，收藏，主页访问，粉丝增量等数据

# 获取缓存文件（pkl文件）
- 如果已经有了，请直接复制到pkl文件夹中，命名方式
    - 抖音：douyin + _ + 其他任意字符（最好是账号名），如douyin_123456.pkl
    - 小红书：xhs + _ + 其他任意字符（最好是账号名）, 如xhs_123456.pkl
- 如果没有pkl文件，第一次需要扫码登录，登陆后回到代码界面输入回车，即可继续。然后把pkl文件剪切到pkl文件夹

# 用法
## 安装requirements.txt
- pip install requirements.txt
## 直接运行main.py即可
- 如果只是仅仅对抓取抖音和小红书后台内容有兴趣，直接运行spiders文件夹下的douyin.py或xhs.py即可。

## 数据处理部分，在data_processing文件夹中
- 可以先从后台下载对应的excel文件，清空标题以外的内容，命名为yesterday.xlsx
- 系统会自动下载data.xlsx,并在处理完后，自动将data.xlsx命名为yesterday.xlsx
