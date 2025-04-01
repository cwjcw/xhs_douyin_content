import pickle
import time
import os, sys
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from webdriver_manager.microsoft import EdgeChromiumDriverManager
# 获取当前文件的路径
current_dir = os.path.dirname(os.path.abspath(__file__))

# 获取 `project_config` 的路径
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))

# 添加到 sys.path
sys.path.append(parent_dir)

# 现在可以导入
from project_config.project import dy_file_path


class Douyin:
    def __init__(self, url, account_name, cookies_file):
        
        self.url = url
        self.account_name = account_name
        self.cookies_file = cookies_file
        self.download_folder = dy_file_path  # 指定下载文件夹
        # Edge 浏览器选项
        options = webdriver.EdgeOptions()
        prefs = {
            "download.default_directory": self.download_folder,  # 设置默认下载目录
            "download.prompt_for_download": False,  # 关闭下载提示框
            "download.directory_upgrade": True,  # 允许升级目录权限
            "safebrowsing.enabled": True  # 启用安全浏览
        }
        options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Edge(service=Service(EdgeChromiumDriverManager().install()), options=options)
        self.driver.maximize_window()
        

        


    def load_cookies(self):
        try:
            with open(self.cookies_file, "rb") as cookie_file:
                cookies = pickle.load(cookie_file)
                self.driver.get(self.url)
                self.driver.delete_all_cookies()
                for cookie in cookies:
                    if 'expiry' in cookie:
                        cookie['expiry'] = int(cookie['expiry'])
                    self.driver.add_cookie(cookie)
                self.driver.refresh()
                print(f"✅ [{self.account_name}] 登录成功")
                self._post_login_flow()
        except FileNotFoundError:
            self._manual_login()

    def _manual_login(self):
        print(f"❌ [{self.account_name}] 无cookie文件，请手动登录")
        self.driver.get(self.url)
        input("完成登录后按Enter继续...")
        self._save_cookies()
        self._post_login_flow()

    def _save_cookies(self):
        with open(self.cookies_file, "wb") as cookie_file:
            cookies = [c for c in self.driver.get_cookies() if c['name'] not in ['passport_csrf_token']]
            pickle.dump(cookies, cookie_file)
        print(f"✅ [{self.account_name}] cookies已保存")

    def _post_login_flow(self):
        # 此处调用你原先实现的具体业务逻辑
        print(f"🚀 [{self.account_name}] 执行数据抓取任务...")
        # 例如：
        self.go_to_data_center()
        self.close_all_popups()
        self.click_tgzp_tab()
        self.click_post_list_tab()
        self.input_start_date()
        self.input_end_date()
        self.click_export_data_button()
        
    def close_browser(self):
        self.driver.quit()
        print(f"🛑 [{self.account_name}] 浏览器已关闭")

    def run(self):
        try:
            self.load_cookies()
            time.sleep(5)  # 调整等待时间
        except Exception as e:
            print(f"❗ [{self.account_name}] 异常发生: {str(e)}")
        finally:
            self.close_browser()

if __name__ == "__main__":
    accounts = [
        {"name": "momoling", "cookie_file": "douyin_44698605892.pkl"},
        {"name": "在丽江的摄影师小薯", "cookie_file": "douyin_bojuegz.pkl"},
        {"name": "铂爵在厦门", "cookie_file": "douyin_bojuexiamen.pkl"},
        {"name": "铂爵小相册", "cookie_file": "douyin_NCHQYX520.pkl"},
        {"name": "冰糖葫芦娃", "cookie_file": "douyin_53693141223.pkl"},
        {"name": "铂爵旅拍", "cookie_file": "douyin_BJ_520.pkl"},
        # 更多账号
    ]

    url = "https://creator.douyin.com/creator-micro/home"

    for account in accounts:
        print(f"\n🔄 正在处理账号: {account['name']}")
        douyin_bot = Douyin(url, account["name"], account["cookie_file"])
        douyin_bot.run()
        time.sleep(10)  # 账号间隔时间，避免请求频率过高
