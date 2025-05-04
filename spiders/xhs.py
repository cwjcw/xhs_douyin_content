import os, sys
import pickle
import time
import glob
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains

# 自动添加项目根目录到 sys.path
from utils.init_path import setup_project_root
setup_project_root()
from project_config.project import (
    xhs_file_path, driver_path, pkl_path, get_full_cookie_paths
)

class Xhs:
    def __init__(self, url, cookies_file, download_path=xhs_file_path):
        self.url = url
        self.cookies_file = cookies_file
        self.data_center_url = "https://creator.xiaohongshu.com/statistics/data-analysis"
        self.download_path = download_path

        edge_options = Options()
        prefs = {
            "download.default_directory": str(self.download_path),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        edge_options.add_experimental_option("prefs", prefs)

        if self.cookies_file:
            print(f"使用本地 EdgeDriver 路径: {driver_path}")
            self.driver = webdriver.Edge(
                service=Service(driver_path),
                options=edge_options
            )
            self.driver.maximize_window()
        else:
            self.driver = None

    def run(self):
        try:
            self.load_cookies()
            time.sleep(10)
        except Exception as e:
            print(f"❗ Unknown error occurred: {str(e)}")
        finally:
            if self.driver:
                self.driver.quit()
                print("🛑 Browser closed")
        time.sleep(5)

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
                print("✅ Cookies loaded, auto-login successful!")
                self._post_login_flow()
        except FileNotFoundError:
            print(f"❌ Cookie 文件未找到: {self.cookies_file}")
        except Exception as e:
            print(f"❌ 加载 Cookie 失败: {e}")

    def _manual_login(self):
        print("❌ Cookies not found, manual login required")
        self.driver.get(self.url)
        input("Please complete login and press Enter to continue...")
        self._save_cookies()
        self._post_login_flow()

    def _save_cookies(self):
        with open(self.cookies_file, "wb") as cookie_file:
            cookies = [c for c in self.driver.get_cookies() if c['name'] not in ['passport_csrf_token']]
            pickle.dump(cookies, cookie_file)
        print("✅ Cookies saved successfully")

    def _post_login_flow(self):
        self.go_to_data_center()
        self.click_export_data_button()

    def go_to_data_center(self):
        print("🚀 Navigating to data center...")
        self.driver.get(self.data_center_url)
        self.wait_for_page_ready()

    def wait_for_page_ready(self, timeout=30):
        WebDriverWait(self.driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == 'complete'
        )
        print("📄 Page loaded successfully")

    def click_export_data_button(self):
        locator = (By.XPATH, "//button[.//span[contains(.,'导出数据')]]")
        try:
            self.wait_for_page_ready()
            time.sleep(2)
            button = WebDriverWait(self.driver, 20).until(EC.presence_of_element_located(locator))
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            self.driver.execute_script("arguments[0].click();", button)
            print("✅ 点击“导出数据”成功")
        except Exception as e:
            print(f"❌ 未能成功点击“导出数据”按钮：{e}")

    def merge_and_cleanup_xlsx_files(self):
        keyword = "笔记列表明细表"
        all_files = glob.glob(os.path.join(self.download_path, f"*{keyword}*.xlsx"))

        if not all_files:
            print("⚠️ 没有找到任何包含关键字的 Excel 文件")
            return None

        all_dfs = []
        for file in all_files:
            try:
                df = pd.read_excel(file, skiprows=1)
                df['来源文件'] = os.path.basename(file)
                all_dfs.append(df)
            except Exception as e:
                print(f"❌ 读取失败：{file}，错误：{e}")

        if all_dfs:
            result = pd.concat(all_dfs, ignore_index=True)
            if '首次发布时间' in result.columns:
                try:
                    result['首次发布时间'] = pd.to_datetime(
                        result['首次发布时间'].astype(str),
                        format='%Y年%m月%d日%H时%M分%S秒',
                        errors='coerce'
                    ).dt.strftime('%Y-%m-%d')
                    print("✅ 成功格式化“首次发布时间”为 YYYY-MM-DD")
                except Exception as e:
                    print(f"⚠️ 格式化“首次发布时间”失败：{e}")

            output_path = os.path.join(self.download_path, "汇总笔记列表明细表.xlsx")
            result.to_excel(output_path, index=False)
            print(f"✅ 汇总成功，已保存：{output_path}")

            for file in all_files:
                if os.path.basename(file) == os.path.basename(output_path):
                    continue
                try:
                    os.remove(file)
                    print(f"🗑️ 已删除文件：{file}")
                except Exception as e:
                    print(f"❌ 删除失败：{file}，错误：{e}")
            return result
        else:
            print("⚠️ 没有可用的数据进行汇总")
            return None

    @classmethod
    def run_all(cls):
        print("📊 开始运行 run_all()：处理所有 XHS 账号")
        full_paths = get_full_cookie_paths("xhs", pkl_path)
        print("🧾 Cookie 路径列表：")
        for p in full_paths:
            print(" -", p)

        if not full_paths:
            print("❌ 未找到任何 cookie 文件，任务终止")
            return

        for full_path in full_paths:
            try:
                print(f"\n================ 处理：{full_path} ================\n")
                account = cls(url="https://creator.xiaohongshu.com/statistics/data-analysis", cookies_file=full_path)
                account.run()
            except Exception as e:
                print(f"❌ 账号处理失败：{full_path}，错误：{e}")

        print("📁 准备合并 Excel 文件...")
        merged_instance = cls(url="https://creator.xiaohongshu.com/statistics/data-analysis", cookies_file="")
        final_df = merged_instance.merge_and_cleanup_xlsx_files()
        if final_df is not None:
            print("✅ XHS 数据采集成功，展示部分数据：")
            print(final_df.head())
        else:
            print("⚠️ XHS 数据采集未成功或无数据")

if __name__ == "__main__":
    Xhs.run_all()
