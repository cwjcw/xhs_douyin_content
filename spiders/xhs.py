import os
import pickle
import time
import glob
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


class Xhs:
    def __init__(self, url, cookies_file="xh.pkl", download_path=r"E:\douyin_xhs_data\xhs"):
        self.url = url
        self.cookies_file = cookies_file
        self.data_center_url = "https://creator.xiaohongshu.com/creator/notemanage?roleType=creator"
        self.download_path = download_path

        # 配置 Edge 下载路径
        edge_options = Options()
        prefs = {
            "download.default_directory": self.download_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        edge_options.add_experimental_option("prefs", prefs)

        self.driver = webdriver.Edge(
            service=Service(EdgeChromiumDriverManager().install()),
            options=edge_options
        )
        self.driver.maximize_window()

    def run(self):
        try:
            self.load_cookies()
            time.sleep(10)
        except Exception as e:
            print(f"❗ Unknown error occurred: {str(e)}")
        finally:
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
            self._manual_login()

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
        # self.close_all_popups()
        # self.click_tgzp_tab()
        # self.click_post_list_tab()
        # self.input_start_date()
        # self.input_end_date()
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

    def close_all_popups(self):
        print("🛡️ Starting popup defense mechanism")
        self._close_generic_popup(["下一页", "立即体验", "我知道了", "完成"])
        self._try_close_popup((By.XPATH, "//div[contains(@class,'banner-close')]"), "Floating ads")
        self._try_close_popup((By.XPATH, "//div[contains(@class,'mask-close')]"), "Final modal")

    def _close_generic_popup(self, texts):
        for text in texts:
            locator = (By.XPATH, f"//button[contains(.,'{text}')]")
            self._try_close_popup(locator, f"Popup: {text}")

    def _try_close_popup(self, locator, name, timeout=8):
        try:
            btn = WebDriverWait(self.driver, timeout).until(EC.element_to_be_clickable(locator))
            self.driver.execute_script("arguments[0].click();", btn)
            print(f"✅ Closed {name}")
            return True
        except:
            print(f"⏳ {name} not found or not clickable")
            return False

    def click_tgzp_tab(self):
        locator = (By.XPATH, "//div[@id='semiTab1' and text()='投稿作品']")
        el = self.wait_for_element_clickable(locator)
        if el:
            el.click()
            print("✅ 点击“投稿作品”成功")

    def click_post_list_tab(self):
        locator = (By.XPATH, "//span[contains(text(),'投稿列表')]")
        el = self.wait_for_element_clickable(locator)
        if el:
            el.click()
            print("✅ 点击“投稿列表”成功")

    def input_start_date(self):
        self._input_date("//input[@placeholder='开始日期']", max(datetime.now() - timedelta(days=90), datetime(2025, 3, 4)))

    def input_end_date(self):
        self._input_date("//input[@placeholder='结束日期']", datetime.now() - timedelta(days=1))

    def _input_date(self, xpath, date_obj):
        locator = (By.XPATH, f"//div[@id='semiTabPanel1']{xpath}")
        target_date = date_obj.strftime("%Y-%m-%d")
        try:
            el = WebDriverWait(self.driver, 15).until(EC.presence_of_element_located(locator))
            self.driver.execute_script("arguments[0].removeAttribute('readonly')", el)
            self.driver.execute_script("arguments[0].value = arguments[1];", el, target_date)
            self.driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true })); arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", el)
            print(f"✅ 设置日期成功：{target_date}")
        except:
            print(f"❌ 设置日期失败：{target_date}")

    def click_export_data_button(self):
        locator = (By.XPATH, "//button[.//span[text()='导出数据']]")
        try:
            self.wait_for_page_ready()
            time.sleep(2)
            button = WebDriverWait(self.driver, 20).until(EC.presence_of_element_located(locator))
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            self.driver.execute_script("arguments[0].click();", button)
            print("✅ 点击“导出数据”成功")
        except:
            print("❌ 未能成功点击“导出数据”按钮")

    def wait_for_element_clickable(self, locator, timeout=20):
        try:
            return WebDriverWait(self.driver, timeout).until(EC.element_to_be_clickable(locator))
        except:
            return None

    def merge_and_cleanup_xlsx_files(self):
        keyword = "笔记列表明细表"
        all_files = glob.glob(os.path.join(self.download_path, f"*{keyword}*.xlsx"))

        if not all_files:
            print("⚠️ 没有找到任何包含关键字的 Excel 文件")
            return

        all_dfs = []
        for file in all_files:
            try:
                df = pd.read_excel(file)
                df['来源文件'] = os.path.basename(file)
                all_dfs.append(df)
            except Exception as e:
                print(f"❌ 读取失败：{file}，错误：{e}")

        if all_dfs:
            result = pd.concat(all_dfs, ignore_index=True)
            output_path = os.path.join(self.download_path, "汇总笔记列表明细表.xlsx")
            result.to_excel(output_path, index=False)
            print(f"✅ 汇总成功，已保存：{output_path}")

            for file in all_files:
                try:
                    os.remove(file)
                    print(f"🗑️ 已删除文件：{file}")
                except Exception as e:
                    print(f"❌ 删除失败：{file}，错误：{e}")
        else:
            print("⚠️ 没有可用的数据进行汇总")

# ==========================
# 主程序入口
# ==========================

if __name__ == "__main__":
    cookie_files = [
        # "xhs_336283533.pkl",
        # "xhs_345630498.pkl",
        # "xhs_348492471.pkl",
        # "xhs_348499654.pkl",
        # "xhs_485899710.pkl",
        # "xhs_672578639.pkl",
        # "xhs_713752297I.pkl",
        # "xhs_1159005953.pkl",
        "xhs_2690270173.pkl",
        "xhs_4235229252.pkl",
        "xhs_26501332556.pkl",
        "xhs_yayun92.pkl"
    ]

    base_dir = os.path.dirname(os.path.abspath(__file__))
    download_path = r"E:\douyin_xhs_data\xhs"

    for cookie_file in cookie_files:
        print(f"\n================ 处理：{cookie_file} ================\n")
        full_path = os.path.join(base_dir, cookie_file)
        douyin = Xhs(
            url="https://creator.xiaohongshu.com/creator/notemanage?roleType=creator",
            cookies_file=full_path,
            download_path=download_path
        )
        douyin.run()

    # ⬇️ 所有账号处理完后，合并数据
    douyin.merge_and_cleanup_xlsx_files()
