import glob, time, os, sys
import pickle
import traceback
# 忽略 openpyxl 样式警告
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# 获取当前脚本所在目录 (data_processing目录)
current_dir = os.path.dirname(os.path.abspath(__file__))
# 获取项目根目录（即当前目录的上一级）
project_root = os.path.abspath(os.path.join(current_dir, ".."))
# 将项目根目录添加到sys.path中
if project_root not in sys.path:
    sys.path.append(project_root)
from project_config.project import driver_path, dy_file_path

# 多个 cookie 文件名，放在和 .py 脚本同一目录
cookie_list = [
    "douyin_44698605892.pkl",
    "douyin_bojuegz.pkl",
    "douyin_bojuexiamen.pkl",
    "douyin_NCHQYX520.pkl",
    "douyin_53693141223.pkl",
    "douyin_BJ_520.pkl"
]

class Douyin:
    def __init__(self, url, cookies_file):
        self.url = url
        self.cookies_file = cookies_file
        self.data_center_url = "https://creator.douyin.com/creator-micro/data-center/content"

        # 配置Edge下载目录
        edge_options = Options()
        edge_options.add_experimental_option("prefs", {
            "download.default_directory": dy_file_path,  # 设置下载目录
            "download.prompt_for_download": False,       # 不提示保存对话框
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        # 使用自定义的 driver_path
        self.driver = webdriver.Edge(
            service=Service(driver_path),
            options=edge_options
        )
        self.driver.maximize_window()

        # 强制设置下载路径
        # self.driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        #     "behavior": "allow",
        #     "downloadPath": dy_file_path
        # })

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
                print(f"✅ Loaded cookies from {self.cookies_file}")
                self._post_login_flow()
        except FileNotFoundError:
            print(f"❌ Cookie file not found: {self.cookies_file}")

    def _post_login_flow(self):
        self.driver.get(self.data_center_url)
        self.wait_for_page_ready()
        self.click_tgzp_tab()
        self.click_post_list_tab()
        # self.input_start_date()
        # self.input_end_date()
        self.click_export_data_button()

    def wait_for_page_ready(self, timeout=30):
        WebDriverWait(self.driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == 'complete'
        )

    def click_tgzp_tab(self):
        locator = (By.XPATH, "//div[@id='semiTab1' and text()='投稿作品']")
        try:
            element = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(locator)
            )
            self.driver.execute_script("arguments[0].click();", element)
            print("✅ 点击“投稿作品”成功")
        except Exception as e:
            print(f"❌ 点击“投稿作品”失败: {e}")

    def click_post_list_tab(self):
        locator = (By.XPATH, "//div[@id='semiTabPanel1']//span[contains(@class, 'douyin-creator-pc-radio-addon') and normalize-space(text())='投稿列表']")
        try:
            element = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(locator)
            )
            self.driver.execute_script("arguments[0].click();", element)
            print("✅ 点击“投稿列表”成功")
        except Exception as e:
            print(f"❌ 点击“投稿列表”失败: {e}")

    def input_start_date(self):
        locator = (By.XPATH, "//div[@id='semiTabPanel1']//input[@placeholder='开始日期']")
        ninety_days_ago = datetime.now() - timedelta(days=90)
        min_date = datetime(2025, 3, 4)
        target_date = max(ninety_days_ago, min_date).strftime("%Y-%m-%d")
        self._fill_date(locator, target_date, "开始日期")

    def input_end_date(self):
        locator = (By.XPATH, "//div[@id='semiTabPanel1']//input[@placeholder='结束日期']")
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self._fill_date(locator, target_date, "结束日期")

    def _fill_date(self, locator, date_str, label):
        try:
            input_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(locator)
            )
            self.driver.execute_script("arguments[0].removeAttribute('readonly')", input_element)
            self.driver.execute_script("arguments[0].value = arguments[1];", input_element, date_str)
            self.driver.execute_script("""
                arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
                arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
            """, input_element)
            print(f"✅ 输入{label}：{date_str}")
        except Exception as e:
            print(f"❌ 设置{label}失败: {e}")

    def click_export_data_button(self):
        locator = (By.XPATH, "//div[contains(@class,'container-ttkmFy')]//button[.//span[text()='导出数据']]")
        try:
            time.sleep(2)
            button = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located(locator)
            )
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            self.driver.execute_script("arguments[0].click();", button)
            print("✅ 点击导出数据成功")
        except Exception as e:
            print(f"❌ 点击导出数据失败: {e}")


    def run(self):
        try:
            print("🔄 开始加载 cookies 并登录…")
            self.load_cookies()
            time.sleep(10)
            print("✅ run() 执行完毕，无异常。")
        except Exception:
            print("❌ 运行出错，完整异常信息：")
            traceback.print_exc()
        finally:
            self.driver.quit()

def merge_xlsx_files(output_path):
    all_files = glob.glob(os.path.join(output_path, "*data*.xlsx"))
    df_list = []
    for file in all_files:
        try:
            df = pd.read_excel(file)
            df["来源文件"] = os.path.basename(file)
            df_list.append(df)
        except Exception as e:
            print(f"⚠️ 无法读取 {file}: {e}")

    if df_list:
        merged_df = pd.concat(df_list, ignore_index=True)
        final_file = os.path.join(output_path, "douyin_汇总数据.xlsx")
        merged_df.to_excel(final_file, index=False)
        print(f"📊 已成功导出汇总文件：{final_file}")
    else:
        print("❌ 没有可合并的xlsx文件")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))

    for cookie_file in cookie_list:
        full_cookie_path = os.path.join(script_dir, cookie_file)
        print(f"\n🌐 当前账号: {cookie_file}")
        douyin = Douyin("https://creator.douyin.com/creator-micro/home", full_cookie_path)
        douyin.run()
        print("⏳ 等待下载完成...")
        time.sleep(15)  # 视网络情况可增大等待时间

    print("\n📁 开始合并所有Excel文件...")
    merge_xlsx_files(dy_file_path)
