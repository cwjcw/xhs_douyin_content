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
# 获取当前脚本所在目录 (data_processing目录)
current_dir = os.path.dirname(os.path.abspath(__file__))
# 获取项目根目录（即当前目录的上一级）
project_root = os.path.abspath(os.path.join(current_dir, ".."))
# 将项目根目录添加到sys.path中
if project_root not in sys.path:
    sys.path.append(project_root)
from project_config.project import xhs_cookie_list, xhs_file_path, driver_path


class Xhs:
    def __init__(self, url, cookies_file, download_path=xhs_file_path):
        self.url = url
        self.cookies_file = cookies_file
        self.data_center_url = "https://creator.xiaohongshu.com/statistics/data-analysis"
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

        # 当 cookies_file 为空时可以选择不初始化 driver（仅用于数据合并）
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
        # 可根据需要解开下面这些注释
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
        start_date_obj = max(datetime.now() - timedelta(days=90), datetime(2025, 3, 4))
        start_date_str = start_date_obj.strftime("%Y-%m-%d")

        # 更稳妥的定位方法（用contains而非精确匹配）
        locator = (By.XPATH, "//div[contains(text(),'笔记发布时间')]/../..//input[@placeholder='开始时间']")
        
        try:
            el = WebDriverWait(self.driver, 30).until(EC.presence_of_element_located(locator))
            print(f"🔍 元素找到: tag={el.tag_name}, placeholder={el.get_attribute('placeholder')}")

            # 确保元素可见
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
            time.sleep(1)
            self.driver.execute_script("arguments[0].removeAttribute('readonly')", el)

            actions = ActionChains(self.driver)
            actions.move_to_element(el).click().pause(0.5)
            actions.key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).pause(0.2)
            actions.send_keys(Keys.BACKSPACE).pause(0.2)
            actions.send_keys(start_date_str).pause(0.2)
            actions.send_keys(Keys.ENTER).perform()

            print(f"✅ 使用ActionChains设置开始日期成功：{start_date_str}")
        except Exception as e:
            print(f"❌ 使用ActionChains设置开始日期失败：{start_date_str}，错误：{e}")

    def input_end_date(self):
        end_date_obj = datetime.now() - timedelta(days=1)
        end_date_str = end_date_obj.strftime("%Y-%m-%d")

        locator = (By.XPATH, "//div[contains(text(),'笔记发布时间')]/../..//input[@placeholder='结束时间']")
        
        try:
            el = WebDriverWait(self.driver, 30).until(EC.presence_of_element_located(locator))
            print(f"🔍 元素找到: tag={el.tag_name}, placeholder={el.get_attribute('placeholder')}")

            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
            time.sleep(1)
            self.driver.execute_script("arguments[0].removeAttribute('readonly')", el)

            actions = ActionChains(self.driver)
            actions.move_to_element(el).click().pause(0.5)
            actions.key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).pause(0.2)
            actions.send_keys(Keys.BACKSPACE).pause(0.2)
            actions.send_keys(end_date_str).pause(0.2)
            actions.send_keys(Keys.ENTER).perform()

            print(f"✅ 使用ActionChains设置结束日期成功：{end_date_str}")
        except Exception as e:
            print(f"❌ 使用ActionChains设置结束日期失败：{end_date_str}，错误：{e}")




    def click_export_data_button(self):
        # 新 XPath，更灵活匹配含“导出数据”的按钮
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

    def wait_for_element_clickable(self, locator, timeout=20):
        try:
            return WebDriverWait(self.driver, timeout).until(EC.element_to_be_clickable(locator))
        except:
            return None

    def merge_and_cleanup_xlsx_files(self):
        """
        查找下载目录下所有包含关键词的 Excel 文件，合并成一个 DataFrame，
        同时保存合并后的 Excel 文件并删除单个文件。
        返回合并后的 DataFrame（若没有数据则返回 None）。
        """
        keyword = "笔记列表明细表"
        all_files = glob.glob(os.path.join(self.download_path, f"*{keyword}*.xlsx"))

        if not all_files:
            print("⚠️ 没有找到任何包含关键字的 Excel 文件")
            return None

        all_dfs = []
        for file in all_files:
            try:
                # 跳过第一行（说明性提示），从第二行开始读取
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
                # 跳过最终合并输出文件
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
    def process_all_accounts(cls, cookie_list):
        """
        处理多个账号：
        1. 根据传入的 cookie_list 和当前脚本目录，依次初始化 Xhs 实例并运行 run() 方法；
        2. 最后调用 merge_and_cleanup_xlsx_files() 合并所有下载的 Excel 文件，
           返回合并后的 DataFrame。
        """
        base_dir = os.path.dirname(os.path.abspath(__file__))
        for cookie_file in cookie_list:
            print(f"\n================ 处理：{cookie_file} ================\n")
            full_path = os.path.join(base_dir, cookie_file)
            account = cls(url="https://creator.xiaohongshu.com/statistics/data-analysis", cookies_file=full_path)
            account.run()
        # 调用一个临时实例来执行合并方法（下载目录为统一配置）
        merged_instance = cls(url="https://creator.xiaohongshu.com/statistics/data-analysis", cookies_file="")  
        df = merged_instance.merge_and_cleanup_xlsx_files()
        return df

# ==========================
# 主程序入口（调用 process_all_accounts 即可）
# ==========================

if __name__ == "__main__":
    # 调用 process_all_accounts 方法处理所有账号并返回合并后的 DataFrame
    final_df = Xhs.process_all_accounts(xhs_cookie_list)
    if final_df is not None:
        print("✅ 最终合并的 DataFrame：")
        print(final_df.head())
    else:
        print("⚠️ 未能生成合并的 DataFrame")
