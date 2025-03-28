import pickle
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException

class Xhs:
    def __init__(self, url, cookies_file="xhs.pkl"):
        self.url = url
        self.data_center_url = "https://creator.xiaohongshu.com/creator/notemanage?roleType=creator"
        self.cookies_file = cookies_file
        self.driver = webdriver.Edge(service=Service(EdgeChromiumDriverManager().install()))
        self.driver.maximize_window()

    def load_cookies(self):
        """Load cookies for automatic login"""
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

    def _post_login_flow(self):
        """Post-login operations"""
        self.go_to_data_center()
        self.close_all_popups()
        self.click_tgzp_tab()
        self.click_post_list_tab()
        self.input_start_date()
        self.input_end_date()
        self.click_export_data_button()

    def _manual_login(self):
        """Manual login flow"""
        print("❌ Cookies not found, manual login required")
        self.driver.get(self.url)
        input("Please complete login and press Enter to continue...")
        self._save_cookies()
        self._post_login_flow()

    def _save_cookies(self):
        """Save cookies after manual login"""
        with open(self.cookies_file, "wb") as cookie_file:
            # Filter out potentially problematic cookies like csrf tokens
            cookies = [c for c in self.driver.get_cookies() if c['name'] not in ['passport_csrf_token']]
            pickle.dump(cookies, cookie_file)
        print("✅ Cookies saved successfully")

    def go_to_data_center(self):
        """Navigate to the data center page"""
        print("🚀 Navigating to data center...")
        self.driver.get(self.data_center_url)
        self.wait_for_page_ready()

    def wait_for_page_ready(self, timeout=30):
        """Wait for the page to be fully loaded"""
        WebDriverWait(self.driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == 'complete'
        )
        print("📄 Page loaded successfully")

    def close_all_popups(self):
        """Close all types of popups"""
        print("🛡️ Starting popup defense mechanism")
        self._close_pagination_popup()
        self._close_knowledge_popup()
        self._close_floating_ads()
        self._close_final_modal()

    def _close_pagination_popup(self):
        """Close pagination-related popups"""
        max_attempts = 5
        closed_pages = 0
        
        for attempt in range(max_attempts):
            button_texts = ["下一页", "立即体验", "我知道了", "完成"]
            locator = (By.XPATH, f"//button[contains(.,'{button_texts[0]}') or contains(.,'{button_texts[1]}') or contains(.,'{button_texts[2]}') or contains(.,'{button_texts[3]}')]")
            
            if self._try_close_popup(locator, f"Pagination page {closed_pages + 1}", timeout=2):
                closed_pages += 1
                time.sleep(1.2)
                if closed_pages >= 3:
                    break
            else:
                break

    def _close_knowledge_popup(self):
        """Close standalone 'I understand' popups"""
        locator = (By.XPATH, "//button[contains(.,'我知道了') and @type='button']")
        self._try_close_popup(locator, "Knowledge popup")

    def _close_floating_ads(self):
        """Close floating advertisements"""
        locator = (By.XPATH, "//div[contains(@class,'banner-close')] | //div[contains(@class,'close-icon')]")
        self._try_close_popup(locator, "Floating ads")

    def _close_final_modal(self):
        """Close final modal or overlay"""
        locator = (By.XPATH, "//div[@class='modal-close'] | //div[contains(@class,'mask-close')]")
        self._try_close_popup(locator, "Final modal", timeout=1.5)

    def _try_close_popup(self, locator, name, timeout=8):
        """Attempt to close a popup with retry logic"""
        try:
            btn = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable(locator)
            )
            self.driver.execute_script("""
                arguments[0].scrollIntoView({behavior: 'instant', block: 'center'});
                arguments[0].click();
            """, btn)
            print(f"✅ Closed {name}")
            return True
        except TimeoutException:
            print(f"⏳ {name} not detected")
            return False
        except Exception as e:
            print(f"❌ Failed to close {name}: {str(e)}")
            return False

    def click_export_data_button(self):
        """
        稳定版：点击导出数据按钮
        """
        locator = (
                    By.XPATH,
                    "//div[contains(@class,'container-ttkmFy')]"
                    "//button[.//span[text()='导出数据']]"
                )
        # locatorx = (
        #             By.XPATH,
        #             "//div[@id='semiTabPanel1']"
        #             "//button[.//span[text()='导出数据']]"
        #         )

        try:
            self.wait_for_page_ready(timeout=30)
            self.close_all_popups()
            time.sleep(2)  # 额外等待按钮加载完成

            # 等待按钮存在
            button = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located(locator)
            )

            # 滚动到按钮
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(1)

            # 使用JavaScript强制点击
            self.driver.execute_script("arguments[0].click();", button)

            print("✅ 已成功点击「导出数据」按钮（稳定版）")
        except TimeoutException:
            print("❌ 等待超时：按钮未能出现或不可点击")
            # 保存页面源码用于调试
            with open("export_button_debug.html", "w", encoding='utf-8') as f:
                f.write(self.driver.page_source)
            print("🔍 页面源码已保存为 export_button_debug.html，供进一步排查")
        except Exception as e:
            print(f"❌ 点击「导出数据」按钮异常：{e}")



    def _smart_click(self, element):
        """Intelligent click strategy to handle various scenarios"""
        try:
            element.click()
        except Exception:
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
            self.driver.execute_script("arguments[0].click();", element)

    def _scroll_away(self):
        """Safe scrolling to remove obstructions"""
        self.driver.execute_script("window.scrollBy(0, 100);")
        time.sleep(0.3)
        self.driver.execute_script("window.scrollBy(0, -50);")

    def wait_for_element_visible(self, locator, timeout=20):
        """Wait for an element to be visible"""
        try:
            return WebDriverWait(self.driver, timeout).until(
                EC.visibility_of_element_located(locator)
            )
        except TimeoutException:
            print(f"⏳ Element not visible: {locator}")
            return None

    def wait_for_element_clickable(self, locator, timeout=20):
        """Wait for an element to be clickable"""
        try:
            return WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable(locator)
            )
        except TimeoutException:
            print(f"⏳ Element not clickable: {locator}")
            return None
    
    def click_tgzp_tab(self):
        """
        点击“投稿作品”Tab
        """
        # 1. 构造定位器
        post_works_locator = (By.XPATH, f"//div[@id='semiTab1' and text()='投稿作品']")

        # 2. 等待元素可点击
        post_works_element = self.wait_for_element_clickable(post_works_locator, timeout=10)

        # 3. 如果能找到，就点击
        if post_works_element:
            self._smart_click(post_works_element)
            print(f"✅ 点击'投稿作品'成功")
            time.sleep(1)
        else:
            print(f"❌ 未能找到'投稿作品'Tab，请检查定位是否正确")

    def click_post_list_tab(self):
        """
        点击「投稿列表」按钮
        """
        locator = locator = locator = (By.XPATH,
                                            "//div[@id='semiTabPanel1']//span["
                                            "contains(@class, 'douyin-creator-pc-radio-addon') "
                                            "and normalize-space(text())='投稿列表'"
                                            "]"
                                        )
        try:
            # 等待元素可点击
            element = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable(locator)
            )
            # 点击元素
            self._smart_click(element)
            print("✅ 点击「投稿列表」成功")
            time.sleep(1)
        except TimeoutException:
            print("❌ 等待超时：未找到或无法点击「投稿列表」，请检查定位和页面状态")
        except Exception as e:
            print(f"❌ 点击「投稿列表」异常：{e}")

    def input_start_date(self):
        locator = (By.XPATH, "//div[@id='semiTabPanel1']//input[@placeholder='开始日期']")

        # 计算日期逻辑
        ninety_days_ago = datetime.now() - timedelta(days=90)
        min_date = datetime(2025, 3, 4)
        target_date = max(ninety_days_ago, min_date).strftime("%Y-%m-%d")

        try:
            self.wait_for_page_ready(timeout=20)
            time.sleep(1)

            # 等待元素加载完毕
            input_element = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located(locator)
            )

            # 解除readonly属性
            self.driver.execute_script("arguments[0].removeAttribute('readonly')", input_element)
            time.sleep(0.5)

            # 使用JavaScript强制设置输入框的值
            self.driver.execute_script("arguments[0].value = arguments[1];", input_element, target_date)

            # 主动触发前端框架监听的input/change事件，确保前端框架数据也被更新
            self.driver.execute_script("""
                arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
                arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
            """, input_element)

            print(f"✅ 成功输入日期（强制覆盖默认值）：{target_date}")
        except TimeoutException:
            print("❌ 超时未能定位到「开始日期」输入框")
        except Exception as e:
            print(f"❌ 输入日期时异常：{e}")

    def input_end_date(self):
        locator = (By.XPATH, "//div[@id='semiTabPanel1']//input[@placeholder='结束日期']")

        # 计算日期逻辑
        yesterday = datetime.now() - timedelta(days=1)
        target_date = yesterday.strftime("%Y-%m-%d")

        try:
            self.wait_for_page_ready(timeout=20)
            time.sleep(1)

            # 等待元素加载完毕
            input_element = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located(locator)
            )

            # 解除readonly属性
            self.driver.execute_script("arguments[0].removeAttribute('readonly')", input_element)
            time.sleep(0.5)

            # 使用JavaScript强制设置输入框的值
            self.driver.execute_script("arguments[0].value = arguments[1];", input_element, target_date)

            # 主动触发前端框架监听的input/change事件，确保前端框架数据也被更新
            self.driver.execute_script("""
                arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
                arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
            """, input_element)

            print(f"✅ 成功输入结束日期（强制覆盖默认值）：{target_date}")
        except TimeoutException:
            print("❌ 超时未能定位到「结束日期」输入框")
        except Exception as e:
            print(f"❌ 输入结束日期时异常：{e}")


    def run(self):
        """Main execution flow"""
        try:
            self.load_cookies()
            time.sleep(10)  # Brief pause to ensure everything stabilizes
        except Exception as e:
            print(f"❗ Unknown error occurred: {str(e)}")
        finally:
            self.driver.quit()
            print("🛑 Browser closed")
        time.sleep(5)

if __name__ == "__main__":
    douyin = Xhs("https://creator.xiaohongshu.com/creator/notemanage?roleType=creator")
    douyin.run()
    