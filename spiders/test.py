import pickle
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException

class Douyin:
    def __init__(self, url, cookies_file="cookies_douyin.pkl"):
        self.url = url
        self.data_center_url = "https://creator.douyin.com/creator-micro/data-center/content"
        self.cookies_file = cookies_file

        # ---------- 新增：指定下载路径 ----------
        edge_options = webdriver.EdgeOptions()
        prefs = {
            "download.default_directory": r"E:\Downloads",   # 这里指定默认下载目录
            "download.prompt_for_download": False,           # 不提示下载窗口
            "download.directory_upgrade": True               # 允许目录升级
        }
        edge_options.add_experimental_option("prefs", prefs)
        # -------------------------------------

        # 启动Edge浏览器并使用自定义options
        self.driver = webdriver.Edge(
            service=Service(EdgeChromiumDriverManager().install()),
            options=edge_options
        )
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
            locator = (
                By.XPATH, 
                f"//button[contains(.,'{button_texts[0]}') or "
                f"contains(.,'{button_texts[1]}') or "
                f"contains(.,'{button_texts[2]}') or "
                f"contains(.,'{button_texts[3]}')]"
            )
            
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
        """Click the '导出数据' (Export Data) button"""
        print("🔄 Attempting to click '导出数据' button...")

        # Define a precise locator for the "导出数据" button based on the HTML snippet
        export_button_locator = (
            By.XPATH, 
            "//button[contains(@class,'douyin-creator-pc-button-tertiary') and "
            "contains(@class,'douyin-creator-pc-button-with-icon')]"
            "//span[contains(@class,'douyin-creator-pc-button-content-right') and text()='导出数据']"
        )

        # Ensure the page is ready and the button is visible/clickable
        self.wait_for_page_ready()
        self.close_all_popups()

        # Retry clicking the button with robust error handling
        for attempt in range(5):
            try:
                button = WebDriverWait(self.driver, 15).until(
                    EC.element_to_be_clickable(export_button_locator)
                )
                self._smart_click(button)
                print("🎯 Successfully clicked '导出数据' button")
                return True
            except TimeoutException:
                print(f"⏳ Attempt {attempt + 1}: '导出数据' button not clickable yet")
                self.close_all_popups()
                self._scroll_away()
            except ElementClickInterceptedException:
                print(f"🛡️ Attempt {attempt + 1}: Button is obstructed")
                self.close_all_popups()
                self._scroll_away()
        
        print("❌ Failed to click '导出数据' button after multiple attempts")
        return False

    def _smart_click(self, element):
        """Intelligent click strategy to handle various scenarios"""
        try:
            element.click()
        except Exception:
            self.driver.execute_script(
                "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", 
                element
            )
            self.driver.execute_script("arguments[0].click();", element)

    def _scroll_away(self):
        """Safe scrolling to remove obstructions"""
        self.driver.execute_script("window.scrollBy(0, 100);")
        time.sleep(0.3)
        self.driver.execute_script("window.scrollBy(0, -50);")

    def run(self):
        """Main execution flow"""
        try:
            self.load_cookies()
            time.sleep(3)  # Brief pause to ensure everything stabilizes
        except Exception as e:
            print(f"❗ Unknown error occurred: {str(e)}")
        finally:
            self.driver.quit()
            print("🛑 Browser closed")


if __name__ == "__main__":
    douyin = Douyin("https://creator.douyin.com/creator-micro/home")
    douyin.run()
