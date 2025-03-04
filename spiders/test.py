import pickle
import time
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.microsoft import EdgeChromiumDriverManager

class Douyin:
    def __init__(self, url, cookies_file="cookies_douyin.pkl"):
        self.url = url
        self.data_center_url = "https://creator.douyin.com/creator-micro/data-center/content"
        self.cookies_file = cookies_file
        self.driver = webdriver.Edge(service=Service(EdgeChromiumDriverManager().install()))
        self.driver.maximize_window()

    def load_cookies(self):
        """加载Cookies并登录"""
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
                print("✅ Cookies已加载，自动登录成功！")
                self._post_login_flow()
        except FileNotFoundError:
            self._manual_login()

    def _post_login_flow(self):
        """登录后统一操作"""
        self.go_to_data_center()
        self.close_all_popups()
        if self.safe_click_tougao():  
            self.click_publish_list()

    def _manual_login(self):
        """手动登录并保存Cookies"""
        print("❌ 未找到Cookies，需要手动登录")
        self.driver.get(self.url)
        input("请完成登录后按Enter继续...")
        self._save_cookies()
        self._post_login_flow()

    def _save_cookies(self):
        """保存Cookies"""
        with open(self.cookies_file, "wb") as cookie_file:
            cookies = [c for c in self.driver.get_cookies() if c['name'] not in ['passport_csrf_token']]
            pickle.dump(cookies, cookie_file)
        print("✅ 关键Cookies已保存")

    def go_to_data_center(self):
        """进入数据中心"""
        print(f"🚀 进入数据中心...")
        self.driver.get(self.data_center_url)
        self.wait_for_page_ready()

    def wait_for_page_ready(self, timeout=30):
        """等待页面加载完成"""
        WebDriverWait(self.driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == 'complete'
        )
        print("📄 页面加载完成")

    def close_all_popups(self):
        """关闭所有弹窗"""
        print("🛡️ 关闭弹窗...")
        popup_xpaths = [
            "//button[contains(text(),'下一页')]",  # 先关闭引导页
            "//div[contains(@class,'modal-close') or contains(@class,'mask-close')]",  # 关闭大弹窗“×”
            "//button[contains(text(),'我知道了')]"  # 关闭“我知道了”
        ]
        for xpath in popup_xpaths:
            self._try_close_popup(xpath)

    def _try_close_popup(self, xpath, timeout=5):
        """尝试关闭弹窗"""
        try:
            btn = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            self.driver.execute_script("arguments[0].click();", btn)
            print(f"✅ 关闭弹窗: {xpath}")
        except:
            print(f"⏳ 未检测到弹窗: {xpath}")

    def safe_click_tougao(self):
        """点击 '投稿作品' """
        locator = (By.XPATH, "//div[@role='tab' and normalize-space()='投稿作品']")
        return self._retry_click(locator, "投稿作品")

    def click_publish_list(self):
        """点击 '投稿列表'"""
        print("🔄 尝试点击 '投稿列表' ...")

        # 确保主内容区域加载完成
        content_locator = (By.XPATH, "//div[contains(@class,'data-center-content')]")
        self.wait_for_element_visible(content_locator, 15)

        # 找到 '投稿列表' 并点击
        sub_tab_locator = (By.XPATH, "//span[contains(text(),'投稿列表')]")
        return self._retry_click(sub_tab_locator, "投稿列表", max_attempts=5)

    def _retry_click(self, locator, element_name, max_attempts=3):
        """多次尝试点击"""
        for attempt in range(max_attempts):
            self.close_all_popups()
            try:
                element = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(locator)
                )
                self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
                self.driver.execute_script("arguments[0].click();", element)
                print(f"✅ 成功点击 {element_name}")
                return True
            except:
                print(f"⏳ 尝试 {attempt + 1}/{max_attempts}: 失败")
                time.sleep(2)
        print(f"❌ 无法点击 {element_name}")
        return False

    def wait_for_element_visible(self, locator, timeout=20):
        """等待元素可见"""
        try:
            return WebDriverWait(self.driver, timeout).until(
                EC.visibility_of_element_located(locator)
            )
        except:
            print(f"⏳ 元素不可见: {locator}")
            return None

    def run(self):
        """主流程"""
        try:
            self.load_cookies()
            time.sleep(3)
        except Exception as e:
            print(f"❗ 发生未知错误: {str(e)}")
        finally:
            self.driver.quit()
            print("🛑 浏览器已关闭")

if __name__ == "__main__":
    douyin = Douyin("https://creator.douyin.com/creator-micro/home")
    douyin.run()
