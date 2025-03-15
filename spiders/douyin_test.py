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

class Douyin:
    def __init__(self, url, cookies_file="cookies_douyin_bjlp.pkl"):
        self.url = url
        self.data_center_url = "https://creator.douyin.com/creator-micro/data-center/content"
        self.cookies_file = cookies_file
        self.driver = webdriver.Edge(service=Service(EdgeChromiumDriverManager().install()))
        self.driver.maximize_window()

    def load_cookies(self):
        """增强版cookies加载"""
        try:
            with open(self.cookies_file, "rb") as cookie_file:
                cookies = pickle.load(cookie_file)
                self.driver.get(self.url)
                self.driver.delete_all_cookies()
                for cookie in cookies:
                    # expiry需要是整数
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
            # 如果想在“投稿列表”页面点击“导出数据”，可在此调用
            self.click_export_data()

    def _manual_login(self):
        """增强手动登录流程"""
        print("❌ 未找到cookies，需要手动登录")
        self.driver.get(self.url)
        input("请完成登录后按Enter继续...")  # 这里等待用户手动登录
        self._save_cookies()
        self._post_login_flow()

    def _save_cookies(self):
        """保存cookies增强"""
        with open(self.cookies_file, "wb") as cookie_file:
            # 过滤掉可能会影响后续登录的csrf token
            cookies = [c for c in self.driver.get_cookies() if c['name'] not in ['passport_csrf_token']]
            pickle.dump(cookies, cookie_file)
        print("✅ 关键cookies已保存")

    def go_to_data_center(self):
        """安全跳转数据中心"""
        print(f"🚀 正在进入数据中心...")
        self.driver.get(self.data_center_url)
        self.wait_for_page_ready()

    def wait_for_page_ready(self, timeout=30):
        """智能等待页面就绪"""
        WebDriverWait(self.driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == 'complete'
        )
        print("📄 页面加载完成")

    def close_all_popups(self):
        """关闭所有类型弹窗（专项优化第三页）"""
        print("🛡️ 启动弹窗防御机制")
        self._close_pagination_popup()
        self._close_knowledge_popup()
        self._close_floating_ads()
        self._close_final_modal()

    def _close_pagination_popup(self):
        """专项优化分页处理（处理1-3页所有情况）"""
        max_attempts = 5
        closed_pages = 0
        
        for attempt in range(max_attempts):
            # 可以尝试一次匹配多个引导按钮
            button_texts = ["下一页", "立即体验", "我知道了", "完成"]
            locator = (By.XPATH, 
                "//button[contains(.,'{}')]".format("') or contains(.,'".join(button_texts)))
            
            if self._try_close_popup(locator, f"分页第{closed_pages+1}页", timeout=2):
                closed_pages += 1
                time.sleep(1.2)
                # 假设最多就3个“下一页”弹窗
                if closed_pages >= 3:
                    break
            else:
                break

    def _close_knowledge_popup(self):
        """独立处理游离的我知道了弹窗"""
        locator = (By.XPATH, "//button[contains(.,'我知道了') and @type='button']")
        self._try_close_popup(locator, "独立教学弹窗")

    def _close_floating_ads(self):
        """关闭悬浮广告"""
        locator = (By.XPATH, "//div[contains(@class,'banner-close')] | //div[contains(@class,'close-icon')]")
        self._try_close_popup(locator, "悬浮广告")

    def _close_final_modal(self):
        """最终弹窗清理"""
        locator = (By.XPATH, "//div[@class='modal-close'] | //div[contains(@class,'mask-close')]")
        self._try_close_popup(locator, "残留蒙层", timeout=1.5)

    def _try_close_popup(self, locator, name, timeout=8):
        """优化点击逻辑，返回是否成功关闭"""
        try:
            btn = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable(locator)
            )
            self.driver.execute_script("""
                arguments[0].scrollIntoView({behavior: 'instant', block: 'center'});
                arguments[0].click();
            """, btn)
            print(f"✅ 已关闭{name}")
            return True
        except TimeoutException:
            print(f"⏳ 未检测到{name}")
            return False
        except Exception as e:
            print(f"❌ 关闭{name}失败: {str(e)}")
            return False

    def safe_click_tougao(self):
        """增强版投稿点击"""
        locator = (By.XPATH, "//div[@role='tab' and normalize-space()='投稿作品']")
        for attempt in range(3):
            try:
                if element := self.wait_for_element_clickable(locator, 15):
                    self._smart_click(element)
                    print("🎯 投稿作品点击成功")
                    return True
            except ElementClickInterceptedException:
                print(f"🛡️ 检测到遮挡，第{attempt+1}次重试...")
                self.close_all_popups()
                self._scroll_away()
        print("❌ 多次点击失败")
        return False

    def click_publish_list(self):
        """点击 '投稿列表'"""
        print("🔄 尝试点击 '投稿列表' ...")

        # 确保主内容区域加载完成
        content_locator = (By.XPATH, "//div[contains(@class,'data-center-content')]")
        self.wait_for_element_visible(content_locator, 15)

        # 找到 '投稿列表' 并点击
        sub_tab_locator = (By.XPATH, "//span[contains(text(),'投稿列表')]")
        return self._retry_click(sub_tab_locator, "投稿列表", max_attempts=5)

    def click_export_data(self):
        """点击 '导出数据' 按钮"""
        print("🔄 尝试点击 '导出数据' 按钮 ...")
        # 注意这里修正了 'douyn' → 'douyin'
        locator = (By.XPATH,
            "//button[contains(@class,'douyin-creator-pc-button-tertiary') "
            "and contains(@class,'douyin-creator-pc-button-with-icon') "
            "and .//span[contains(@class,'x-semi-prop-children') and text()='导出数据']]"
        )
        # 或者更简单些：只要保证文本命中即可
        # locator = (By.XPATH, "//button[.//span[text()='导出数据']]")

        if self._retry_click(locator, "导出数据", max_attempts=5):
            print("✅ 已成功点击“导出数据”按钮")
        else:
            print("❌ 点击“导出数据”按钮失败")

    def _retry_click(self, locator, element_name, max_attempts=3):
        """带重试机制的点击方法"""
        for attempt in range(max_attempts):
            self.close_all_popups()
            try:
                element = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(locator)
                )
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                self.driver.execute_script("arguments[0].click();", element)
                print(f"✅ 成功点击{element_name}")
                return True
            except TimeoutException:
                print(f"⏳ 第{attempt+1}次尝试: 等待{element_name}超时")
            except ElementClickInterceptedException:
                print(f"🛡️ 第{attempt+1}次尝试: {element_name}被遮挡")
                self._scroll_away()
        print(f"❌ 无法点击{element_name}")
        return False

    def _smart_click(self, element):
        """智能点击策略"""
        try:
            element.click()
        except:
            self.driver.execute_script(
                "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element
            )
            self.driver.execute_script("arguments[0].click();", element)

    def _scroll_away(self):
        """安全滚动操作"""
        self.driver.execute_script("window.scrollBy(0, 100);")
        time.sleep(0.3)
        self.driver.execute_script("window.scrollBy(0, -50);")

    def wait_for_element_visible(self, locator, timeout=20):
        """等待元素可见（增强版）"""
        try:
            return WebDriverWait(self.driver, timeout).until(
                EC.visibility_of_element_located(locator)
            )
        except TimeoutException:
            print(f"⏳ 元素不可见: {locator}")
            return None

    def wait_for_element_clickable(self, locator, timeout=20):
        """等待元素可点击（增强版）"""
        try:
            return WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable(locator)
            )
        except TimeoutException:
            print(f"⏳ 元素不可点击: {locator}")
            return None

    def run(self):
        """主流程增强"""
        try:
            self.load_cookies()
            time.sleep(3)
            # 如果不想在 _post_login_flow() 中点击导出数据
            # 也可以在这里显式调用 self.click_export_data()
        except Exception as e:
            print(f"❗ 发生未知错误: {str(e)}")
        finally:
            self.driver.quit()
            print("🛑 浏览器已关闭")


if __name__ == "__main__":
    douyin = Douyin("https://creator.douyin.com/creator-micro/home")
    douyin.run()
