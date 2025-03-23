import pickle
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class Douyin:
    def __init__(self, url, cookies_file="douyin_BJ_520.pkl"):
        self.url = url
        self.data_center_url = "https://creator.douyin.com/creator-micro/data-center/content"
        self.cookies_file = cookies_file
        self.driver = webdriver.Edge(service=Service(EdgeChromiumDriverManager().install()))
        self.driver.maximize_window()

    def load_cookies(self):
        """加载Cookies实现自动登录"""
        try:
            with open(self.cookies_file, "rb") as cookie_file:
                cookies = pickle.load(cookie_file)
                self.driver.get(self.url)
                time.sleep(2)  # 等待基础页面加载
                self.driver.delete_all_cookies()
                for cookie in cookies:
                    if 'expiry' in cookie:
                        del cookie['expiry']  # 移除过期时间避免类型错误
                    self.driver.add_cookie(cookie)
                self.driver.refresh()
                print("✅ Cookies加载成功，自动登录完成！")
                self._post_login_flow()
        except (FileNotFoundError, EOFError):
            self._manual_login()

    def _post_login_flow(self):
        """登录后操作流程"""
        self.go_to_data_center()
        self.close_all_popups()
        self.click_tgzp_tab()
        self.click_post_list_tab()
        self.input_dates()
        self.click_export_data_button()

    def _manual_login(self):
        """人工登录处理"""
        print("❌ 未找到Cookie文件，请手动登录...")
        self.driver.get(self.url)
        input("请完成登录操作，按Enter继续...")
        self._save_cookies()
        self._post_login_flow()

    def _save_cookies(self):
        """保存有效Cookies"""
        with open(self.cookies_file, "wb") as cookie_file:
            # 过滤掉敏感Cookie
            cookies = [c for c in self.driver.get_cookies() 
                      if c['name'] not in ['passport_csrf_token', 'sessionid']]
            pickle.dump(cookies, cookie_file)
        print("✅ 登录状态已保存")

    def go_to_data_center(self):
        """导航到数据中心"""
        print("🚀 正在进入数据中心...")
        self.driver.get(self.data_center_url)
        self.wait_for_page_ready(timeout=45)  # 数据页面加载较慢

    def wait_for_page_ready(self, timeout=30):
        """增强版页面加载检测"""
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == 'complete'
                and d.find_element(By.CSS_SELECTOR, '#semiTab1').is_displayed()
            )
            print("📄 页面加载完成")
        except TimeoutException:
            print("⚠️ 页面加载超时，但继续执行")

    def close_all_popups(self):
        """关闭所有类型弹窗"""
        print("🛡️ 启动弹窗防御机制...")
        self._close_pagination_popup()
        self._close_knowledge_popup()
        self._close_floating_ads()
        self._close_final_modal()

    # 弹窗关闭方法保持不变...

    def click_tgzp_tab(self):
        """点击「投稿作品」标签"""
        try:
            tab = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.ID, 'semiTab1'))
            )
            self.driver.execute_script("arguments[0].click();", tab)
            print("✅ 成功切换至投稿作品")
            time.sleep(1.5)
        except Exception as e:
            self._take_debug_screenshot("tab_error")
            raise RuntimeError(f"无法点击投稿作品标签: {str(e)}")

    def click_post_list_tab(self):
        """点击「投稿列表」标签"""
        try:
            locator = (By.XPATH, "//span[contains(text(),'投稿列表')]/ancestor::div[@role='tab']")
            tab = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable(locator)
            )
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tab)
            tab.click()
            print("✅ 成功进入投稿列表")
            time.sleep(2)
        except Exception as e:
            self._take_debug_screenshot("post_list_error")
            raise RuntimeError(f"无法进入投稿列表: {str(e)}")

    def input_dates(self):
        """
        智能日期选择方案
        策略：优先使用JS直填，失败后启用传统点击方式
        """
        today = datetime.now()
        start_date = today - timedelta(days=89)  # 平台限制90天（含当天）
        end_date = today - timedelta(days=1)

        # 格式转换
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        print(f"📅 尝试设置日期范围：{start_str} 至 {end_str}")

        try:
            # 方案一：使用JavaScript直接输入
            self.driver.execute_script(f"""
                document.querySelector("input[placeholder='开始日期']").value = '{start_str}';
                document.querySelector("input[placeholder='结束日期']").value = '{end_str}';
            """)
            print("✅ 通过JS直填日期成功")
            time.sleep(1)
            
            # 触发日期变更事件
            ActionChains(self.driver).send_keys(Keys.TAB).perform()
            time.sleep(1.5)
            
            # 验证日期是否生效
            start_value = self.driver.find_element(By.CSS_SELECTOR, "input[placeholder='开始日期']").get_attribute('value')
            if start_value != start_str:
                raise ValueError("日期设置未生效")
                
        except Exception as js_error:
            print(f"⚠️ JS直填失败，启用备用方案: {str(js_error)}")
            self._fallback_date_selection(start_str, end_str)

    def _fallback_date_selection(self, start_date, end_date):
        """传统日期选择方案"""
        print("🔄 正在使用传统日期选择方式...")
        try:
            # 处理开始日期
            start_input = self.driver.find_element(By.CSS_SELECTOR, "input[placeholder='开始日期']")
            start_input.click()
            time.sleep(1)
            
            # 定位开始日期元素
            start_locator = f"//div[@aria-label='{start_date}' and contains(@class,'datepicker-day')]"
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, start_locator))
            ).click()
            print(f"✅ 已选择开始日期：{start_date}")

            # 处理结束日期
            end_input = self.driver.find_element(By.CSS_SELECTOR, "input[placeholder='结束日期']")
            end_input.click()
            time.sleep(1)
            
            # 定位结束日期元素
            end_locator = f"//div[@aria-label='{end_date}' and contains(@class,'datepicker-day')]"
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, end_locator))
            ).click()
            print(f"✅ 已选择结束日期：{end_date}")

            # 关闭日期选择器
            ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(1)

        except Exception as e:
            self._take_debug_screenshot("date_selection_fail")
            raise RuntimeError(f"传统日期选择失败: {str(e)}")

    def click_export_data_button(self):
        """增强版导出按钮点击"""
        print("📤 尝试导出数据...")
        try:
            # 使用多个特征定位导出按钮
            locator = (By.XPATH, '''//button[contains(.,'导出数据') and 
                        not(contains(@class,'disabled')) and 
                        not(@disabled)]''')
            
            button = WebDriverWait(self.driver, 25).until(
                EC.element_to_be_clickable(locator)
            )
            
            # 滚动到可视区域
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(0.5)
            
            # 使用ActionChain点击更可靠
            ActionChains(self.driver).move_to_element(button).click().perform()
            print("✅ 导出按钮点击成功")
            
            # 检查是否弹出导出选项
            WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, "//div[text()='导出数据范围']"))
            )
            print("⏳ 正在处理导出请求...")
            
            # 选择Excel格式
            excel_btn = self.driver.find_element(By.XPATH, "//span[contains(text(),'Excel')]/preceding-sibling::span")
            excel_btn.click()
            time.sleep(1)
            
            # 确认导出
            confirm_btn = self.driver.find_element(By.XPATH, "//button[.//span[text()='导出']]")
            confirm_btn.click()
            print("✅ 导出任务已提交，请稍后查看邮箱")

        except Exception as e:
            self._take_debug_screenshot("export_fail")
            raise RuntimeError(f"导出操作失败: {str(e)}")

    def _take_debug_screenshot(self, name):
        """保存调试截图"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.png"
        self.driver.save_screenshot(filename)
        print(f"📸 已保存调试截图：{filename}")

    def run(self):
        """主执行流程"""
        try:
            self.load_cookies()
            time.sleep(8)  # 等待数据加载
        except Exception as e:
            print(f"❗ 发生未知错误: {str(e)}")
        finally:
            self.driver.quit()
            print("🛑 浏览器已关闭")

if __name__ == "__main__":
    douyin = Douyin("https://creator.douyin.com/creator-micro/home")
    douyin.run()