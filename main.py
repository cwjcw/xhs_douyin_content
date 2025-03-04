import pickle
import time
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.microsoft import EdgeChromiumDriverManager

# 设置 WebDriver 路径
driver = webdriver.Edge(service=Service(EdgeChromiumDriverManager().install()))

# 目标网站
url = "https://creator.douyin.com/creator-micro/home"

# 读取保存的 cookies，如果存在
cookies_file = "cookies.pkl"
try:
    with open(cookies_file, "rb") as cookie_file:
        cookies = pickle.load(cookie_file)
        driver.get(url)
        # 添加 cookies 到浏览器
        for cookie in cookies:
            driver.add_cookie(cookie)
        # 刷新页面使 cookies 生效
        driver.refresh()
        print("Cookies 已加载，自动登录成功！")
except FileNotFoundError:
    print("没有找到保存的 cookies，需要手动登录。")

    # 如果没有保存的 cookies，打开登录页面
    driver.get(url)
    input("请手动登录并按 Enter 键继续...")

    # 登录后保存 cookies
    cookies = driver.get_cookies()
    with open(cookies_file, "wb") as cookie_file:
        pickle.dump(cookies, cookie_file)
    print("Cookies 已保存！")

# 保持浏览器开启，直到用户手动关闭
input("登录成功，按 Enter 键退出...")

# 关闭浏览器
driver.quit()
