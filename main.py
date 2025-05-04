from utils.init_path import setup_project_root
setup_project_root()

from spiders.xhs import Xhs
from spiders.douyin import Douyin

if __name__ == "__main__":
    print("📦 程序启动")

    try:
        print("▶ 开始处理 Douyin 数据")
        Douyin.run_all()
        print("✅ Douyin 处理完成")
    except Exception as e:
        print(f"❌ Douyin 出错: {e}")

    try:
        print("▶ 开始处理 XHS 数据")
        Xhs.run_all()
        print("✅ XHS 处理完成")
    except Exception as e:
        print(f"❌ XHS 出错: {e}")

    print("🏁 程序结束")
