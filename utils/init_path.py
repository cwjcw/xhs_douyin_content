import sys
from pathlib import Path

def setup_project_root():
    """
    自动将项目根目录添加到 sys.path，确保可以导入项目模块（如 project_config）。
    """
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent  # 即 XHS_DOUYIN_CONTENT 路径
    sys_path_strs = [str(p) for p in sys.path]
    if str(project_root) not in sys_path_strs:
        sys.path.insert(0, str(project_root))
        print(f"✅ 添加项目路径: {project_root}")
    else:
        print(f"✅ 路径已存在: {project_root}")

