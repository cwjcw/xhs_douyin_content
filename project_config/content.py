import pandas as pd
import os

# 读取上传的 Excel 文件路径
file_path = r'E:\Downloads\内容管理_20250329191924.xlsx'

# 读取 Excel 文件
df = pd.read_excel(file_path)

# 检查是否包含目标列
if '半成品内容提供' not in df.columns:
    raise ValueError("❌ Excel中不包含 '半成品内容提供' 字段，请检查字段名是否正确。")

# 使用 explode 前，先将 '半成品内容提供' 按 , 分割成列表
df['半成品内容提供'] = df['半成品内容提供'].astype(str).str.split(',')

# 展开成多行
expanded_df = df.explode('半成品内容提供')

# 去除内容前后空格（可选）
expanded_df['半成品内容提供'] = expanded_df['半成品内容提供'].str.strip()

# 重置索引
expanded_df = expanded_df.reset_index(drop=True)

# 保存为新的 Excel 文件
output_path = r'E:\Downloads\内容管理_new.xlsx'
expanded_df.to_excel(output_path, index=False)

print(f"✅ 已成功处理并保存为：{output_path}")
