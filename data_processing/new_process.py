import pandas as pd
import re

file_path = '内容管理.xlsx'
df = pd.read_excel(file_path)

# 总金额
platform_totals = {
    '抖音': 1400,
    '小红书': 3200
}

# 按平台统计每个data_id数量
platform_dataid_count = df.groupby('所属平台')['data_id'].nunique().to_dict()
# 得到每个data_id应该分配的总金额
dataid_amount_map = {}
for platform, total in platform_totals.items():
    count = platform_dataid_count.get(platform, 0)
    if count > 0:
        single_amount = total / count
        dataids = df[df['所属平台'] == platform]['data_id'].unique()
        for did in dataids:
            dataid_amount_map[(platform, did)] = round(single_amount, 8)  # 保留高精度

def split_names(name_str):
    if pd.isna(name_str) or str(name_str).strip() == '':
        return []
    return [n.strip() for n in re.split('[,，]', str(name_str)) if n.strip() != '']

records = []
# 用于累计每个data_id的实际分配金额
actual_amounts = {}

for idx, row in df.iterrows():
    platform = row['所属平台']
    data_id = row['data_id']
    base_amount = dataid_amount_map.get((platform, data_id), 0)
    common_info = {
        'data_id': data_id,
        '所属平台': platform,
        '账号ID': row.get('账号ID', ''),
        '账号名称': row.get('账号名称', ''),
        '正片标题': row.get('正片标题', ''),
        '正片链接': row.get('正片链接', ''),
        '正片ID': row.get('正片ID', '')
    }
    full_providers = split_names(row.get('完整内容提供', ''))
    half_providers = split_names(row.get('半成品内容提供', ''))
    editor = row.get('剪辑字段', '') if pd.notna(row.get('剪辑字段', '')) else ''
    publisher = row.get('发布运营', '') if pd.notna(row.get('发布运营', '')) else ''

    dist_list = []

    if full_providers:
        n_full = len(full_providers)
        full_money_total = round(base_amount * 0.6, 8)
        # 多人时，每人两位小数，最后一人补齐差额
        if n_full == 1:
            full_money_list = [round(full_money_total, 2)]
        else:
            unit = round(full_money_total / n_full, 2)
            full_money_list = [unit] * (n_full - 1)
            last_money = round(full_money_total - sum(full_money_list), 2)
            full_money_list.append(last_money)
        for i, name in enumerate(full_providers):
            dist_list.append((name, full_money_list[i]))
        # 发布运营 40%
        if publisher:
            dist_list.append((publisher, round(base_amount * 0.4, 2)))
    else:
        # 半成品内容提供 40%
        n_half = len(half_providers)
        half_money_total = round(base_amount * 0.4, 8)
        if n_half > 0:
            if n_half == 1:
                half_money_list = [round(half_money_total, 2)]
            else:
                unit = round(half_money_total / n_half, 2)
                half_money_list = [unit] * (n_half - 1)
                last_money = round(half_money_total - sum(half_money_list), 2)
                half_money_list.append(last_money)
            for i, name in enumerate(half_providers):
                dist_list.append((name, half_money_list[i]))
        # 剪辑字段 20%
        if editor:
            dist_list.append((editor, round(base_amount * 0.2, 2)))
        # 发布运营 40%
        if publisher:
            dist_list.append((publisher, round(base_amount * 0.4, 2)))

    # 累加每个data_id分配金额，方便调试和校准
    amount_sum = sum([x[1] for x in dist_list])
    actual_amounts.setdefault((platform, data_id), 0)
    actual_amounts[(platform, data_id)] += amount_sum

    for name, amount in dist_list:
        records.append({**common_info, '姓名': name, '金额': amount})

result_df = pd.DataFrame(records)
result_df.to_excel('内容管理_分配结果_精确对齐.xlsx', index=False)

# 校验分配金额是否严格等于平台总额
total_dy = result_df[result_df['所属平台'] == '抖音']['金额'].sum()
total_xhs = result_df[result_df['所属平台'] == '小红书']['金额'].sum()
print('抖音分配总金额:', total_dy)
print('小红书分配总金额:', total_xhs)
print('全部分配总金额:', result_df['金额'].sum())

# 校验每个data_id的金额分配是否与理论值相等
diffs = []
for (platform, data_id), expected in dataid_amount_map.items():
    actual = actual_amounts.get((platform, data_id), 0)
    if abs(actual - expected) > 0.01:
        diffs.append((platform, data_id, expected, actual))
if diffs:
    print('下列data_id分配金额与理论值有差异：')
    for x in diffs:
        print(x)
else:
    print('所有data_id分配金额都严格对齐！')

print('分配完成，已保存为内容管理_分配结果_精确对齐.xlsx')
