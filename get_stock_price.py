import pandas as pd
import akshare as ak
import time
import os
from datetime import datetime

def load_stock_price_cache(cache_file):
    """
    加载股票价格缓存文件
    
    参数:
        cache_file: 缓存文件路径
        
    返回:
        缓存的股票价格DataFrame，如果文件不存在则返回空DataFrame
    """
    if os.path.exists(cache_file):
        try:
            cache_df = pd.read_csv(cache_file,dtype={'代码': str})
            print(f"已加载股票价格缓存，共 {len(cache_df)} 条记录")
            return cache_df
        except Exception as e:
            print(f"加载缓存文件失败: {e}")
    
    # 如果文件不存在或加载失败，返回空DataFrame
    return pd.DataFrame(columns=['代码', '名称', '最新价', '更新时间'])

def save_stock_price_cache(stock_data, cache_file):
    """
    保存股票价格到缓存文件
    
    参数:
        stock_data: 股票价格DataFrame
        cache_file: 缓存文件路径
    """
    # 添加更新时间列
    stock_data['更新时间'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        stock_data.to_csv(cache_file, index=False, encoding='utf-8-sig')
        print(f"股票价格缓存已保存到 {cache_file}，共 {len(stock_data)} 条记录")
    except Exception as e:
        print(f"保存缓存文件失败: {e}")

def process_stock_data(input_df):
    """
    处理股票数据，计算股息率并判断是否超过start值
    
    参数:
        input_df: 包含股票信息的DataFrame
        
    返回:
        处理后的DataFrame
    """
    # 创建输入数据的副本，避免修改原始数据
    df = input_df.copy()
    
    # 创建新列用于存储股价和股息率
    df['price'] = None
    df['dividend_yield'] = None
    df['exceed_start'] = None

    # 缓存文件路径
    cache_dir = 'data/cache'
    os.makedirs(cache_dir, exist_ok=True)
    a_stock_cache_file = f'{cache_dir}/a_stock_price_cache.csv'
    hk_stock_cache_file = f'{cache_dir}/hk_stock_price_cache.csv'
    
    # 加载缓存
    a_stock_cache = load_stock_price_cache(a_stock_cache_file)
    hk_stock_cache = load_stock_price_cache(hk_stock_cache_file)
    
    # 判断缓存是否需要更新（超过1小时则更新）
    update_a_cache = True
    update_hk_cache = True
    a_stock_data = None
    hk_stock_data = None
    
    if not a_stock_cache.empty and '更新时间' in a_stock_cache.columns:
        last_update = pd.to_datetime(a_stock_cache['更新时间'].iloc[0])
        if (datetime.now() - last_update).total_seconds() < 3600:  # 1小时 = 3600秒
            update_a_cache = False
            a_stock_data = a_stock_cache
            print("使用A股缓存数据")
    
    if not hk_stock_cache.empty and '更新时间' in hk_stock_cache.columns:
        last_update = pd.to_datetime(hk_stock_cache['更新时间'].iloc[0])
        if (datetime.now() - last_update).total_seconds() < 3600:  # 1小时 = 3600秒
            update_hk_cache = False
            hk_stock_data = hk_stock_cache
            print("使用港股缓存数据")

    # 如果需要更新缓存，则获取最新数据
    if update_a_cache:
        try:
            print("正在获取A股数据...")
            a_stock_data = ak.stock_zh_a_spot_em()
            if a_stock_data is not None and not a_stock_data.empty:
                save_stock_price_cache(a_stock_data, a_stock_cache_file)
                print(f"成功获取A股数据，共 {len(a_stock_data)} 条记录")
            else:
                print("获取A股数据失败：返回数据为空")
                if not a_stock_cache.empty:
                    a_stock_data = a_stock_cache
                    print("使用A股缓存数据")
        except Exception as e:
            print(f"获取A股数据失败: {e}")
            # 如果获取失败但有缓存，则使用缓存
            if not a_stock_cache.empty:
                a_stock_data = a_stock_cache
                print("使用A股缓存数据")
    
    if update_hk_cache:
        try:
            print("正在获取港股数据...")
            hk_stock_data = ak.stock_hk_spot_em()
            if hk_stock_data is not None and not hk_stock_data.empty:
                save_stock_price_cache(hk_stock_data, hk_stock_cache_file)
                print(f"成功获取港股数据，共 {len(hk_stock_data)} 条记录")
            else:
                print("获取港股数据失败：返回数据为空")
                if not hk_stock_cache.empty:
                    hk_stock_data = hk_stock_cache
                    print("使用港股缓存数据")
        except Exception as e:
            print(f"获取港股数据失败: {e}")
            # 如果获取失败但有缓存，则使用缓存
            if not hk_stock_cache.empty:
                hk_stock_data = hk_stock_cache
                print("使用港股缓存数据")

    # 检查是否有可用的股票数据
    if (a_stock_data is None or a_stock_data.empty) and (hk_stock_data is None or hk_stock_data.empty):
        print("错误：无法获取股票数据，也没有可用的缓存数据")
        return df

    # 遍历每一行，处理股票数据
    for index, row in df.iterrows():
        stock_id = str(row['id']) if not pd.isna(row['id']) else ''
        area = str(row['area']) if not pd.isna(row['area']) else ''
        
        # 跳过没有id的股票
        if stock_id == '':
            continue
        
        try:
            # 根据交易所确定股票代码格式和数据源
            stock_code = None
            stock_data = None
            
            if '深交所' in area or (stock_id.startswith('0') or stock_id.startswith('3')):
                if a_stock_data is not None and not a_stock_data.empty:
                    stock_code = stock_id
                    stock_data = a_stock_data
            elif '上交所' in area or (stock_id.startswith('6')):
                if a_stock_data is not None and not a_stock_data.empty:
                    stock_code = stock_id
                    stock_data = a_stock_data
            elif '港股' in stock_id or '港股' in area:
                if hk_stock_data is not None and not hk_stock_data.empty:
                    # 处理港股代码，确保是5位数
                    stock_code = stock_id.replace('港股', '')
                    try:
                        stock_code = f"{int(stock_code):05d}"
                    except ValueError:
                        print(f"无法解析港股代码: {stock_id}")
                        continue
                    stock_data = hk_stock_data
            
            if stock_code is None or stock_data is None:
                print(f"跳过股票 {stock_id}：无法确定交易所或没有对应的数据源")
                continue
            
            # 查找对应股票的行情数据
            # 对于A股，直接匹配代码
            # 对于港股，需要检查代码是否包含在代码列中
            if '港股' in stock_id or '港股' in area:
                # 港股代码可能有不同格式，尝试多种匹配方式
                stock_info = hk_stock_data[hk_stock_data['代码'].astype(str).str.contains(stock_code)]
            else:
                # A股代码直接匹配
                stock_info = stock_data[stock_data['代码'].astype(str) == stock_code]
            
            if not stock_info.empty:
                # 获取最新价格
                price = stock_info['最新价'].values[0]
                df.at[index, 'price'] = price
                
                # 计算股息率
                if not pd.isna(row['dividend']) and price > 0:
                    dividend_yield = (row['dividend'] / price) * 100
                    df.at[index, 'dividend_yield'] = round(dividend_yield, 2)
                    
                    # 判断股息率是否超过start值
                    if not pd.isna(row['start']):
                        df.at[index, 'exceed_start'] = "是" if dividend_yield >= row['start'] else "否"
            else:
                print(f"未找到股票 {stock_id} 的价格数据")
            
        except Exception as e:
            print(f"处理股票 {stock_id} 时出错: {e}")
    
    return df

if __name__ == "__main__":
    # 获取当前日期和小时
    now = datetime.now()
    date_hour_mark = now.strftime("%Y%m%d_%H")
    
    # 读取原始CSV文件
    input_df = pd.read_csv('data/High_Dividend_Companies_fixed.csv')
    # 清理列名中的不可见字符
    input_df.columns = [col.replace('\u200b', '') for col in input_df.columns]
    
    # 处理数据
    result_df = process_stock_data(input_df)
    
    # 保存结果到新的CSV文件，文件名包含日期和小时
    output_filename = f'data/High_Dividend_Companies_Analysis_{date_hour_mark}.csv'
    result_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
    
    print(f"分析完成，结果已保存到 {output_filename}")
    
    # 使用prettytable创建表格
    from prettytable import PrettyTable
    
    # 定义ANSI颜色代码
    class Colors:
        GREEN = '\033[92m'  # 绿色
        YELLOW = '\033[93m'  # 黄色
        BLUE = '\033[94m'  # 蓝色
        RED = '\033[91m'  # 红色
        BOLD = '\033[1m'  # 粗体
        UNDERLINE = '\033[4m'  # 下划线
        END = '\033[0m'  # 结束颜色
    
    # 筛选股息率超过start的股票并按领域和股息率排序
    exceed_stocks = result_df[result_df['exceed_start'] == "是"].copy()
    if not exceed_stocks.empty:
        # 计算超出的绝对值
        exceed_stocks['exceed_value'] = exceed_stocks.apply(
            lambda row: row['dividend_yield'] - row['start'] 
            if not pd.isna(row['start']) and not pd.isna(row['dividend_yield']) else None, 
            axis=1
        )
        
        # 先按cate1排序，然后在每个分类内按股息率从高到低排序
        exceed_stocks = exceed_stocks.sort_values(by=['cate1', 'dividend_yield'], ascending=[True, False])
        
        print("\n" + "=" * 80)
        print(f"{Colors.GREEN}{Colors.BOLD}★★★ 股息率超过start的股票 ★★★{Colors.END}".center(90))
        print("=" * 80)
        
        # 定义要显示的列和表头
        headers = ['股票ID', '名称', '一级分类', '二级分类', '特点', '最新价', '分红', '股息率(%)', 'start值', '超出值(%)']
        
        # 按cate1分组
        grouped_cate1 = exceed_stocks.groupby('cate1')
        
        for cate1, group_df in grouped_cate1:
            # 为每个一级分类创建一个表格
            table = PrettyTable()
            table.field_names = headers
            
            # 设置表格样式
            table.align = 'l'  # 左对齐
            table.border = True
            table.header = True
            
            # 添加数据行
            for _, row in group_df.iterrows():
                table.add_row([
                    str(row['id']),
                    str(row.get('company', 'N/A')),
                    str(row.get('cate1', '')),
                    str(row.get('cate2', '')),
                    str(row.get('features', '')),
                    f"{row['price']:.2f}" if not pd.isna(row['price']) else 'N/A',
                    f"{row['dividend']:.4f}" if not pd.isna(row['dividend']) else 'N/A',
                    f"{row['dividend_yield']:.2f}" if not pd.isna(row['dividend_yield']) else 'N/A',
                    f"{row['start']:.2f}" if not pd.isna(row['start']) else 'N/A',
                    f"{row['exceed_value']:.2f}" if not pd.isna(row['exceed_value']) else 'N/A'
                ])
            
            # 打印一级分类名称和表格
            print(f"\n{Colors.GREEN}{Colors.BOLD}【{cate1 or '未分类'}】:{Colors.END}")
            print(table)
    else:
        print("\n没有股息率超过start的股票")
    
    # 筛选股息率距离start有1以内的股票（接近但未超过）并按领域和股息率排序
    close_stocks = result_df[(result_df['exceed_start'] == "否") & 
                            (~pd.isna(result_df['dividend_yield'])) & 
                            (~pd.isna(result_df['start'])) & 
                            (result_df['start'] - result_df['dividend_yield'] <= 1) & 
                            (result_df['start'] - result_df['dividend_yield'] > 0)].copy()
    
    if not close_stocks.empty:
        # 先按cate1排序，然后在每个分类内按股息率从高到低排序
        close_stocks = close_stocks.sort_values(by=['cate1', 'dividend_yield'], ascending=[True, False])
        
        print("\n" + "=" * 80)
        print(f"{Colors.YELLOW}{Colors.BOLD}○○○ 股息率接近start值(差距≤1%)的股票 ○○○{Colors.END}".center(90))
        print("=" * 80)
        
        # 定义要显示的列和表头
        headers = ['股票ID', '名称', '一级分类', '二级分类', '特点', '最新价', '分红', '股息率(%)', 'start值', '差距(%)']
        
        # 按cate1分组
        grouped_cate1 = close_stocks.groupby('cate1')
        
        for cate1, group_df in grouped_cate1:
            # 为每个一级分类创建一个表格
            table = PrettyTable()
            table.field_names = headers
            
            # 设置表格样式
            table.align = 'l'  # 左对齐
            table.border = True
            table.header = True
            
            # 添加数据行
            for _, row in group_df.iterrows():
                diff = row['start'] - row['dividend_yield']
                table.add_row([
                    str(row['id']),
                    str(row.get('company', 'N/A')),
                    str(row.get('cate1', '')),
                    str(row.get('cate2', '')),
                    str(row.get('features', '')),
                    f"{row['price']:.2f}" if not pd.isna(row['price']) else 'N/A',
                    f"{row['dividend']:.4f}" if not pd.isna(row['dividend']) else 'N/A',
                    f"{row['dividend_yield']:.2f}" if not pd.isna(row['dividend_yield']) else 'N/A',
                    f"{row['start']:.2f}" if not pd.isna(row['start']) else 'N/A',
                    f"{diff:.2f}"
                ])
            
            # 打印一级分类名称和表格
            print(f"\n{Colors.YELLOW}{Colors.BOLD}【{cate1 or '未分类'}】:{Colors.END}")
            print(table)
    else:
        print("\n没有股息率接近start值的股票")