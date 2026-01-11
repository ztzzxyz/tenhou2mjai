#!/usr/bin/env python3
"""
脚本功能：
1. 接受两个参数：输入路径和输出路径
2. 遍历输入路径及其子目录中的所有文件
3. 根据文件名前8位日期创建年/月/日目录结构
4. 将JSON文件直接压缩为json.gz格式并保存到对应日期目录
"""

import os
import sys
import gzip
import re
import argparse
import shutil
from pathlib import Path
from datetime import datetime

def parse_date_from_filename(filename):
    """
    从文件名中提取日期部分
    文件名格式如：2019070419gm-00a9-0000-557e4086.json
    返回格式：datetime对象或None
    """
    # 使用正则表达式提取前8位数字作为日期
    match = re.match(r'^(\d{8})', filename)
    if match:
        date_str = match.group(1)
        try:
            # 将8位数字转换为datetime对象
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            return date_obj
        except ValueError:
            return None
    return None

def compress_json_file(input_file_path, output_base_dir, date_obj):
    """
    直接压缩JSON文件为gzip格式（二进制复制，不解析JSON内容）
    """
    try:
        # 构建输出路径：输出目录/年/月/日
        year = str(date_obj.year)
        month = f"{date_obj.month:02d}"
        day = f"{date_obj.day:02d}"
        
        output_dir = Path(output_base_dir) / year / month / day
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 构建输出文件名（保持原文件名，添加.gz扩展名）
        output_file_name = input_file_path.name + '.gz'
        output_file_path = output_dir / output_file_name
        
        # 使用二进制方式直接压缩文件，不解析JSON内容
        with open(input_file_path, 'rb') as f_in:
            with gzip.open(output_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        print(f"已压缩: {input_file_path} -> {output_file_path}")
        return True
        
    except Exception as e:
        print(f"处理文件 {input_file_path} 时出错: {e}")
        return False

def process_all_json_files(input_path, output_path):
    """
    处理所有JSON文件
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    
    # 统计变量
    total_files = 0
    processed_files = 0
    skipped_files = 0
    failed_files = 0
    
    # 遍历输入目录及其所有子目录
    for root, dirs, files in os.walk(input_path):
        root_path = Path(root)
        
        for file in files:
            file_path = root_path / file
            total_files += 1
            
            # 检查文件扩展名是否为.json
            if file_path.suffix.lower() != '.json':
                print(f"跳过非JSON文件: {file_path}")
                skipped_files += 1
                continue
            
            # 从文件名中提取日期
            date_obj = parse_date_from_filename(file)
            
            if date_obj is None:
                print(f"警告: 无法从文件名中提取日期，跳过文件: {file_path}")
                skipped_files += 1
                continue
            
            # 压缩文件
            if compress_json_file(file_path, output_path, date_obj):
                processed_files += 1
            else:
                failed_files += 1
    
    return total_files, processed_files, skipped_files, failed_files

def main():
    # 设置命令行参数解析
    parser = argparse.ArgumentParser(
        description='将指定目录中的JSON文件按日期直接压缩为gzip格式（不解析JSON内容）'
    )
    parser.add_argument('input_path', help='输入目录路径')
    parser.add_argument('output_path', help='输出目录路径')
    
    args = parser.parse_args()
    
    # 验证输入路径是否存在
    input_path = Path(args.input_path)
    if not input_path.exists():
        print(f"错误: 输入路径 '{input_path}' 不存在")
        sys.exit(1)
    
    # 确保输入路径是目录
    if not input_path.is_dir():
        print(f"错误: 输入路径 '{input_path}' 不是目录")
        sys.exit(1)
    
    # 创建输出目录（如果不存在）
    output_path = Path(args.output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print(f"开始处理: 输入路径={input_path}, 输出路径={output_path}")
    print("-" * 50)
    
    # 处理所有文件
    total_files, processed_files, skipped_files, failed_files = process_all_json_files(
        input_path, output_path
    )
    
    # 输出统计信息
    print("\n" + "="*50)
    print("处理完成！")
    print(f"总文件数: {total_files}")
    print(f"成功处理: {processed_files}")
    print(f"跳过文件: {skipped_files}")
    print(f"处理失败: {failed_files}")
    print(f"输出目录: {output_path}")

if __name__ == "__main__":
    main()
