#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gzip
import json
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import pycountry

from qcloud_cos import CosConfig, CosS3Client
import oss2


# ================== 配置区 ==================

# 腾讯云 COS 配置（路径时间基于 UTC+8）
TENCENT_SECRET_ID = os.environ['COS_SECRET_ID']
TENCENT_SECRET_KEY = os.environ['COS_SECRET_KEY']
TENCENT_REGION_LIST = ['de', 'sg', 'us']
TENCENT_APPID = "1374116111"
COS_REGION_MAP = {
    'de': 'eu-frankfurt',
    'sg': 'ap-singapore',
    'us': 'na-siliconvalley'
}

# 阿里云 OSS 配置（路径时间基于 UTC+0）
ALI_ACCESS_KEY_ID = os.environ["OSS_ACCESS_KEY_ID"]
ALI_ACCESS_KEY_SECRET = os.environ["OSS_ACCESS_KEY_SECRET"]
ALI_OSS_ENDPOINT = "https://oss-ap-southeast-1.aliyuncs.com"  # 替换为你实际的 endpoint
ALI_BUCKET_NAME = "tracking-collect-data-test"  # TODO: 改成实际的

# 文件大小限制（单位：MB），key 为 (platform, geo3_lower)，value 为最大 MB 数
# 示例：达到限制后不再追加数据，直接上传已有部分
SIZE_LIMITS_MB = {
    ('android', 'idn'): 200,
    ('android', 'tha'): 100,
    ('android', 'phl'): 240,
    ('android', 'ita'): 15,
    ('android', 'pol'): 20,
    ('android', 'nld'): 20,
    ('android', 'bra'): 80,
    ('android', 'mex'): 150,
    ('android', 'zaf'): 20,
    ('android', 'kor'): 7,
    ('android', 'ukr'): 20,
    ('android', 'can'): 11,
    ('android', 'usa'): 40,
    ('android', 'esp'): 35,
    ('android', 'ind'): 75,
    ('android', 'sau'): 70,
    ('android', 'fra'): 50,
    ('android', 'gbr'): 20,
    ('android', 'deu'): 75,
    ('android', 'are'): 40,
    ('android', 'rus'): 200,
}

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# 获取上一个小时的时间对象（UTC+8 和 UTC+0 对应）
def get_time_ranges_for_previous_hour():
    now_utc = datetime.utcnow()
    prev_hour_utc = now_utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

    # UTC+0 输出路径用
    utc0_hour_dt = prev_hour_utc

    # UTC+8 用于构建腾讯 COS 路径
    utc8_now = now_utc + timedelta(hours=8)
    utc8_prev_hour = utc8_now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

    return utc0_hour_dt, utc8_prev_hour


# 构建腾讯云 COS 的前缀列表（使用 UTC+8 时间）
def build_cos_prefixes(base_date_str, hour_str):
    prefixes = []
    for region in TENCENT_REGION_LIST:
        bucket_name = f"pando-adx-{region}-{TENCENT_APPID}"
        prefix = f"adx_device/request/{base_date_str}/{hour_str}/"
        region_cos = COS_REGION_MAP[region]
        config = CosConfig(Region=region_cos, SecretId=TENCENT_SECRET_ID, SecretKey=TENCENT_SECRET_KEY)
        client = CosS3Client(config)
        prefixes.append({
            'bucket': bucket_name,
            'prefix': prefix,
            'client': client
        })
    return prefixes


# 下载单个 COS 文件
def download_and_parse_file(task):
    client = task['client']
    bucket = task['bucket']
    key = task['key']
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        file_stream = response['Body'].get_raw_stream()
        lines = []
        for line in file_stream:
            line = line.strip()
            if line:
                lines.append(line.decode('utf-8'))
        logger.info(f"Downloaded {len(lines)} lines from {key}")
        return lines
    except Exception as e:
        logger.error(f"Error downloading {key}: {str(e)}")
        return []


# 获取上一个小时的所有日志（UTC+8 时间范围）
def fetch_all_logs(utc8_hour_dt):
    date_str = utc8_hour_dt.strftime("%Y%m%d")
    hour_str = utc8_hour_dt.strftime("%H")

    prefixes = build_cos_prefixes(date_str, hour_str)
    all_tasks = []

    for conf in prefixes:
        marker = ""
        while True:
            try:
                response = conf['client'].list_objects(
                    Bucket=conf['bucket'],
                    Prefix=conf['prefix'],
                    Marker=marker
                )
                contents = response.get('Contents')
                if not contents:
                    break
                for item in contents:
                    key = item['Key']
                    if not key.endswith('/'):
                        all_tasks.append({
                            'client': conf['client'],
                            'bucket': conf['bucket'],
                            'key': key
                        })
                if not response.get('isTruncated'):
                    break
                marker = response.get('NextMarker', "")
            except Exception as e:
                logger.error(f"Error listing {conf['bucket']}/{conf['prefix']}: {e}")
                break

    # 并行下载
    all_lines = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(download_and_parse_file, all_tasks)
        for res in results:
            all_lines.extend(res)

    return all_lines


# 二位国家码转三位（小写）
def country_2to3_lower(cc2):
    try:
        country = pycountry.countries.get(alpha_2=cc2)
        return country.alpha_3.lower() if country else "xxx"
    except Exception:
        return "xxx"


# 主函数
def main():
    # 获取 UTC+0 和 UTC+8 时间
    utc0_hour_dt, utc8_hour_dt = get_time_ranges_for_previous_hour()

    logger.info(f"开始处理上一个小时的数据")
    logger.info(f"UTC+8 时间段: {utc8_hour_dt.strftime('%Y-%m-%d %H:00')} (用于读取腾讯 COS)")
    logger.info(f"UTC+0 时间段: {utc0_hour_dt.strftime('%Y-%m-%d %H:00')} (用于写入阿里 OSS)")

    # Step 1: 拉取日志（使用 UTC+8 时间）
    log_lines = fetch_all_logs(utc8_hour_dt)
    if not log_lines:
        logger.warning("没有获取到任何日志数据，跳过上传。")
        return

    logger.info(f"共读取 {len(log_lines)} 条日志记录。")

    # Step 2: 按 platform 和 geo3 分组
    grouped_data = defaultdict(list)

    parse_errors = 0
    for line in log_lines:
        try:
            data = json.loads(line)
            cc2 = data.get("country_code", "").strip().upper()
            platform = data.get("platform", "").strip().lower()

            if platform not in ["android", "ios"]:
                continue

            geo3_lower = country_2to3_lower(cc2)
            grouped_data[(platform, geo3_lower)].append(line)
        except Exception as e:
            parse_errors += 1
            continue

    if parse_errors:
        logger.warning(f"有 {parse_errors} 条日志解析失败。")

    if not grouped_data:
        logger.warning("没有有效数据可上传。")
        return

    # Step 3: 处理大小限制（截断，不切分）
    upload_tasks = []  # (platform, geo3, lines)

    for (platform, geo3), lines in grouped_data.items():
        limit_mb = SIZE_LIMITS_MB.get((platform, geo3))
        if limit_mb is None:
            # 无限制，全部保留
            upload_tasks.append((platform, geo3, lines))
        else:
            max_bytes = limit_mb * 1024 * 1024
            current_size = 0
            selected_lines = []

            for line in lines:
                line_size = len(line.encode('utf-8')) + 1  # +1 for newline
                if current_size + line_size > max_bytes:
                    break
                selected_lines.append(line)
                current_size += line_size

            logger.info(f"文件 ({platform}.{geo3}) 限制 {limit_mb}MB，保留 {len(selected_lines)} 条记录")
            upload_tasks.append((platform, geo3, selected_lines))

    # Step 4: 上传到阿里云 OSS（使用 UTC+0 时间路径）
    auth = oss2.Auth(ALI_ACCESS_KEY_ID, ALI_ACCESS_KEY_SECRET)
    bucket = oss2.Bucket(auth, ALI_OSS_ENDPOINT, ALI_BUCKET_NAME)

    date_part = utc0_hour_dt.strftime("%Y-%m-%d")
    hour_part = utc0_hour_dt.strftime("%H")

    success_count = 0
    for platform, geo3, lines in upload_tasks:
        if not lines:
            logger.warning(f"跳过空文件: {platform}.{geo3}")
            continue

        filename = f"{platform}.{geo3}.log.gz"
        full_path = f"{ALI_BUCKET_NAME}/track/{date_part}/{hour_part}/{filename}"

        # 压缩
        content = '\n'.join(lines).encode('utf-8')
        compressed_content = gzip.compress(content)

        # 上传
        try:
            bucket.put_object(full_path, compressed_content)
            logger.info(f"成功上传 {len(lines)} 条记录 到 {full_path} ({len(compressed_content)} bytes)")
            success_count += 1
        except Exception as e:
            logger.error(f"上传失败 {full_path}: {str(e)}")

    logger.info(f"处理完成，共上传 {success_count} 个文件。")


if __name__ == "__main__":
    main()