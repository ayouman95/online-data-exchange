import os
import gzip
import json
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import pycountry
import tempfile

from qcloud_cos import CosConfig, CosS3Client
import oss2


# ================== 配置区（同前）==================
TENCENT_SECRET_ID = os.getenv("COS_SECRET_ID")
TENCENT_SECRET_KEY = os.getenv("COS_SECRET_KEY")
TENCENT_REGION_LIST = ['de', 'sg', 'us']
TENCENT_APPID = "1374116111"
COS_REGION_MAP = {
    'de': 'eu-frankfurt',
    'sg': 'ap-singapore',
    'us': 'na-siliconvalley'
}

ALI_ACCESS_KEY_ID = os.getenv("OSS_ACCESS_KEY_ID")
ALI_ACCESS_KEY_SECRET = os.getenv("OSS_ACCESS_KEY_SECRET")
ALI_OSS_ENDPOINT = "https://oss-ap-southeast-1.aliyuncs.com"
ALI_BUCKET_NAME = "tracking-collect-data-test"

SIZE_LIMITS_MB = {
    ('android', 'usa'): 50,
    ('ios', 'chn'): 100,
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)


# 获取时间范围（机器时区为 UTC+8）
def get_time_ranges_for_previous_hour():
    local_now = datetime.now()
    utc8_prev_hour = local_now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    utc0_prev_hour = utc8_prev_hour - timedelta(hours=8)
    return utc0_prev_hour, utc8_prev_hour


def country_2to3_lower(cc2):
    try:
        country = pycountry.countries.get(alpha_2=cc2)
        return country.alpha_3.lower() if country else "xxx"
    except Exception:
        return "xxx"


# 主函数：低内存流式处理
def main():
    utc0_hour_dt, utc8_hour_dt = get_time_ranges_for_previous_hour()
    logger.info(f"UTC+8 时间段: {utc8_hour_dt.strftime('%Y-%m-%d %H:00')} (读取腾讯 COS)")
    logger.info(f"UTC+0 时间段: {utc0_hour_dt.strftime('%Y-%m-%d %H:00')} (写入阿里 OSS)")

    # 转换大小限制为字节数
    size_limits_bytes = {
        k: v * 1024 * 1024 for k, v in SIZE_LIMITS_MB.items()
    }

    # 为每个 (platform, geo3) 维护状态：当前数据和已用字节数
    buffers = defaultdict(lambda: {'lines': [], 'current_size': 0})

    # 初始化阿里云 OSS 客户端
    auth = oss2.Auth(ALI_ACCESS_KEY_ID, ALI_ACCESS_KEY_SECRET)
    bucket = oss2.Bucket(auth, ALI_OSS_ENDPOINT, ALI_BUCKET_NAME)

    date_part = utc0_hour_dt.strftime("%Y-%m-%d")
    hour_part = utc0_hour_dt.strftime("%H")

    def process_line(line_str):
        try:
            data = json.loads(line_str)
            cc2 = data.get("country_code", "").strip().upper()
            platform = data.get("platform", "").strip().lower()

            if platform not in ["android", "ios"]:
                return

            geo3 = country_2to3_lower(cc2)
            key = (platform, geo3)

            line_size = len(line_str.encode('utf-8')) + 1
            max_size = size_limits_bytes.get(key)

            # 如果无限制，直接追加
            if max_size is None:
                buffers[key]['lines'].append(line_str)
                buffers[key]['current_size'] += line_size
                return

            # 有大小限制：判断是否还能加
            if buffers[key]['current_size'] + line_size <= max_size:
                buffers[key]['lines'].append(line_str)
                buffers[key]['current_size'] += line_size
        except Exception as e:
            pass  # 忽略解析失败的行

    # ===== 第一步：遍历所有 COS 文件，流式处理每一行 =====
    prefixes = build_cos_prefixes(utc8_hour_dt.strftime("%Y%m%d"), utc8_hour_dt.strftime("%H"))

    total_files = 0
    total_lines = 0

    for conf in prefixes:
        marker = ""
        while True:
            try:
                response = conf['client'].list_objects(
                    Bucket=conf['bucket'],
                    Prefix=conf['prefix'],
                    Marker=marker
                )
                contents = response.get('Contents', [])
                if not contents:
                    break

                for item in contents:
                    key = item['Key']
                    if key.endswith('/'):
                        continue

                    total_files += 1
                    try:
                        file_stream = conf['client'].get_object(
                            Bucket=conf['bucket'], Key=key
                        )['Body'].get_raw_stream()

                        line_count = 0
                        for raw_line in file_stream:
                            line = raw_line.strip().decode('utf-8')
                            if line:
                                process_line(line)
                                line_count += 1

                        logger.info(f"Processed {line_count} lines from {key}")
                        total_lines += line_count
                    except Exception as e:
                        logger.error(f"Error streaming {key}: {e}")

                if not response.get('IsTruncated'):
                    break
                marker = response.get('NextMarker', "")
            except Exception as e:
                logger.error(f"Error listing {conf['bucket']}/{conf['prefix']}: {e}")
                break

    logger.info(f"共处理 {total_files} 个文件，{total_lines} 条日志。")

    # ===== 第二步：批量上传所有缓冲区 =====
    success_count = 0
    for (platform, geo3), buf in buffers.items():
        lines = buf['lines']
        if not lines:
            continue

        filename = f"{platform}.{geo3}.log.gz"
        full_path = f"{ALI_BUCKET_NAME}/track/{date_part}/{hour_part}/{filename}"

        content = '\n'.join(lines).encode('utf-8')
        compressed_content = gzip.compress(content)

        try:
            bucket.put_object(full_path, compressed_content)
            logger.info(f"上传成功: {full_path} ({len(lines)} 条, {len(compressed_content)} 字节)")
            success_count += 1
        except Exception as e:
            logger.error(f"上传失败 {full_path}: {e}")

    logger.info(f"处理完成，共上传 {success_count} 个文件。")


def build_cos_prefixes(date_str, hour_str):
    prefixes = []
    for region in TENCENT_REGION_LIST:
        bucket_name = f"pando-adx-{region}-{TENCENT_APPID}"
        prefix = f"adx_device/request/{date_str}/{hour_str}/"
        region_cos = COS_REGION_MAP[region]
        config = CosConfig(Region=region_cos, SecretId=TENCENT_SECRET_ID, SecretKey=TENCENT_SECRET_KEY)
        client = CosS3Client(config)
        prefixes.append({'bucket': bucket_name, 'prefix': prefix, 'client': client})
    return prefixes


if __name__ == "__main__":
    main()