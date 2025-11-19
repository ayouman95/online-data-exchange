#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import logging
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pycountry
import gzip
from io import BytesIO

from qcloud_cos import CosConfig, CosS3Client
import oss2


# ================== 配置区 ==================
TENCENT_SECRET_ID = os.environ['COS_SECRET_ID']
TENCENT_SECRET_KEY = os.environ['COS_SECRET_KEY']
SELECT_REGION = os.environ.get('SELECT_REGION').lower()
TENCENT_APPID = "1374116111"
COS_REGION_MAP = {
    'de': 'eu-frankfurt',
    'sg': 'ap-singapore',
    'us': 'na-ashburn'
}

# 阿里云 OSS 配置（路径时间基于 UTC+0）
ALI_ACCESS_KEY_ID = os.environ["OSS_ACCESS_KEY_ID_ALI"]
ALI_ACCESS_KEY_SECRET = os.environ["OSS_ACCESS_KEY_SECRET_ALI"]
ALI_OSS_ENDPOINT = "https://oss-ap-southeast-1.aliyuncs.com"  # 替换为你实际的 endpoint
ALI_BUCKET_NAME = "tracking-collect-data"

# 文件大小限制（单位：MB），key 为 (platform, geo3_lower)，value 为最大 MB 数
# 示例：达到限制后不再追加数据，直接上传已有部分
SIZE_LIMITS_MB = {
    ('android', 'are'): 80,
    ('android', 'bra'): 300,
    ('android', 'can'): 20,
    ('android', 'deu'): 20,
    ('android', 'esp'): 70,
    ('android', 'fra'): 100,
    ('android', 'gbr'): 40,
    ('android', 'idn'): 300,
    ('android', 'ind'): 150,
    ('android', 'ita'): 30,
    ('android', 'kor'): 20,
    ('android', 'mex'): 300,
    ('android', 'nld'): 40,
    ('android', 'phl'): 300,
    ('android', 'pol'): 40,
    ('android', 'rus'): 300,
    ('android', 'sau'): 150,
    ('android', 'tha'): 200,
    ('android', 'ukr'): 40,
    ('android', 'usa'): 80,
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)


def country_2to3_lower(cc2):
    try:
        country = pycountry.countries.get(alpha_2=cc2)
        return country.alpha_3.lower() if country else "xxx"
    except Exception:
        return "xxx"


def get_time_ranges_for_previous_hour():
    now_utc = datetime.utcnow()
    prev_hour_utc = now_utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

    # UTC+0 输出路径用
    utc0_hour_dt = prev_hour_utc

    # UTC+8 用于构建腾讯 COS 路径
    utc8_now = now_utc + timedelta(hours=8)
    utc8_prev_hour = utc8_now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

    return utc0_hour_dt, utc8_prev_hour


# ==== 核心：低内存上传器 ====
class BufferedUploader:
    def __init__(self, platform, geo3, limit_mb, oss_bucket, date_part, hour_part):
        self.platform = platform
        self.geo3 = geo3
        self.limit_bytes = limit_mb * 1024 * 1024 if limit_mb else None
        self.oss_bucket = oss_bucket
        self.date_part = date_part
        self.hour_part = hour_part

        # 压缩流
        self.buffer = BytesIO()
        self.gz_file = gzip.GzipFile(mode='wb', fileobj=self.buffer)
        self.current_size = 0
        self.line_count = 0

        self.uploaded = False

    def write(self, line: str):
        if self.uploaded:
            return

        line_bytes = (line + '\n').encode('utf-8')
        added_size = len(line_bytes)

        # 检查是否超限
        if self.limit_bytes and self.current_size + added_size > self.limit_bytes:
            logger.info(f"⚠️ 达到限制: {self.platform}.{self.geo3}.log.gz ({self.line_count} 行, {self.current_size} 字节)")
            self._flush()  # 达到限制，立即上传
            return

        self.gz_file.write(line_bytes)
        self.current_size += added_size
        self.line_count += 1

    def _flush(self):
        if self.line_count == 0 or self.uploaded:
            return

        # 关闭压缩流
        self.gz_file.close()

        # 构造 OSS 路径
        filename = f"{self.platform}.{self.geo3}.log.gz"
        key = f"track/{self.date_part}/{self.hour_part}/{filename}"

        # 上传
        try:
            self.buffer.seek(0)
            self.oss_bucket.put_object(key, self.buffer)
            logger.info(f"✅ 上传完成: {key} ({self.line_count} 行, {self.current_size} 字节)")
            self.uploaded = True
        except Exception as e:
            logger.error(f"❌ 上传失败 {key}: {e}")
        finally:
            self.buffer.close()


# ==== 主函数 ====
def transform_line(data, geo3):
    os = data.get("platform", "")
    osi = 0
    if os == "android":
        osi = 2
    if os == "ios":
        osi = 1

    fields = [
        geo3.upper(),
        osi,
        data.get("display_manager", ""),
        data.get("deviceId", ""),
        data.get("brand", ""),
        data.get("user_agent", ""),
        data.get("ip", ""),
        data.get("language", ""),
        data.get("timestamp", ""),
        data.get("os_version", ""),
        data.get("app_id", ""),
        data.get("model", ""),
        data.get("network_type", "")
    ]

    # 拼接为 @ 分隔字符串
    output_line = "@".join(str(f) for f in fields)

    return output_line


def main():
    utc0_hour_dt, utc8_hour_dt = get_time_ranges_for_previous_hour()
    logger.info(f"UTC+8 时间段: {utc8_hour_dt.strftime('%Y-%m-%d %H:00')} (读取腾讯 COS)")
    logger.info(f"UTC+0 时间段: {utc0_hour_dt.strftime('%Y-%m-%d %H:00')} (写入阿里 OSS)")

    # 初始化 OSS
    auth = oss2.Auth(ALI_ACCESS_KEY_ID, ALI_ACCESS_KEY_SECRET)
    bucket = oss2.Bucket(auth, ALI_OSS_ENDPOINT, ALI_BUCKET_NAME)

    date_part = utc0_hour_dt.strftime("%Y-%m-%d")
    hour_part = utc0_hour_dt.strftime("%H")

    # 所有活跃的上传器
    uploaders = {}

    def get_uploader(platform, geo3):
        key = (platform, geo3)
        if key not in uploaders:
            limit_mb = SIZE_LIMITS_MB.get(key) * 5
            uploader = BufferedUploader(platform, geo3, limit_mb, bucket, date_part, hour_part)
            uploaders[key] = uploader
        return uploaders[key]

    # ===== 流式处理所有日志文件 =====
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
                            if not line:
                                continue

                            try:
                                data = json.loads(line)

                                # 矫正国家
                                cc2 = data.get("country_code", "").strip().upper()
                                if cc2 == "UK":
                                    cc2 = "GB"
                                platform = data.get("platform", "").strip().lower()

                                if platform not in ["android", "ios"]:
                                    continue

                                # 如果找不到三位国家代码，则忽略
                                geo3 = country_2to3_lower(cc2)
                                if geo3 == 'xxx':
                                    continue

                                # 没有限制文件大小，则忽略
                                if not SIZE_LIMITS_MB.get((platform, geo3)):
                                    continue
                                uploader = get_uploader(platform, geo3)
                                new_line = transform_line(data, geo3)
                                uploader.write(new_line)  # ⬅️ 边读边写！

                                line_count += 1
                            except Exception:
                                pass  # 忽略无效行

                        total_lines += line_count
                        logger.info(f"✅ 处理完成: {key} ({line_count} 行)")

                    except Exception as e:
                        logger.error(f"❌ 处理失败 {key}: {e}")

                if not response.get('isTruncated'):
                    break
                marker = response.get('NextMarker', "")

            except Exception as e:
                logger.error(f"❌ 列出失败 {conf['bucket']}/{conf['prefix']}: {e}")
                break

    # ===== 所有文件读完后，上传剩余未满的文件 =====
    for uploader in uploaders.values():
        uploader._flush()

    logger.info(f"📊 处理完成：共 {total_files} 个文件，{total_lines} 条日志。")


def build_cos_prefixes(date_str, hour_str):
    prefixes = []
    bucket_name = f"pando-adx-{SELECT_REGION}-{TENCENT_APPID}"
    prefix = f"adx_device/request/{date_str}/{hour_str}/"
    region_cos = COS_REGION_MAP[SELECT_REGION]
    config = CosConfig(Region=region_cos, SecretId=TENCENT_SECRET_ID, SecretKey=TENCENT_SECRET_KEY)
    client = CosS3Client(config)
    prefixes.append({'bucket': bucket_name, 'prefix': prefix, 'client': client})
    return prefixes


if __name__ == "__main__":
    # 计算耗时
    start_time = time.time()
    main()
    logger.info(f"耗时: {time.time() - start_time} 秒")