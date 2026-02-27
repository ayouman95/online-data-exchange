#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import logging
import time
from datetime import datetime, timedelta
import pycountry
import gzip
from io import BytesIO

from qcloud_cos import CosConfig, CosS3Client
import oss2
from pybloom_live import BloomFilter


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
ALI_ACCESS_KEY_ID = os.environ["OSS_ACCESS_KEY_ID_TOPLINK"]
ALI_ACCESS_KEY_SECRET = os.environ["OSS_ACCESS_KEY_SECRET_TOPLINK"]
ALI_OSS_ENDPOINT = "https://oss-ap-southeast-3.aliyuncs.com"
ALI_BUCKET_NAME = "toplink-shared-bucket"

QPS_LIMITS = {
    ('android', 'bgd'): 80,
    ('ios', 'bgd'): 20,
    ('android', 'egy'): 80,
    ('ios', 'egy'): 20,
    ('android', 'civ'): 80,
    ('ios', 'civ'): 20,
    ('android', 'mar'): 80,
    ('ios', 'mar'): 20,
    ('android', 'can'): 80,
    ('ios', 'can'): 20,
    ('android', 'gbr'): 80,
    ('ios', 'gbr'): 20,
    ('android', 'fra'): 80,
    ('ios', 'fra'): 20,
    ('android', 'deu'): 80,
    ('ios', 'deu'): 20,
    ('android', 'rus'): 80,
    ('ios', 'rus'): 20,
    ('android', 'vnm'): 80,
    ('ios', 'vnm'): 20,
}

BLOOM_FILTER_CAPACITY = sum(QPS_LIMITS.values()) * 3600
BLOOM_FILTER_ERROR_RATE = 0.001

MANUAL_COUNTRY_2_TO_3 = {
    "UK": "gbr",
}

MANUAL_COUNTRY_3_TO_2 = {
    "GBR": "UK",
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)


def normalize_country_code(cc2):
    return (cc2 or "").strip().upper()


def country_2to3_lower(cc2):
    if cc2.upper() in MANUAL_COUNTRY_2_TO_3:
        return MANUAL_COUNTRY_2_TO_3[cc2.upper()]
    try:
        country = pycountry.countries.get(alpha_2=cc2.upper())
        return country.alpha_3.lower() if country else "xxx"
    except Exception:
        return "xxx"


def country_3to2_upper(cc3):
    if cc3.upper() in MANUAL_COUNTRY_3_TO_2:
        return MANUAL_COUNTRY_3_TO_2[cc3.upper()]
    try:
        country = pycountry.countries.get(alpha_3=cc3.upper())
        return country.alpha_2.upper() if country else "xxx"
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


class BufferedUploader:
    def __init__(self, platform, geo3, qps_limit, oss_bucket, date_part, hour_part):
        self.platform = platform
        self.geo3 = geo3
        self.max_lines = qps_limit * 3600 if qps_limit else None
        self.oss_bucket = oss_bucket
        self.date_part = date_part
        self.hour_part = hour_part

        self.buffer = BytesIO()
        self.gz_file = gzip.GzipFile(mode='wb', fileobj=self.buffer)
        self.line_count = 0
        self.uploaded = False

    def write(self, line):
        if self.uploaded:
            return False

        self.gz_file.write((line + '\n').encode('utf-8'))
        self.line_count += 1

        if self.max_lines and self.line_count >= self.max_lines:
            logger.info(
                f"⚠️ 达到限制: {self.platform}.{self.geo3}.log.gz "
                f"({self.line_count} 行, 上限 {self.max_lines} 行)"
            )
            self._flush()

        return True

    def _flush(self):
        if self.line_count == 0 or self.uploaded:
            return

        self.gz_file.close()

        geo2_upper = country_3to2_upper(self.geo3)
        filename = f"{self.date_part}-{self.hour_part}.{SELECT_REGION}.{self.platform}.{geo2_upper}.log.gz"
        key = f"{self.date_part}-{self.hour_part}/{geo2_upper}/{self.platform}/{filename}"

        try:
            self.buffer.seek(0)
            self.oss_bucket.put_object(key, self.buffer)
            logger.info(f"✅ 上传完成: {key} ({self.line_count} 行)")
            self.uploaded = True
        except Exception as exc:
            logger.error(f"❌ 上传失败 {key}: {exc}")
        finally:
            self.buffer.close()


def transform_line(data, geo3):
    os_name = data.get("platform", "")
    osi = 0
    if os_name == "android":
        osi = 2
    if os_name == "ios":
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

    return "@".join(str(field) for field in fields)


def list_cos_keys(client, bucket_name, prefix):
    keys = []
    marker = ""

    while True:
        response = client.list_objects(
            Bucket=bucket_name,
            Prefix=prefix,
            Marker=marker
        )

        for item in response.get("Contents", []):
            key = item["Key"]
            if not key.endswith("/"):
                keys.append(key)

        if not response.get("isTruncated"):
            break

        marker = response.get("NextMarker", "")

    keys.sort()
    return keys


def build_cos_prefixes(date_str, hour_str):
    bucket_name = f"pando-adx-{SELECT_REGION}-{TENCENT_APPID}"
    region_cos = COS_REGION_MAP[SELECT_REGION]
    config = CosConfig(Region=region_cos, SecretId=TENCENT_SECRET_ID, SecretKey=TENCENT_SECRET_KEY)
    client = CosS3Client(config)

    prefixes = []
    for minute in range(59, -1, -1):
        prefixes.append({
            "bucket": bucket_name,
            "prefix": f"adx_device/request/{date_str}/{hour_str}/{minute:02d}/",
            "client": client,
            "minute": minute,
        })
    return prefixes


def main():
    utc0_hour_dt, utc8_hour_dt = get_time_ranges_for_previous_hour()
    logger.info(f"UTC+8 时间段: {utc8_hour_dt.strftime('%Y-%m-%d %H:00')} (读取腾讯 COS)")
    logger.info(f"UTC+0 时间段: {utc0_hour_dt.strftime('%Y-%m-%d %H:00')} (写入阿里 OSS)")

    auth = oss2.Auth(ALI_ACCESS_KEY_ID, ALI_ACCESS_KEY_SECRET)
    bucket = oss2.Bucket(auth, ALI_OSS_ENDPOINT, ALI_BUCKET_NAME)

    date_part = utc0_hour_dt.strftime("%Y-%m-%d")
    hour_part = utc0_hour_dt.strftime("%H")

    uploaders = {
        key: BufferedUploader(
            key[0],
            key[1],
            QPS_LIMITS[key],
            bucket,
            date_part,
            hour_part,
        )
        for key in QPS_LIMITS
    }
    device_filter = BloomFilter(capacity=BLOOM_FILTER_CAPACITY, error_rate=BLOOM_FILTER_ERROR_RATE)

    stats = {
        "total_files": 0,
        "written_lines": 0,
        "duplicate_device_ids": 0,
        "missing_device_ids": 0,
    }

    def get_uploader(platform, geo3):
        return uploaders[(platform, geo3)]

    def all_targets_uploaded():
        return all(uploader.uploaded for uploader in uploaders.values())

    prefixes = build_cos_prefixes(utc8_hour_dt.strftime("%Y%m%d"), utc8_hour_dt.strftime("%H"))

    for conf in prefixes:
        if all_targets_uploaded():
            logger.info("✅ 所有 QPS_LIMITS 目标文件均已上传，停止继续读取原始数据。")
            break

        try:
            keys = list_cos_keys(conf["client"], conf["bucket"], conf["prefix"])
        except Exception as exc:
            logger.error(f"❌ 列出失败 {conf['bucket']}/{conf['prefix']}: {exc}")
            continue

        if not keys:
            continue

        logger.info(f"📂 开始处理 minute={conf['minute']:02d}, 文件数={len(keys)}")

        for key in keys:
            if all_targets_uploaded():
                logger.info("✅ 所有 QPS_LIMITS 目标文件均已上传，停止继续读取原始数据。")
                break

            stats["total_files"] += 1

            try:
                file_stream = conf["client"].get_object(
                    Bucket=conf["bucket"],
                    Key=key
                )["Body"].get_raw_stream()

                file_written_lines = 0
                for raw_line in file_stream:
                    line = raw_line.strip().decode("utf-8")
                    if not line:
                        continue

                    try:
                        data = json.loads(line)
                    except Exception:
                        continue

                    platform = (data.get("platform") or "").strip().lower()
                    cc2 = normalize_country_code(data.get("country_code"))
                    geo3 = country_2to3_lower(cc2)
                    if geo3 == "xxx" or (platform, geo3) not in QPS_LIMITS:
                        continue

                    device_id = str(data.get("deviceId") or "").strip()
                    if not device_id:
                        stats["missing_device_ids"] += 1
                        continue

                    if device_id in device_filter:
                        stats["duplicate_device_ids"] += 1
                        continue
                    device_filter.add(device_id)

                    uploader = get_uploader(platform, geo3)
                    if uploader.write(transform_line(data, geo3)):
                        file_written_lines += 1

                stats["written_lines"] += file_written_lines
                logger.info(f"✅ 处理完成: {key} ({file_written_lines} 行)")
            except Exception as exc:
                logger.error(f"❌ 处理失败 {key}: {exc}")

    for uploader in uploaders.values():
        uploader._flush()

    logger.info(
        "📊 处理完成："
        f"共 {stats['total_files']} 个文件，"
        f"写入 {stats['written_lines']} 条，"
        f"去重丢弃 {stats['duplicate_device_ids']} 条，"
        f"缺失 deviceId 丢弃 {stats['missing_device_ids']} 条。"
    )


if __name__ == "__main__":
    start_time = time.time()
    main()
    logger.info(f"耗时: {time.time() - start_time} 秒")
