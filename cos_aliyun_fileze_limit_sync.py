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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from qcloud_cos import CosConfig, CosS3Client
import oss2
import orjson


# ================== 配置区 ==================
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

# 并发配置
DOWNLOAD_WORKERS = 8   # 下载并发数（建议 = CPU 核数）
UPLOAD_WORKERS = 4     # 上传并发数

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


# ==== 全局上传线程池 ====
upload_executor = ThreadPoolExecutor(max_workers=UPLOAD_WORKERS)


# ==== 高性能上传器 ====
class BufferedUploader:
    def __init__(self, platform, geo3, limit_mb, oss_bucket, date_part, hour_part):
        self.platform = platform
        self.geo3 = geo3
        self.limit_bytes = limit_mb * 1024 * 1024 if limit_mb else None
        self.oss_bucket = oss_bucket
        self.date_part = date_part
        self.hour_part = hour_part

        self.buffer = BytesIO()
        self.gz_file = gzip.GzipFile(mode='wb', fileobj=self.buffer)
        self.current_size = 0
        self.line_count = 0
        self.lock = threading.Lock()
        self.uploaded = False

    def write(self, line: str):
        with self.lock:
            if self.uploaded:
                return

            line_bytes = (line + '\n').encode('utf-8')
            added_size = len(line_bytes)

            if self.limit_bytes and self.current_size + added_size > self.limit_bytes:
                self._submit_upload()  # 触发异步上传
                return

            self.gz_file.write(line_bytes)
            self.current_size += added_size
            self.line_count += 1

    def _submit_upload(self):
        """提交异步上传任务"""
        if self.line_count == 0 or self.uploaded:
            return

        self.gz_file.close()
        self.buffer.seek(0)
        content = self.buffer.read()

        upload_executor.submit(self._do_upload, content)

    def _do_upload(self, content: bytes):
        filename = f"{self.platform}.{self.geo3}.log.gz"
        key = f"track/{self.date_part}/{self.hour_part}/{filename}"
        try:
            self.oss_bucket.put_object(key, content)
            logger.info(f"✅ 上传完成: {key} ({self.line_count} 行)")
        except Exception as e:
            logger.error(f"❌ 上传失败 {key}: {e}")
        self.uploaded = True


# ==== 主函数 ====
def main():
    utc0_hour_dt, utc8_hour_dt = get_time_ranges_for_previous_hour()
    logger.info(f"UTC+8 时间段: {utc8_hour_dt.strftime('%Y-%m-%d %H:00')} (读取腾讯 COS)")
    logger.info(f"UTC+0 时间段: {utc0_hour_dt.strftime('%Y-%m-%d %H:00')} (写入阿里 OSS)")

    auth = oss2.Auth(ALI_ACCESS_KEY_ID, ALI_ACCESS_KEY_SECRET)
    bucket = oss2.Bucket(auth, ALI_OSS_ENDPOINT, ALI_BUCKET_NAME)

    date_part = utc0_hour_dt.strftime("%Y-%m-%d")
    hour_part = utc0_hour_dt.strftime("%H")

    # 全局 uploader 缓存
    uploaders = {}
    uploader_lock = threading.Lock()

    def get_uploader(platform, geo3):
        key = (platform, geo3)
        if key not in uploaders:
            with uploader_lock:
                if key not in uploaders:  # double-check
                    limit_mb = SIZE_LIMITS_MB.get(key)
                    uploaders[key] = BufferedUploader(platform, geo3, limit_mb, bucket, date_part, hour_part)
        return uploaders[key]

    # ===== 多线程下载所有文件 =====
    prefixes = build_cos_prefixes(utc8_hour_dt.strftime("%Y%m%d"), utc8_hour_dt.strftime("%H"))
    file_list = []

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
                for item in contents:
                    if not item['Key'].endswith('/'):
                        file_list.append((conf['client'], conf['bucket'], item['Key']))
                if not response.get('isTruncated'):
                    break
                marker = response.get('NextMarker', "")
            except Exception as e:
                logger.error(f"列出失败: {conf['bucket']}/{conf['prefix']}: {e}")
                break

    logger.info(f"发现 {len(file_list)} 个待处理文件，启动 {DOWNLOAD_WORKERS} 个下载线程...")

    def process_single_file(args):
        # TODO: 如果所有uploader都已上传完成，则结束
        if all(uploader.uploaded for uploader in uploaders.values()):
            logger.info("所有文件已处理完毕，结束下载线程...")
            return

        client, bucket_name, key = args
        line_count = 0
        try:
            stream = client.get_object(Bucket=bucket_name, Key=key)['Body'].get_raw_stream()
            for raw_line in stream:
                line = raw_line.strip().decode('utf-8')
                if not line:
                    continue
                try:
                    data = orjson.loads(line)
                    cc2 = data.get("country_code", "").strip().upper()
                    platform = data.get("platform", "").strip().lower()

                    # 矫正国家
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


                    geo3 = country_2to3_lower(cc2)
                    uploader = get_uploader(platform, geo3)

                    new_line = transform_line(data, geo3)
                    uploader.write(new_line)
                    line_count += 1
                except Exception:
                    pass
            logger.debug(f"完成: {key} ({line_count} 行)")
        except Exception as e:
            logger.error(f"处理失败 {key}: {e}")
        return line_count

    total_lines = 0
    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as download_executor:
        futures = [download_executor.submit(process_single_file, file_args) for file_args in file_list]
        for future in as_completed(futures):
            total_lines += future.result()

    # 等待所有上传完成
    upload_executor.shutdown(wait=True)

    logger.info(f"📊 处理完成：共 {len(file_list)} 个文件，{total_lines} 条日志。")


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
    # 计算耗时
    start_time = time.time()
    main()
    logger.info(f"耗时: {time.time() - start_time} 秒")