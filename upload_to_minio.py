#!/usr/bin/env python3
"""
upload_to_minio.py — upload file source JSON lên bucket MinIO.

Chạy TỪ HOST (máy bạn), sau khi `docker compose up` đã chạy và MinIO healthy:

    pip install boto3
    KPLUS_RUNTIME=host python upload_to_minio.py /duong/dan/toi/20220401.json

Nếu không truyền đường dẫn, script tìm file 20220401.json ở thư mục hiện tại.
File sẽ được đưa lên: s3a://raw/kplus/20220401.json
"""
import os
import sys

# Bắt buộc chạy ở host mode để trỏ về localhost:9000
os.environ.setdefault("KPLUS_RUNTIME", "host")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "config"))
import config as C  # noqa: E402

try:
    import boto3
    from botocore.client import Config as BotoConfig
    from botocore.exceptions import ClientError
except ImportError:
    sys.exit("Thiếu boto3. Chạy: pip install boto3")


def main():
    # Đường dẫn file local
    local_path = sys.argv[1] if len(sys.argv) > 1 else C.SOURCE_FILE
    if not os.path.isfile(local_path):
        sys.exit(f"Không thấy file: {local_path}")

    size_mb = os.path.getsize(local_path) / 1024 / 1024
    print(f"File local : {local_path} ({size_mb:.1f} MB)")
    print(f"Endpoint   : {C.MINIO_ENDPOINT}")
    print(f"Đích       : s3://{C.BUCKET_RAW}/{C.RAW_OBJECT_KEY}")

    s3 = boto3.client(
        "s3",
        endpoint_url=C.MINIO_ENDPOINT,
        aws_access_key_id=C.MINIO_ACCESS_KEY,
        aws_secret_access_key=C.MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name=C.MINIO_REGION,
    )

    # Đảm bảo bucket tồn tại (minio-init đã tạo, nhưng kiểm tra cho chắc)
    try:
        s3.head_bucket(Bucket=C.BUCKET_RAW)
    except ClientError:
        print(f"Bucket '{C.BUCKET_RAW}' chưa có, đang tạo...")
        s3.create_bucket(Bucket=C.BUCKET_RAW)

    print("Đang upload...")
    s3.upload_file(local_path, C.BUCKET_RAW, C.RAW_OBJECT_KEY)

    # Xác nhận
    head = s3.head_object(Bucket=C.BUCKET_RAW, Key=C.RAW_OBJECT_KEY)
    print(f"OK. Đã lên MinIO, size = {head['ContentLength'] / 1024 / 1024:.1f} MB")
    print(f"Spark sẽ đọc tại: {C.RAW_S3_URI}")


if __name__ == "__main__":
    main()
