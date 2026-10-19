"""Create the warehouse bucket once the S3 endpoint answers (run by the create-bucket compose service)."""
import os
import time

import boto3

s3 = boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT", "http://s3:9000"))
bucket = os.environ.get("BUCKET", "warehouse")
for _ in range(30):
    try:
        names = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
        if bucket not in names:
            s3.create_bucket(Bucket=bucket)
        print("bucket ready")
        break
    except Exception as e:  # endpoint still starting
        print("waiting for s3:", e)
        time.sleep(2)
else:
    raise SystemExit(1)
