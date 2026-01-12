#!/usr/bin/env python3
"""
puls8_dbf_data_prep
S3 COPY-ONLY Data Preparation Job
Schedule-agnostic, Dockerized, IAM-role based
"""

import boto3, logging, fnmatch, os
from datetime import datetime
import pytz

SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "prim-dev-p8-upload-01")
SOURCE_PREFIX = os.getenv("SOURCE_PREFIX", "primo-dbf/")
DEST_BUCKET   = os.getenv("DEST_BUCKET", "dcai-primo-dbf-dev")
DEST_BASE     = os.getenv("DEST_BASE", "primobrands/data")
TIMEZONE      = os.getenv("TIMEZONE", "US/Eastern")
FILE_PATTERNS = os.getenv("FILE_PATTERNS", "*.csv,*.txt").split(",")

LOG_FILE = "/logs/s3_copy_job.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s | %(message)s")

def matches(name):
    return any(fnmatch.fnmatch(name, p) for p in FILE_PATTERNS)

def main():
    now = datetime.now(pytz.timezone(TIMEZONE))
    dest_prefix = f"{DEST_BASE}/{now.strftime('%Y-%m-%d_%H-%M-%S')}/"
    s3 = boto3.client("s3")

    resp = s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=SOURCE_PREFIX)
    if "Contents" not in resp:
        logging.info("No files found in source path")
        return

    for o in resp["Contents"]:
        k = o["Key"]
        if k.endswith("/"):
            continue

        name = k.split("/")[-1]
        if not matches(name):
            continue

        s3.copy_object(
            CopySource={"Bucket": SOURCE_BUCKET, "Key": k},
            Bucket=DEST_BUCKET,
            Key=f"{dest_prefix}{name}"
        )
        logging.info(f"Copied {name}")

if __name__ == "__main__":
    main()
