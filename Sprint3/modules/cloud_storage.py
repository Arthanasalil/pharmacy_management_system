import os
import uuid
from datetime import datetime
from urllib.parse import urlparse

import boto3
from flask import current_app
from werkzeug.utils import secure_filename


def _get_s3_client():
    access_key = current_app.config.get("AWS_ACCESS_KEY_ID") or current_app.config.get("AWS_ACCESS_KEY")
    secret_key = current_app.config.get("AWS_SECRET_ACCESS_KEY") or current_app.config.get("AWS_SECRET_KEY")
    region = current_app.config.get("AWS_REGION", "ap-south-1")
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def upload_file(file, filename):
    bucket = current_app.config.get("S3_BUCKET")
    if not bucket:
        raise ValueError("S3_BUCKET is not configured.")
    s3 = _get_s3_client()
    s3.upload_fileobj(file, bucket, filename)
    return f"https://{bucket}.s3.{current_app.config.get('AWS_REGION', 'ap-south-1')}.amazonaws.com/{filename}"


def upload_prescription_document(file_storage, client_id):
    if not file_storage or not file_storage.filename:
        raise ValueError("No file was provided.")

    bucket = current_app.config.get("S3_BUCKET")
    if not bucket:
        raise ValueError("S3 bucket is not configured.")

    safe_name = secure_filename(file_storage.filename) or "document"
    ext = os.path.splitext(safe_name)[1].lower() or ".bin"
    prefix = str(current_app.config.get("S3_PRESCRIPTION_PREFIX", "prescriptions")).strip("/") or "prescriptions"
    date_tag = datetime.utcnow().strftime("%Y%m%d")
    unique = uuid.uuid4().hex[:12]
    object_key = f"{prefix}/client_{client_id}/{date_tag}/{unique}{ext}"

    s3 = _get_s3_client()
    s3.upload_fileobj(
        file_storage,
        bucket,
        object_key,
        ExtraArgs={
            "ContentType": file_storage.mimetype or "application/octet-stream",
        },
    )
    url = f"https://{bucket}.s3.{current_app.config.get('AWS_REGION', 'ap-south-1')}.amazonaws.com/{object_key}"
    return object_key, url


def _extract_object_key(stored_value):
    value = (stored_value or "").strip()
    if not value:
        return None

    if value.startswith("s3://"):
        rest = value[5:]
        parts = rest.split("/", 1)
        return parts[1] if len(parts) > 1 else None

    if value.startswith("http://") or value.startswith("https://"):
        parsed = urlparse(value)
        return parsed.path.lstrip("/")

    return value


def get_prescription_access_url(stored_value, expires_in=3600):
    bucket = current_app.config.get("S3_BUCKET")
    if not bucket:
        raise ValueError("S3 bucket is not configured.")

    object_key = _extract_object_key(stored_value)
    if not object_key:
        raise ValueError("Prescription file key is missing.")

    s3 = _get_s3_client()
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": object_key},
        ExpiresIn=expires_in,
    )

