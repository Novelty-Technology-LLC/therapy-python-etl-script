"""
Complete AWS S3 Bucket Implementation in Python
Requires: pip install boto3 python-dotenv
"""

import os
import json
import boto3
import logging
from pathlib import Path
from typing import Optional
from botocore.exceptions import ClientError, NoCredentialsError

from src.config.config import Config


# ──────────────────────────────────────────────
# Logging Setup
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# S3Manager Class
# ──────────────────────────────────────────────
class AwsS3Helper:
    """
    A complete AWS S3 manager class for all common bucket and object operations.

    Environment Variables (.env):
        AWS_ACCESS_KEY_ID      – Your AWS access key
        AWS_SECRET_ACCESS_KEY  – Your AWS secret key
        AWS_REGION             – AWS region (default: us-east-1)
        NODE_ENV               – Environment folder prefix: development | staging | production
                                 All object keys are automatically scoped under this folder.
                                 The folder is created in each bucket if it doesn't exist yet.
    """

    def __init__(self):

        self.s3_bucket_config = Config.get_s3_bucket()
        self.bucket_name = self.s3_bucket_config["bucket_name"]

        # Trailing slash marks it as a folder placeholder in S3
        self.env_folder = f"{Config.get_application()["node_env"]}/"
        # ───────────────────────────────────────────────────────────────────

        try:
            self.s3 = boto3.client(
                "s3",
                region_name=self.s3_bucket_config["region"],
                aws_access_key_id=self.s3_bucket_config["access_key"],
                aws_secret_access_key=self.s3_bucket_config["secret_key"],
            )
            self.s3_resource = boto3.resource(
                "s3",
                region_name=self.s3_bucket_config["region"],
                aws_access_key_id=self.s3_bucket_config["access_key"],
                aws_secret_access_key=self.s3_bucket_config["secret_key"],
            )

            logger.info(f"✅ S3Manager initialized '")
        except NoCredentialsError:
            logger.error(
                "❌ AWS credentials not found. Set them in .env or AWS config."
            )
            raise

    # ──────────────────────────────────────────
    # BUCKET OPERATIONS
    # ──────────────────────────────────────────
    def create_bucket(self) -> bool:
        """Create a new S3 bucket. Skips creation if bucket already exists."""
        if self.bucket_exists():
            logger.info(
                f"🪣  Bucket '{self.bucket_name}' already exists — skipping creation."
            )
            return True
        try:
            self.s3.create_bucket(Bucket=self.bucket_name)
            logger.info(f"✅ Bucket created: {self.bucket_name}")
            return True
        except ClientError as e:
            logger.error(f"❌ Failed to create bucket: {e}")
            return False

    def delete_bucket(self, force: bool = False) -> bool:
        """
        Delete a bucket. Set force=True to delete all objects first.
        Checks bucket existence before attempting deletion; creates it if missing
        (edge-case guard so callers never crash on a missing bucket).
        """
        if not self._ensure_bucket():
            return False
        try:
            if force:
                self.delete_all_objects()
            self.s3.delete_bucket(Bucket=self.bucket_name)
            logger.info(f"✅ Bucket deleted: {self.bucket_name}")
            return True
        except ClientError as e:
            logger.error(f"❌ Failed to delete bucket: {e}")
            return False

    def list_buckets(self) -> list[str]:
        """
        List all S3 buckets in the account.
        """
        try:
            response = self.s3.list_buckets()
            buckets = [b["Name"] for b in response.get("Buckets", [])]
            logger.info(f"📦 Found {len(buckets)} bucket(s)")
            return buckets
        except ClientError as e:
            logger.error(f"❌ Failed to list buckets: {e}")
            return []

    def bucket_exists(self) -> bool:
        """Check if a bucket exists and is accessible."""
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError:
            return False

    def _ensure_bucket(self) -> bool:
        """
        Internal helper — checks if a bucket exists; creates it if not.
        Returns True if the bucket is ready to use, False on failure.
        """
        if self.bucket_exists():
            logger.info(f"🪣  Bucket already exists: '{self.bucket_name}'")
            return True
        logger.warning(
            f"⚠️  Bucket '{self.bucket_name}' not found — creating it automatically..."
        )
        return self.create_bucket()

    # ──────────────────────────────────────────
    # NODE_ENV FOLDER HELPERS
    # ──────────────────────────────────────────
    def _prefix_key(self, s3_key: str) -> str:
        """
        Prepend the NODE_ENV folder to an S3 key.

        Examples (NODE_ENV=production):
            'report.csv'          ->  'production/report.csv'
            'production/x.txt'    ->  'production/x.txt'   (no double-prefix)
        """
        if s3_key.startswith(self.env_folder):
            return s3_key
        return f"{self.env_folder}{s3_key}"

    def folder_exists(self, folder_prefix: str) -> bool:
        """
        Check whether a folder placeholder exists.
        Works even if the folder was created implicitly by uploading files into it.
        """
        prefix = folder_prefix if folder_prefix.endswith("/") else f"{folder_prefix}/"
        try:
            resp = self.s3.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix, MaxKeys=1
            )
            return resp.get("KeyCount", 0) > 0
        except ClientError:
            return False

    def create_folder(self, folder_prefix: str) -> bool:
        """
        Create a folder placeholder in S3 (zero-byte object with a trailing '/').
        S3 has no real directories — this is the standard convention used by the
        AWS Console and most S3 clients.
        """
        prefix = folder_prefix if folder_prefix.endswith("/") else f"{folder_prefix}/"
        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=prefix, Body=b"")
            logger.info(f"📁 Folder created: s3://{self.bucket_name}/{prefix}")
            return True
        except ClientError as e:
            logger.error(f"❌ Failed to create folder '{prefix}': {e}")
            return False

    def _ensure_env_folder(self) -> bool:
        """
        Internal helper — ensures the NODE_ENV folder exists inside a bucket.

        Flow:
          1. Bucket must already exist (call _ensure_bucket first).
          2. Check if  {NODE_ENV}/  placeholder is present.
          3. If not -> create it as a zero-byte object.

        Returns True when the folder is ready, False on creation failure.
        """
        if self.folder_exists(self.env_folder):
            logger.info(
                f"📂 Env folder already exists: s3://{self.bucket_name}/{self.env_folder}"
            )
            return True
        logger.warning(
            f"⚠️  Env folder '{self.env_folder}' not found in '{self.bucket_name}' "
            f"— creating it automatically..."
        )
        return self.create_folder(self.env_folder)

    def _ensure_bucket_and_folder(self) -> bool:
        """
        Convenience wrapper: ensure bucket exists AND env folder exists inside it.
        Used as a single guard at the start of every object-level operation.
        """
        if not self._ensure_bucket():
            return False
        return self._ensure_env_folder()

    # ──────────────────────────────────────────
    # UPLOAD OPERATIONS
    # ──────────────────────────────────────────

    def upload_file(
        self,
        local_path: str,
        s3_key: str = None,
        extra_args: dict = None,
    ) -> bool:
        """
        Upload a local file to S3.
        Auto-creates the bucket and the NODE_ENV folder if they do not exist.
        The key is automatically prefixed with the NODE_ENV folder.

        Args:
            bucket_name: Target bucket name
            local_path:  Path to local file
            s3_key:      S3 object key (defaults to filename, scoped under NODE_ENV/)
            extra_args:  Extra args like {'ContentType': 'image/png', 'ACL': 'public-read'}
        """
        if not self._ensure_bucket_and_folder():
            return False
        s3_key = self._prefix_key(s3_key or Path(local_path).name)
        try:
            self.s3.upload_file(
                local_path, self.bucket_name, s3_key, ExtraArgs=extra_args or {}
            )
            logger.info(f"✅ Uploaded: {local_path} → s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"❌ Upload failed: {e}")
            return False

    def upload_bytes(
        self,
        data: bytes,
        s3_key: str,
        content_type: str = "application/octet-stream",
    ) -> bool:
        """Upload raw bytes or a string directly to S3 (no local file needed).
        Auto-creates the bucket and NODE_ENV folder if they do not exist.
        The key is automatically prefixed with the NODE_ENV folder."""
        if not self._ensure_bucket_and_folder():
            return False
        s3_key = self._prefix_key(s3_key)
        try:
            self.s3.put_object(
                Bucket=self.bucket_name, Key=s3_key, Body=data, ContentType=content_type
            )
            logger.info(f"✅ Uploaded bytes → s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"❌ Failed to upload bytes: {e}")
            return False

    def upload_folder(self, local_folder: str, s3_prefix: str = "") -> int:
        """
        Recursively upload an entire local folder to S3.
        Auto-creates the bucket and NODE_ENV folder if they do not exist.
        All keys are scoped under  NODE_ENV/{s3_prefix}/...
        Returns the number of files uploaded.
        """
        if not self._ensure_bucket_and_folder():
            return 0
        folder = Path(local_folder)
        count = 0
        for file_path in folder.rglob("*"):
            if file_path.is_file():
                relative = file_path.relative_to(folder)
                # Build key relative to the env folder; upload_file will add the prefix
                rel_key = (
                    f"{s3_prefix}/{relative}".lstrip("/")
                    if s3_prefix
                    else str(relative)
                )
                if self.upload_file(str(file_path), rel_key):
                    count += 1
        logger.info(
            f"✅ Uploaded {count} file(s) from '{local_folder}' → s3://{self.bucket_name}/{self.env_folder}"
        )
        return count

    # ──────────────────────────────────────────
    # DOWNLOAD OPERATIONS
    # ──────────────────────────────────────────

    def download_file(self, s3_key: str, local_path: str) -> bool:
        """Download a file from S3 to a local path."""
        try:
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            self.s3.download_file(self.bucket_name, s3_key, local_path)
            logger.info(
                f"✅ Downloaded: s3://{self.bucket_name}/{s3_key} → {local_path}"
            )
            return True
        except ClientError as e:
            logger.error(f"❌ Download failed: {e}")
            return False

    def read_object(self, bucket_name: str, s3_key: str) -> Optional[bytes]:
        """Read an S3 object's content directly into memory."""
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=s3_key)
            content = response["Body"].read()
            logger.info(f"✅ Read object: s3://{bucket_name}/{s3_key}")
            return content
        except ClientError as e:
            logger.error(f"❌ Failed to read object: {e}")
            return None

    # ──────────────────────────────────────────
    # LIST & SEARCH OPERATIONS
    # ──────────────────────────────────────────

    def list_objects(self) -> list[dict]:
        """
        List all objects in a bucket (handles pagination automatically).
        Returns list of dicts with Key, Size, LastModified, ETag.
        """
        objects = []
        paginator = self.s3.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(
                Bucket=self.bucket_name, Prefix=self.env_folder
            ):
                for obj in page.get("Contents", []):
                    objects.append(
                        {
                            "key": obj["Key"],
                            "size_bytes": obj["Size"],
                            "last_modified": obj["LastModified"].isoformat(),
                            "etag": obj["ETag"].strip('"'),
                        }
                    )
            logger.info(f"📄 Listed {len(objects)} object(s) in '{self.bucket_name}'")
            return objects
        except ClientError as e:
            logger.error(f"❌ Failed to list objects: {e}")
            return []

    def object_exists(self, s3_key: str) -> bool:
        """Check if a specific object exists in a bucket."""
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError:
            return False

    def get_object_metadata(self, s3_key: str) -> Optional[dict]:
        """Get metadata and properties of an S3 object."""
        try:
            response = self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            return {
                "content_type": response.get("ContentType"),
                "content_length": response.get("ContentLength"),
                "last_modified": (
                    response.get("LastModified", "").isoformat()
                    if response.get("LastModified")
                    else None
                ),
                "etag": response.get("ETag", "").strip('"'),
                "metadata": response.get("Metadata", {}),
            }
        except ClientError as e:
            logger.error(f"❌ Failed to get metadata: {e}")
            return None

    # ──────────────────────────────────────────
    # DELETE OPERATIONS
    # ──────────────────────────────────────────

    def delete_object(self, s3_key: str) -> bool:
        """Delete a single object from S3.
        Auto-creates bucket and NODE_ENV folder if they do not exist.
        The key is automatically prefixed with the NODE_ENV folder."""
        if not self._ensure_bucket_and_folder():
            return False
        s3_key = self._prefix_key(s3_key)
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"✅ Deleted: s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"❌ Failed to delete object: {e}")
            return False

    def delete_objects_batch(self, s3_keys: list[str]) -> bool:
        """Efficiently delete multiple objects in a single API call (max 1000).
        Auto-creates bucket and NODE_ENV folder if they do not exist.
        All keys are automatically prefixed with the NODE_ENV folder."""
        if not self._ensure_bucket_and_folder():
            return False
        prefixed_keys = [self._prefix_key(k) for k in s3_keys]
        try:
            objects = [{"Key": key} for key in prefixed_keys]
            # AWS allows max 1000 per request
            for i in range(0, len(objects), 1000):
                batch = objects[i : i + 1000]
                self.s3.delete_objects(
                    Bucket=self.bucket_name, Delete={"Objects": batch, "Quiet": True}
                )
            logger.info(
                f"✅ Deleted {len(s3_keys)} object(s) from '{self.bucket_name}/{self.env_folder}'"
            )
            return True
        except ClientError as e:
            logger.error(f"❌ Batch delete failed: {e}")
            return False

    def delete_all_objects(self) -> bool:
        """Delete ALL objects under the NODE_ENV folder in a bucket.
        Auto-creates bucket and NODE_ENV folder if they do not exist."""
        if not self._ensure_bucket_and_folder():
            return False
        # Only list objects scoped to the current env folder
        objects = self.list_objects(prefix=self.env_folder)
        if not objects:
            logger.info(
                f"ℹ️  No objects to delete in '{self.bucket_name}/{self.env_folder}'"
            )
            return True
        # Keys already carry the env prefix, so pass raw keys directly (no double-prefix)
        raw_keys = [obj["key"] for obj in objects]
        try:
            s3_objects = [{"Key": key} for key in raw_keys]
            for i in range(0, len(s3_objects), 1000):
                batch = s3_objects[i : i + 1000]
                self.s3.delete_objects(
                    Bucket=self.bucket_name, Delete={"Objects": batch, "Quiet": True}
                )
            logger.info(
                f"✅ Deleted {len(raw_keys)} object(s) from '{self.bucket_name}/{self.env_folder}'"
            )
            return True
        except ClientError as e:
            logger.error(f"❌ Failed to delete all objects: {e}")
            return False

    # # ──────────────────────────────────────────
    # # COPY & MOVE OPERATIONS
    # # ──────────────────────────────────────────

    # def copy_object(
    #     self,
    #     src_bucket: str,
    #     src_key: str,
    #     dst_bucket: str,
    #     dst_key: str,
    # ) -> bool:
    #     """
    #     Copy an object from one location to another (within or across buckets).
    #     Auto-ensures both source and destination buckets exist AND that the
    #     NODE_ENV folder exists in each. Keys are prefixed with NODE_ENV/ automatically.
    #     """
    #     if not self._ensure_bucket_and_folder(src_bucket):
    #         logger.error(
    #             f"❌ Could not ensure source bucket/folder '{src_bucket}/{self.env_folder}'"
    #         )
    #         return False
    #     if not self._ensure_bucket_and_folder(dst_bucket):
    #         logger.error(
    #             f"❌ Could not ensure destination bucket/folder '{dst_bucket}/{self.env_folder}'"
    #         )
    #         return False
    #     src_key = self._prefix_key(src_key)
    #     dst_key = self._prefix_key(dst_key)
    #     try:
    #         self.s3.copy_object(
    #             CopySource={"Bucket": src_bucket, "Key": src_key},
    #             Bucket=dst_bucket,
    #             Key=dst_key,
    #         )
    #         logger.info(
    #             f"✅ Copied: s3://{src_bucket}/{src_key} → s3://{dst_bucket}/{dst_key}"
    #         )
    #         return True
    #     except ClientError as e:
    #         logger.error(f"❌ Copy failed: {e}")
    #         return False

    # def move_object(
    #     self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str
    # ) -> bool:
    #     """Move (copy + delete) an object to a new location."""
    #     if self.copy_object(src_bucket, src_key, dst_bucket, dst_key):
    #         return self.delete_object(src_bucket, src_key)
    #     return False

    # ──────────────────────────────────────────
    # PRESIGNED URL OPERATIONS
    # ──────────────────────────────────────────

    def generate_presigned_url(
        self,
        s3_key: str,
        expiration: int = 3600,
        operation: str = "get_object",
    ) -> Optional[str]:
        """
        Generate a time-limited presigned URL for secure access.

        Args:
            bucket_name: Bucket name
            s3_key:      Object key
            expiration:  Seconds until URL expires (default: 1 hour)
            operation:   'get_object' (download) or 'put_object' (upload)
        """
        try:
            url = self.s3.generate_presigned_url(
                ClientMethod=operation,
                Params={"Bucket": self.bucket_name, "Key": self._prefix_key(s3_key)},
                ExpiresIn=expiration,
            )
            logger.info(f"🔗 Presigned URL generated (expires in {expiration}s)")
            return url
        except ClientError as e:
            logger.error(f"❌ Failed to generate presigned URL: {e}")
            return None

    # ──────────────────────────────────────────
    # BUCKET POLICY & SETTINGS
    # ──────────────────────────────────────────

    def enable_versioning(self) -> bool:
        """Enable versioning on a bucket to protect against accidental deletes."""
        try:
            self.s3.put_bucket_versioning(
                Bucket=self.bucket_name,
                VersioningConfiguration={"Status": "Enabled"},
            )
            logger.info(f"✅ Versioning enabled on '{self.bucket_name}'")
            return True
        except ClientError as e:
            logger.error(f"❌ Failed to enable versioning: {e}")
            return False

    def set_bucket_policy(self, policy: dict) -> bool:
        """Apply an IAM-style JSON bucket policy."""
        try:
            self.s3.put_bucket_policy(
                Bucket=self.bucket_name, Policy=json.dumps(policy)
            )
            logger.info(f"✅ Bucket policy applied to '{self.bucket_name}'")
            return True
        except ClientError as e:
            logger.error(f"❌ Failed to set bucket policy: {e}")
            return False

    def get_bucket_policy(self) -> Optional[dict]:
        """Retrieve the current bucket policy."""
        try:
            response = self.s3.get_bucket_policy(Bucket=self.bucket_name)
            return json.loads(response["Policy"])
        except ClientError as e:
            logger.error(f"❌ Failed to get bucket policy: {e}")
            return None

    def set_lifecycle_rule(self, days_to_expire: int = 30) -> bool:
        """
        Add a lifecycle rule to automatically delete objects after N days.
        Useful for cost management on temporary storage.
        """
        try:
            self.s3.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration={
                    "Rules": [
                        {
                            "ID": "auto-expire",
                            "Status": "Enabled",
                            "Filter": {"Prefix": ""},
                            "Expiration": {"Days": days_to_expire},
                        }
                    ]
                },
            )
            logger.info(
                f"✅ Lifecycle rule set: objects expire after {days_to_expire} days"
            )
            return True
        except ClientError as e:
            logger.error(f"❌ Failed to set lifecycle rule: {e}")
            return False

    # ──────────────────────────────────────────
    # MULTIPART UPLOAD (for large files > 100MB)
    # ──────────────────────────────────────────

    def multipart_upload(
        self,
        local_path: str,
        s3_key: str,
        part_size_mb: int = 10,
    ) -> bool:
        """
        Upload a large file using S3 multipart upload.
        Recommended for files over 100MB.
        Auto-creates the bucket and NODE_ENV folder if they do not exist.
        The key is automatically prefixed with the NODE_ENV folder.

        Args:
            bucket_name:   Target bucket
            local_path:    Path to large local file
            s3_key:        S3 object key (will be scoped under NODE_ENV/)
            part_size_mb:  Size of each part in MB (min 5MB)
        """
        if not self._ensure_bucket_and_folder():
            return False
        s3_key = self._prefix_key(s3_key)

        part_size = max(part_size_mb, 5) * 1024 * 1024  # minimum 5MB
        parts = []

        try:
            # Initiate upload
            mpu = self.s3.create_multipart_upload(Bucket=self.bucket_name, Key=s3_key)
            upload_id = mpu["UploadId"]
            logger.info(f"🚀 Starting multipart upload: {local_path}")

            with open(local_path, "rb") as f:
                part_num = 1
                while chunk := f.read(part_size):
                    response = self.s3.upload_part(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        UploadId=upload_id,
                        PartNumber=part_num,
                        Body=chunk,
                    )
                    parts.append({"PartNumber": part_num, "ETag": response["ETag"]})
                    logger.info(
                        f"  📤 Uploaded part {part_num} ({len(chunk) / 1024 / 1024:.1f} MB)"
                    )
                    part_num += 1

            # Complete the upload
            self.s3.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            logger.info(
                f"✅ Multipart upload complete → s3://{self.bucket_name}/{s3_key}"
            )
            return True

        except (ClientError, OSError) as e:
            logger.error(f"❌ Multipart upload failed: {e}")
            try:
                self.s3.abort_multipart_upload(
                    Bucket=self.bucket_name, Key=s3_key, UploadId=upload_id
                )
            except Exception:
                pass
            return False


aws_s3_helper = AwsS3Helper()
