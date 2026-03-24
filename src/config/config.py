from typing import Optional, TypedDict
from urllib.parse import quote_plus

from src.config.mapper import ConfigMapper


class MongoDbConfigType(TypedDict):
    protocol: str
    cluster_name: str
    port: Optional[int]
    database: str
    username: str
    password: str


class MongoDbEncryptionVaultConfigType(TypedDict):
    shared_lib_path: str
    collection_name: str
    data_key_name: str


class MongoDbEncryptionAwsConfigType(TypedDict):
    access_key: str
    secret_key: str
    key_arn: str
    region: str


class MongoDbEncryptionConfigType(TypedDict):
    vault: MongoDbEncryptionVaultConfigType
    aws: MongoDbEncryptionAwsConfigType


class DocumentsConfigType(TypedDict):
    support_duplicate_documents: bool


class ApplicationConfig(TypedDict):
    node_env: str


class S3BucketConfig(TypedDict):
    bucket_name: str
    access_key: str
    secret_key: str
    region: str


class Config:
    @staticmethod
    def get_db() -> MongoDbConfigType:
        """Get MongoDB configuration"""
        return {
            "protocol": ConfigMapper.MONGO_DB_PROTOCOL,
            "cluster_name": ConfigMapper.MONGO_CLUSTER_NAME,
            "port": (
                int(ConfigMapper.MONGO_DB_PORT) if ConfigMapper.MONGO_DB_PORT else None
            ),
            "database": ConfigMapper.MONGO_DB_NAME,
            "username": quote_plus(ConfigMapper.MONGO_USERNAME),
            "password": quote_plus(ConfigMapper.MONGO_PASSWORD),
        }

    @staticmethod
    def get_db_encryption() -> MongoDbEncryptionConfigType:
        """Get MongoDB encryption configuration"""
        return {
            "vault": MongoDbEncryptionVaultConfigType(
                shared_lib_path=(
                    ConfigMapper.MONGO_AUTO_ENCRYPTION_SHARED_LIB_PATH
                    if ConfigMapper.MONGO_AUTO_ENCRYPTION_SHARED_LIB_PATH
                    else None
                ),
                collection_name=ConfigMapper.KEY_VAULT_COLLECTION_NAME,
                data_key_name=ConfigMapper.KEY_VAULT_DATA_KEY_NAME,
            ),
            "aws": MongoDbEncryptionAwsConfigType(
                access_key=ConfigMapper.AWS_ACCESS_KEY,
                secret_key=ConfigMapper.AWS_SECRET_KEY,
                key_arn=ConfigMapper.AWS_KEY_ARN,
                region=ConfigMapper.AWS_REGION,
            ),
        }

    @staticmethod
    def resolve_uri(use_atlas: bool = True) -> str:
        """
        Resolve MongoDB connection URI.

        Args:
            use_atlas: If True, returns mongodb+srv:// format for Atlas.
                      If False, returns standard mongodb:// format.

        Returns:
            MongoDB connection string
        """
        db_config = Config.get_db()

        cluster_name = db_config["cluster_name"]
        username = db_config["username"]
        password = db_config["password"]

        if use_atlas:
            # MongoDB Atlas format
            return f"mongodb+srv://{username}:{password}@{cluster_name}.mongodb.net/?retryWrites=true&w=majority"
        else:
            # Local MongoDB format
            protocol = db_config["protocol"] or "mongodb"
            port = db_config["port"]

            auth = f"{username}:{password}@" if username and password else ""
            port_str = f":{port}" if port else ""

            return f"{protocol}://{auth}{cluster_name}{port_str}/"

    @staticmethod
    def get_documents() -> DocumentsConfigType:
        """Get documents configuration"""
        return {
            "support_duplicate_documents": (
                True if ConfigMapper.SUPPORT_DUPLICATE_DOCUMENTS == "true" else False
            ),
        }

    @staticmethod
    def get_application() -> ApplicationConfig:
        """Get application configuration"""
        return {
            "node_env": ConfigMapper.NODE_ENV,
        }

    @staticmethod
    def get_s3_bucket() -> S3BucketConfig:
        """Get S3 bucket configuration"""
        return {
            "bucket_name": ConfigMapper.AWS_S3_BUCKET_NAME,
            "access_key": ConfigMapper.AWS_S3_ACCESS_KEY,
            "secret_key": ConfigMapper.AWS_S3_SECRET_KEY,
            "region": ConfigMapper.AWS_S3_REGION,
        }
