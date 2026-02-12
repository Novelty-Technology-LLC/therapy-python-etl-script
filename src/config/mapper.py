import os
from dotenv import load_dotenv

load_dotenv();


class ConfigMapper:
    MONGO_DB_PORT = os.getenv('MONGO_DB_PORT')
    MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')
    MONGO_CLUSTER_NAME = os.getenv('MONGO_CLUSTER_NAME')
    MONGO_USERNAME = os.getenv('MONGO_USERNAME')
    MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
    MONGO_DB_PROTOCOL = os.getenv('MONGO_DB_PROTOCOL')

    MONGO_AUTO_ENCRYPTION_SHARED_LIB_PATH = os.getenv('MONGO_AUTO_ENCRYPTION_SHARED_LIB_PATH')
    KEY_VAULT_COLLECTION_NAME = os.getenv('KEY_VAULT_COLLECTION_NAME')
    KEY_VAULT_DATA_KEY_NAME = os.getenv('KEY_VAULT_DATA_KEY_NAME')

    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
    AWS_KEY_ARN = os.getenv('AWS_KEY_ARN')
    AWS_REGION = os.getenv('AWS_REGION')

    @classmethod
    def get(cls, key, default=None):
        """Get config value by key name"""
        return getattr(cls, key, default)