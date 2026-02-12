from typing import Optional
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, ConfigurationError

from src.config.config import Config


class MongoDBHelper:
    """MongoDB connection helper with singleton pattern"""

    _client: Optional[MongoClient] = None
    _database: Optional[Database] = None

    @classmethod
    def connect_db(cls, use_atlas: bool = True) -> Database:
        """
        Connect to MongoDB and return database instance.
        """
        database_name = Config.get_db().get("database")
        print(f"Connected to {database_name} successfully")
        cls._get_database()

    @classmethod
    def _connect(cls, use_atlas: bool = True) -> MongoClient:
        """
        Connect to MongoDB and return client instance.
        Uses singleton pattern to reuse connection.

        Args:
            use_atlas: If True, uses mongodb+srv:// for Atlas.
                      If False, uses standard mongodb:// for local.

        Returns:
            MongoClient instance

        Raises:
            ConnectionFailure: If connection fails
            ConfigurationError: If configuration is invalid
        """
        if cls._client is None:
            try:
                connection_string = Config.resolve_uri(use_atlas=use_atlas)
                cls._client = MongoClient(
                    connection_string,
                    serverSelectionTimeoutMS=5000,  # 5 second timeout
                    connectTimeoutMS=10000,  # 10 second connection timeout
                )

                # Test connection
                cls._client.admin.command("ping")

            except ConnectionFailure as e:
                cls._client = None
                raise ConnectionFailure(f"Failed to connect to MongoDB: {e}")
            except ConfigurationError as e:
                cls._client = None
                raise ConfigurationError(f"Invalid MongoDB configuration: {e}")

        return cls._client

    @classmethod
    def _get_database(cls) -> Database:
        """
        Get database instance.

        Args:
            database_name: Database name. If None, uses from config.

        Returns:
            Database instance
        """
        if cls._client is None:
            cls._connect()

        db_config = Config.get_db()
        database_name = db_config.get("database")

        if cls._database is None or cls._database.name != database_name:
            cls._database = cls._client[database_name]

        return cls._database

    @classmethod
    def get_client(cls) -> Optional[MongoClient]:
        """
        Get existing MongoDB client instance.

        Returns:
            MongoClient instance or None if not connected
        """
        return cls._client

    @classmethod
    def close(cls) -> None:
        """
        Close MongoDB connection.
        """
        if cls._client is not None:
            cls._client.close()
            cls._client = None
            cls._database = None

    @classmethod
    def is_connected(cls) -> bool:
        """
        Check if MongoDB is connected.

        Returns:
            True if connected, False otherwise
        """
        if cls._client is None:
            return False

        try:
            cls._client.admin.command("ping")
            return True
        except Exception:
            return False


mongodb_helper = MongoDBHelper()
