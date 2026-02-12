from src.shared.helper.mongodb_helper import mongodb_helper
from datetime import datetime


class BaseEtl:
    def __init__(self):
        self.start_time = None
        self.connection = None

    def __enter__(self):
        """Setup - runs when entering 'with' block"""
        self.start_time = datetime.now()
        self.connection = mongodb_helper.connect_db()
        print(f"[{self.start_time}] ETL Started - MongoDB Connected")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup - runs when exiting 'with' block (even on errors)"""
        end_time = datetime.now()
        duration = (
            (end_time - self.start_time).total_seconds() if self.start_time else 0
        )

        # Close MongoDB connection
        try:
            mongodb_helper.close()
            print(
                f"[{end_time}] ETL Completed - MongoDB Connection Closed (Duration: {duration:.2f}s)"
            )
        except Exception as e:
            print(f"Warning: Error during cleanup: {e}")

        # Return False to propagate exceptions, True to suppress them
        return False

    def cleanup(self):
        """Alternative explicit cleanup method if not using context manager"""
        try:
            mongodb_helper.close()
            print("ETL Cleanup completed")
        except Exception as e:
            print(f"Warning: Error during cleanup: {e}")
