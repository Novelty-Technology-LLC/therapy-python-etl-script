from typing import List

from src.shared.interface.etl.migration import FileMetadata
from src.core.service.subscribers.entity import IArdbSubscriber, ITherapySubscriber
from src.core.service.subscribers.mapper import subscriber_mapper


class SubscriberAdapter:
    """Subscriber Adapter"""

    def to_ardb_format(
        self, subscribers: List[ITherapySubscriber]
    ) -> List[IArdbSubscriber]:
        """Convert the subscriber dataframe to ardb format"""
        return [subscriber_mapper.to_ardb(subscriber) for subscriber in subscribers]

    def to_therapy_format(
        self,
        subscribers: List[IArdbSubscriber],
        file_metadata: FileMetadata,
    ) -> List[ITherapySubscriber]:
        """Convert the subscriber dataframe to therapy format"""
        return [
            subscriber_mapper.to_therapy(subscriber, file_metadata)
            for subscriber in subscribers
        ]


subscriber_adapter = SubscriberAdapter()
