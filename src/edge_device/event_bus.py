# event_bus.py
import asyncio
import logging

logger = logging.getLogger(__name__)

class EventBus:
    def __init__(self):
        self.listeners = {}
        logger.debug("EventBus initialized with an empty listeners dictionary.")

    def subscribe(self, event_type, listener):
        if event_type not in self.listeners:
            self.listeners[event_type] = []
            logger.debug(f"New event type '{event_type}' added to listeners.")
        self.listeners[event_type].append(listener)
        logger.debug(f"Listener added for event type '{event_type}'.")

    async def publish(self, event_type, data):
        if event_type in self.listeners:
            logger.debug(f"Publishing data to listeners of event type '{event_type}'.")
            for listener in self.listeners[event_type]:
                if asyncio.iscoroutinefunction(listener):
                    await listener(data)
                    logger.debug(f"Async listener called for event type '{event_type}'.")
                else:
                    listener(data)
                    logger.debug(f"Sync listener called for event type '{event_type}'.")
        else:
            logger.debug(f"No listeners found for event type '{event_type}'. No action taken.")