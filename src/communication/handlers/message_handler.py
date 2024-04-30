from abc import ABC, abstractmethod

class MessageHandler(ABC):
    @abstractmethod
    def process(self, parsed_data, device_id, experiment_id):
        pass