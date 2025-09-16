from snapred.meta.decorators.Singleton import Singleton


@Singleton
class PubSubManager:
    """
    Manages the publish-subscribe mechanism for inter-component communication.
    """

    def __init__(self):
        self.subscribers = {}

    def subscribe(self, eventType: str, callback):
        if eventType not in self.subscribers:
            self.subscribers[eventType] = []
        self.subscribers[eventType].append(callback)

    def unsubscribe(self, eventType: str, callback):
        if eventType in self.subscribers:
            self.subscribers[eventType].remove(callback)
            if not self.subscribers[eventType]:
                del self.subscribers[eventType]

    def publish(self, eventType: str, data):
        if eventType in self.subscribers:
            for callback in self.subscribers[eventType]:
                callback(data)
