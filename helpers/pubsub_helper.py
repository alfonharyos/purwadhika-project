import json
from typing import Dict, Any, List
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists


class PubSubHelper:
    def __init__(self, project_id: str = None):
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

    # ========== Publisher ==========
    def publish_message(self, topic_id: str, message: Dict[str, Any]) -> str:
        """
        Publish a message to Pub/Sub topic.
        """
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        data = json.dumps(message).encode("utf-8")
        future = self.publisher.publish(topic_path, data)
        return future.result()
    
    def publish_batch(self, topic_id: str, messages: List[Dict[str, Any]]) -> List[str]:
        """
        Publish multiple messages in batch
        """
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        futures = []
        for msg in messages:
            data = json.dumps(msg).encode("utf-8")
            futures.append(self.publisher.publish(topic_path, data))
        return [f.result() for f in futures]

    # ========== Subscriber ==========
    def pull_messages(self, subscription_id: str, max_messages: int = 10) -> List[Dict[str, Any]]:
        """
        Pull messages from Pub/Sub subscription (sync).
        """
        subscription_path = self.subscriber.subscription_path(self.project_id, subscription_id)
        response = self.subscriber.pull(
            request={"subscription": subscription_path, "max_messages": max_messages}
        )

        messages = []
        ack_ids = []
        for msg in response.received_messages:
            payload = json.loads(msg.message.data.decode("utf-8"))
            messages.append(payload)
            ack_ids.append(msg.ack_id)

        # Acknowledge messages
        if ack_ids:
            self.subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )

        return messages

    # ========== Admin ==========
    def create_topic(self, topic_id: str) -> str:
        """
        Create a new Pub/Sub topic if not exists
        """
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        try:
            topic = self.publisher.create_topic(request={"name": topic_path})
            print(f"[PubSub] Created topic: {topic.name}")
            return f"[PubSub] Created topic: {topic.name}"
        except AlreadyExists:
            print(f"[PubSub] Topic already exists: {topic_path}")
        except Exception as e:
            print(f"[PubSub] Failed to create topic: {e}")

    def create_subscription(self, topic_id: str, subscription_id: str) -> str:
        """
        Create a new subscription for a topic if not exists
        """
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        subscription_path = self.subscriber.subscription_path(self.project_id, subscription_id)
        try:
            self.subscriber.create_subscription(
                request={"name": subscription_path, "topic": topic_path}
            )
            print(f"[PubSub] Created subscription: {subscription_path}")
            return f"Created subscription: {subscription_path}"
        except AlreadyExists:
            print(f"[PubSub] Subscription already exists: {subscription_path}")
        except Exception as e:
            print(f"[PubSub] Failed to create subscription: {e}")
