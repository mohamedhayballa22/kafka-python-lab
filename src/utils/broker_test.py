from typing import Dict
from confluent_kafka.admin import AdminClient


def check_kafka_connection(
   broker_url: str = "localhost:9092", 
   timeout: float = 5.0
) -> bool:
   """
   Test the connection to a Kafka broker.
   
   Attempts to connect to a Kafka broker and retrieve cluster metadata to verify
   the connection is working properly. Prints status information to stdout.
   
   Args:
       broker_url: The Kafka broker URL to connect to. Defaults to "localhost:9092".
       timeout: Connection timeout in seconds. Defaults to 5.0.
       
   Returns:
       bool: True if connection was successful, False otherwise.
   """
   print(f"Testing Kafka connection to {broker_url}...")
   
   # Create an AdminClient to check the connection
   admin_config: Dict[str, str] = {"bootstrap.servers": broker_url}
   admin_client: AdminClient = AdminClient(admin_config)
   
   # Try to get cluster metadata with a short timeout
   try:
       metadata = admin_client.list_topics(timeout=timeout)
       broker_metadata = metadata.brokers
       
       if len(broker_metadata) > 0:
           print(f"✅ SUCCESS: Connected to Kafka broker at {broker_url}")
           print(f"Found {len(broker_metadata)} broker(s):")
           for broker_id, broker in broker_metadata.items():
               print(f"  - Broker ID: {broker_id}, Host: {broker.host}, Port: {broker.port}")
           return True
       else:
           print("❌ ERROR: No brokers found in metadata response")
           return False
           
   except Exception as e:
       print(f"❌ ERROR: Could not connect to Kafka: {e}")
       return False


if __name__ == "__main__":
   check_kafka_connection()