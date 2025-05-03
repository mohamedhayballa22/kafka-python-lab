from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError, NoBrokersAvailable


def check_kafka_connection(
   broker_url: str = "localhost:9092", 
   timeout_ms: int = 5000
) -> bool:
   """
   Test the connection to a Kafka broker using kafka-python.
   
   Attempts to connect to a Kafka broker and retrieve cluster metadata to verify
   the connection is working properly. Prints status information to stdout.
   
   Args:
       broker_url: The Kafka broker URL to connect to. Defaults to "localhost:9092".
       timeout_ms: Connection timeout in milliseconds. Defaults to 5000.
       
   Returns:
       bool: True if connection was successful, False otherwise.
       
   Raises:
       No exceptions are raised as they are caught and reported in the output.
   """
   print(f"Testing Kafka connection to {broker_url}...")
   
   # Create an AdminClient to check the connection
   try:
       admin_client = KafkaAdminClient(
           bootstrap_servers=[broker_url],
           client_id="connection-test",
           request_timeout_ms=timeout_ms
       )
       
       # Describe the cluster to verify the connection and get broker info
       cluster_metadata = admin_client.describe_cluster()
       brokers = cluster_metadata.get('brokers', [])
       
       if brokers:
           print(f"✅ SUCCESS: Connected to Kafka broker at {broker_url}")
           print(f"Found {len(brokers)} broker(s):")
           for broker in brokers:
               print(f"  - Broker ID: {broker['node_id']}, Host: {broker['host']}, Port: {broker['port']}")
           admin_client.close()
           return True
       else:
           print("❌ ERROR: No brokers found in metadata response")
           admin_client.close()
           return False
           
   except NoBrokersAvailable:
       print(f"❌ ERROR: No brokers available at {broker_url}")
       return False
   except KafkaError as e:
       print(f"❌ ERROR: Kafka error - {e}")
       return False
   except Exception as e:
       print(f"❌ ERROR: Could not connect to Kafka: {e}")
       return False


if __name__ == "__main__":
   check_kafka_connection()