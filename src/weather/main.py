from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
import threading
from processes import producer_process, consumer_process
import time
from helpers import create_topics


raw_weather_topic = "raw-weather-data"
critical_topic = "critical-notifications"
weather_stats_topic = "weather-data-stats"

client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id="weather-admin")
producer = KafkaProducer(bootstrap_servers="localhost:9092")
consumer = KafkaConsumer(
    raw_weather_topic,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

# Create threads
consumer_thread = threading.Thread(
    target=consumer_process,
    args=(consumer, producer, critical_topic, weather_stats_topic),
    daemon=True,
)

producer_thread = threading.Thread(
    target=producer_process, args=(producer, raw_weather_topic), daemon=True
)
if __name__ == "__main__":
    try:
        # Create topics
        print("Creating topics...")
        create_topics(client, [raw_weather_topic, critical_topic, weather_stats_topic])

        # Start consumer thread first
        print("Starting consumer thread...")
        consumer_thread.start()

        # Give the consumer a moment to initialize
        time.sleep(2)

        # Start producer thread
        print("Starting producer thread...")
        producer_thread.start()

        # Keep the main thread running
        print("Main application running. Press Ctrl+C to exit.")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nReceived keyboard interrupt, shutting down...")

    finally:
        # Clean up resources
        print("Closing Kafka connections...")
        client.close()
        producer.close()
        consumer.close()
        print("Application terminated")
