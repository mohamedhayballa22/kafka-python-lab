from requests_sse import EventSource
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    client_id='firehose-producer'
)

def main():
    """
    Connects to the GitHub Firehose event stream, processes events,
    and sends them to a Kafka topic.

    The function fetches events from 'http://github-firehose.libraries.io/events'.
    Each event is parsed as JSON. The 'id' field from the event data is used
    as the Kafka message key (defaulting to 'no_id' if 'id' is not present).
    The entire event data is sent as the Kafka message value.

    It sends a maximum of 1000 messages before flushing the producer,
    printing metrics, and closing the producer.
    Handles KeyboardInterrupt for graceful shutdown.
    """
    print("Starting data feed...")
    try:
        message_count = 0
        with EventSource('http://github-firehose.libraries.io/events', timeout=5000) as events:
            for event in events:
                if event.data:
                    try:
                        # Prepare and send message
                        data = json.loads(event.data)
                        key = str(data.get('id', 'no_id'))
                        value = data
                        
                        producer.send(
                            'github-events',
                            key=key.encode('utf-8'),
                            value=json.dumps(value).encode('utf-8')
                        )
                        message_count += 1

                        # Send enough messages for batching to occur
                        if message_count >= 1000:
                            break
                        
                    except Exception as e:
                         print(f"Error processing message: {e}")


    except KeyboardInterrupt:
        print('Shutting down...')
    finally:
        print(f"Sent {message_count} messages. Flushing producer...")
        producer.flush()
        print("Producer flushed.")

        # Get metrics after flushing
        metrics = producer.metrics()
        print("\nFinal Producer Metrics:")
        print(json.dumps(metrics, indent=2))

        producer.close()
        print("Producer closed.")


if __name__ == '__main__':
    main()