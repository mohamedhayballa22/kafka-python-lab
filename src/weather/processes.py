from helpers import (
    generate_weather_data,
    send_data,
    analyze_weather_data,
)
from kafka import KafkaProducer, KafkaConsumer
from datetime import date, timedelta
import json


def producer_process(producer: KafkaProducer, raw_weather_topic: str) -> None:
    """Function to generate and send weather data continuously.

    Args:
        producer: An instance of KafkaProducer to use for sending data.
        raw_weather_topic: The name of the Kafka topic to send data to.

    Returns:
        None
    """
    print("Producer thread started - generating and sending weather data...")

    try:
        date_param = date.today()
        while True:
            weather_data = generate_weather_data(date_param)

            # Send data to Kafka
            for message in weather_data:
                send_data(producer, raw_weather_topic, message)
                print("Sent message")

            date_param += timedelta(days=1)

            # Wait before generating more data
            # time.sleep(10)

    except Exception as e:
        print(f"Producer error: {e}")
    finally:
        print("Producer shutting down")


def consumer_process(
    consumer: KafkaConsumer,
    producer: KafkaProducer,
    critical_topic: str,
    weather_stats_topic: str,
) -> None:
    """Function to continuously consume and process weather data.

    Args:
        consumer: An instance of KafkaConsumer to consume data from.

    Returns:
        None
    """
    print("Consumer thread started - waiting for messages...")

    try:
        messages = []
        # Loop through messages as they arrive
        for message in consumer:
            print("Received message")

            # Reformat messages
            message_value: bytes = message.value
            message = message_value.decode("utf-8")
            message = json.loads(message)

            # Store message
            messages.append(message)

            # Analyze daily batch
            if (
                messages
                and isinstance(messages[0], dict)
                and messages[0].get("time", "").endswith("00:00")
                and isinstance(messages[-1], dict)
                and messages[-1].get("time", "").endswith("23:00")
            ):
                print(
                    f"Received batch of {len(messages)} weather records.. calculating stats"
                )
                stats, notifications = analyze_weather_data(messages)
                messages = []

                # Send data
                if stats:
                    send_data(producer, weather_stats_topic, stats)
                    print("Sent stats records")

                if notifications:
                    send_data(producer, critical_topic, notifications)
                    print("Sent critical notifications")

            # Clear list of messages if we can't find a day batch
            if (
                len(messages) > 24
                and isinstance(messages[-1], dict)
                and messages[-1].get("time", "").endswith("23:00")
            ):
                messages = []

    except Exception as e:
        print(f"Consumer error: {e}")
    finally:
        print("Consumer shutting down")
