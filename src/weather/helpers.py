from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from kafka.errors import TopicAlreadyExistsError
import random
from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple, Any
import json


def create_topics(admin_client: KafkaAdminClient, topics_names: List[str]) -> None:
    """Creates multiple Kafka topics in a single batch request.

    Note: This function hardcodes the number of partitions to 1 and the replication
    factor to 1 for all created topics.

    Args:
        admin_client: An instance of KafkaAdminClient to use for creating topics.
        topics_names: A list of strings, where each string is the name of a
                      Kafka topic to be created.

    Returns:
        None
    """
    new_topic_objects = [
        NewTopic(name=topic, num_partitions=1, replication_factor=1)
        for topic in topics_names
    ]

    if not new_topic_objects:
        print("No topics specified for creation.")
        return

    try:
        admin_client.create_topics(new_topics=new_topic_objects, validate_only=False)
        print(f"Successfully initiated creation for topics: {', '.join(topics_names)}.")
        print(
            "Note: Topic creation is asynchronous. Check broker logs or use list_topics for confirmation."
        )

    except TopicAlreadyExistsError:
        print(f"One or more topics in the list {topics_names} already exist.")

    except Exception as e:
        print(f"An error occurred during topic creation: {e}")


def generate_weather_data(date: date) -> List[Dict[str, Any]]:
    """Generate hourly weather data for a 24-hour period

    Args:
        date: The date for which to generate weather data.

    Returns:
        list of dicts: Hourly weather data with time, temperature, wind speed, and cloud cover
    """
    weather_data = []
    start_time = datetime(date.year, date.month, date.day, 0, 0)  # Start at midnight

    # Base values
    base_temp = 15 + random.uniform(-5, 5)
    base_wind = 3 + random.uniform(-1, 1)
    base_cloud = 40 + random.uniform(-20, 20)

    for i in range(24):
        hour = i % 24
        time = start_time + timedelta(hours=hour)

        # Add some variation but keep it logical
        temp = (
            base_temp + random.uniform(-3, 3) + (hour - 12) * 0.5
        )  # Warmer in afternoon
        wind = max(0, base_wind + random.uniform(-1, 1))
        cloud = min(100, max(0, base_cloud + random.uniform(-15, 15)))

        weather_data.append(
            {
                "time": time.strftime("%Y-%m-%d %H:%M"),
                "temperature": round(temp, 1),
                "wind_speed": round(wind, 1),
                "cloud_cover": round(cloud),
            }
        )

    return weather_data


def send_data(producer: KafkaProducer, topic: str, message: Dict) -> None:
    """Send data to a Kafka topic.

    Args:
        producer: An instance of KafkaProducer to use for sending data.
        topic: The name of the Kafka topic to send data to.
        data: A dictionary representing the data to be sent.

    Returns:
        None
    """
    try:
        producer.send(topic, value=json.dumps(message).encode("utf-8"))
    except Exception as e:
        print(f"Error sending message: {e}")
    producer.flush()


def analyze_weather_data(
    weather_data: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Analyzes weather data to calculate statistics and generate critical notifications.

    Args:
        weather_data: A list of dictionaries, where each dictionary represents
                      hourly weather data with 'time', 'temperature', 'wind_speed',
                      and 'cloud_cover' keys.

    Returns:
        A tuple containing two lists:
        - A list of dictionaries, each containing daily weather statistics.
        - A list of dictionaries, each representing a critical weather notification.
    """

    # Validate input
    if not weather_data or "time" not in weather_data[0]:
        print("Error: The first item in the list must contain a 'time' key")
        return [], []  # Return empty lists as per the return type hint

    # Extract date from the first item
    try:
        date_str = weather_data[0]["time"].split(" ")[0]
    except (KeyError, IndexError, AttributeError):
        return {"error": "Invalid 'time' format in the first item"}

    # Extract data
    temperatures = [hour.get("temperature") for hour in weather_data]
    wind_speeds = [hour.get("wind_speed") for hour in weather_data]
    cloud_covers = [hour.get("cloud_cover") for hour in weather_data]

    # Calculate relevant statistics
    stats = [
        {
            "date": date_str,
            "temperature": {
                "min": min(temperatures),
                "max": max(temperatures),
                "avg": sum(temperatures) / len(temperatures),
                "range": max(temperatures) - min(temperatures),
            },
            "wind_speed": {
                "avg": sum(wind_speeds) / len(wind_speeds) if wind_speeds else None,
                "max": max(wind_speeds) if wind_speeds else None,
                "hours_above_5": (
                    sum(1 for speed in wind_speeds if speed > 5) if wind_speeds else 0
                ),
            },
            "cloud_cover": {
                "avg": sum(cloud_covers) / len(cloud_covers) if cloud_covers else None,
                "hours_clear": (
                    sum(1 for cover in cloud_covers if cover < 20)
                    if cloud_covers
                    else 0
                ),
                "hours_overcast": (
                    sum(1 for cover in cloud_covers if cover > 80)
                    if cloud_covers
                    else 0
                ),
            },
        }
    ]

    # Generate critical notifications
    notifications = []

    # Temperature warnings
    if max(temperatures) > 35:
        notifications.append(
            {
                "date": date_str,
                "notification": "HEAT ALERT: Temperature exceeds 35°C today",
            }
        )
    if max(temperatures) - min(temperatures) > 15:
        notifications.append(
            {
                "date": date_str,
                "notification": f"FLUCTUATION ALERT: Large temperature fluctuation of {max(temperatures) - min(temperatures):.1f}°C",
            }
        )

    # Wind warnings
    if wind_speeds and max(wind_speeds) > 10:
        notifications.append(
            {
                "date": date_str,
                "notification": f"HIGH WIND ALERT: Wind speeds reach {max(wind_speeds):.1f} km/h",
            }
        )

    return stats, notifications
