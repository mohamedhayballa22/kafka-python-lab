from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import random
from datetime import datetime, date, timedelta
from typing import List, Dict, Any


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
