from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from typing import List


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
