from confluent_kafka import Producer

# Define your Kafka producer configuration
conf = {
    "bootstrap.servers": "localhost:9092",
    # Add more configurations as needed
}

# Create a Kafka producer instance
producer = Producer(conf)

# Define the Kafka topic
kafka_topic = "test-topic-python1"

# List of messages to produce
list_messages = ["hi", "this is a test", "help me", "to be the best", "i need help", "thanks"]

# Produce the messages to the Kafka topic
for msg in list_messages:
    producer.produce(kafka_topic, value=msg)

# Wait for any outstanding messages to be delivered and delivery reports received
producer.flush()


