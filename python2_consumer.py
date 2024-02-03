from confluent_kafka import Consumer, KafkaError

# Define your Kafka consumer configuration
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "my_consumer_group",
    "auto.offset.reset": "earliest",  # or "latest"
    # Add more configurations as needed
}

# Create a Kafka consumer instance
consumer = Consumer(conf)

# Subscribe to the topic
consumer.subscribe(["test-topic-python2"])

# Consume messages
try:
    while True:
        msg = consumer.poll(1.0)  # adjust the timeout as needed
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    pass
finally:
    # Close the consumer gracefully
    consumer.close()
