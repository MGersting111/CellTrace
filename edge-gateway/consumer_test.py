import time
from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField

TOPIC = "mes.quality"
registry = SchemaRegistryClient({"url": "http://localhost:8081"})
deserializer = AvroDeserializer(registry)
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "debug-quality",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])


deadline = time.time() + 30
while time.time() < deadline:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(msg.error())
        continue
    print(deserializer(msg.value(), SerializationContext(TOPIC, MessageField.VALUE)))
consumer.close()

print(deserializer(msg.value(), SerializationContext(TOPIC, MessageField.VALUE)))
consumer.close()
