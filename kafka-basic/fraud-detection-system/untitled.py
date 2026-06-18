from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

user_ids = [101, 102, 103, 104, 105]

print("Starting Transaction Producer...")

for i in range(100):
    tx_data = {
        "tx_id": i,
        "userId": random.choice(user_ids),
        "amount": random.uniform(10.0, 15000.0), # >10000 triggers fraud
        "timestamp": time.time()
    }
    producer.send('fraud-detection1', value=tx_data)
    print(f"Sent: {tx_data}")
    time.sleep(2)
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()