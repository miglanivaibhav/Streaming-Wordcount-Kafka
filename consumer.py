from confluent_kafka.avro import AvroConsumer
import psycopg2
import os

conn = psycopg2.connect(
    host=os.environ['DB_HOST'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    dbname=os.environ['DB_NAME']
)
cursor = conn.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS word_count (word TEXT PRIMARY KEY, count INTEGER);")
conn.commit()

consumer = AvroConsumer({
    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
    'group.id': 'wordcount-group',
    'auto.offset.reset': 'earliest',
    'schema.registry.url': os.environ['SCHEMA_REGISTRY_URL']
})
consumer.subscribe(['wordcount'])

while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        word = msg.value()['word']
        count = msg.value()['count']
        cursor.execute("""
            INSERT INTO word_count(word, count)
            VALUES (%s, %s)
            ON CONFLICT(word)
            DO UPDATE SET count = word_count.count + EXCLUDED.count
        """, (word, count))
        conn.commit()
        print(f"Consumed: {word}")
    except Exception as e:
        print(f"Error: {e}")