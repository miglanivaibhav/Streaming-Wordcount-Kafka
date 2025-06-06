from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.avro import loads
import requests
import os
import time
import nltk
from nltk.corpus import stopwords

nltk.download("stopwords")
SCHEMA_STR = open("wordcount.avsc", "r").read()
value_schema = loads(SCHEMA_STR)
api_url = "https://api.api-ninjas.com/v1/quotes"

STOP_WORDS = set(stopwords.words("english"))


producer = AvroProducer(
    {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'schema.registry.url': os.getenv("SCHEMA_REGISTRY_URL")
},
default_value_schema=value_schema
)

topic = "wordcount"

while True:
    res = requests.get(api_url, headers={'X-Api-Key': 'CPQooTGC6cKlF9uRwc1+ug==Jr7RcJY1GPgTBKKm'})
    if res.status_code == 200:
        text = res.json()[0]["quote"]
        for word in text.strip().split():
            clean_word = ''.join(filter(str.isalpha, word)).lower()
            if clean_word and clean_word not in STOP_WORDS:
                producer.produce(topic=topic, value={"word":clean_word, "count": 1})
                print(f"Produced: {clean_word}")
    time.sleep(1)