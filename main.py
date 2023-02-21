
from confluent_kafka import Producer,KafkaException
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro
from faker import Faker
import json
import time
import logging
import random


from utils import read_ccloud_config


# LOGGING
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


prod = Producer(read_ccloud_config("client.properties"))

#Produce messages
def produce_messages(iter):
    fake=Faker()
    for i in range(iter):
        print("Iterando")
        data={
           'user_id': fake.random_int(min=20000, max=100000),
           'user_name':fake.name(),
           'user_address':fake.street_address() + ' | ' + fake.city() + ' | ' + fake.country_code(),
           'platform': random.choice(['Mobile', 'Laptop', 'Tablet']),
           'signup_at': str(fake.date_time_this_month())    
           }
        m=json.dumps(data)
        prod.poll(1)
        prod.produce('dlq', m.encode('utf-8'),callback=receipt)
        prod.flush()
        time.sleep(3)


#Produce messages
def produce_messages_dlq(iter):
    fake=Faker()
    for i in range(iter):
        print("Iterando")
        data={
           'us': fake.random_int(min=20000, max=100000),
           'us':fake.name(),
           'abc':fake.street_address() + ' | ' + fake.city() + ' | ' + fake.country_code(),
           'def': random.choice(['Mobile', 'Laptop', 'Tablet']),
           'signup_at': str(fake.date_time_this_month())    
           }
        m=json.dumps(data)
        prod.poll(1)
        prod.produce('dlq', m.encode('utf-8'),callback=receipt)
        prod.flush()
        time.sleep(3)

#Produce Avro Messages
def produce_messages_avro():
    print("Sending Schema")
    key_schema,value_schema = load_avro_schema()
    try:
        producer = AvroProducer(read_ccloud_config("client.properties"), default_key_schema=key_schema, default_value_schema=value_schema)

        f1 = open("avro/user-metadata.json", "r")
        key_str = f1.read();
        f1.close()

        f2 = open("avro/user-details.json", "r")
        value_str = f2.read()
        f2.close()

        producer.produce(topic = "dlq", key = json.loads(key_str), headers = [("my-header1", "Value1")], value = json.loads(value_str))
        producer.flush()

    except KafkaException as e:
            print('Kafka failure ' + e)

#Load Schema from file
def load_avro_schema():
    key_schema=avro.load("")
    value_schema=avro.load("avro/user_data.avsc")

    return key_schema,value_schema

#Reveive acknowledment
def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)


#Put main in here
if __name__=="__main__":
    print("Working")
    produce_messages(10)
    #produce_messages_dlq(10)
    