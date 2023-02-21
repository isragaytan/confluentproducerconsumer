


from confluent_kafka import Producer
from faker import Faker
import json
import time
import logging
import random


from utils import read_ccloud_config


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
    