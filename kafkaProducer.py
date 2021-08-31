from kafka import KafkaProducer
from json import loads, dumps
import uuid
import pandas as pd
from faker import Faker
import numpy as np
import time
fake = Faker('en_US')


myuuid = str(uuid.uuid4())
topic = 'Posting'
server = 'localhost:9092'
review_df = pd.read_csv('./sample_data/review.csv')
user_df = pd.read_csv('./sample_data/user.csv')
NUM_OF_ITEM_TO_PRODUCE = 10000
PRODUCTION_FREQUENCY_IN_MILLIS = 100

print(f'''
          ============================================================================
          |                        Configuration [Producer]                                     |       
          ============================================================================
          | Group id                 {myuuid}                                       
          | Topic                    {topic}                                        
          | Server                   {server}                                       
          | Production freq          {PRODUCTION_FREQUENCY_IN_MILLIS} millis        
          | Num of items             {NUM_OF_ITEM_TO_PRODUCE}                       
          ============================================================================
     ''')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

def generate_fake_data():
     item_to_pick = np.random.randint(0, review_df.shape[0])
     user_to_pick = np.random.randint(0, user_df.shape[0])

     review = {
         'user': user_df['user'].iloc[user_to_pick],
         'text': review_df['review'].iloc[item_to_pick]
     }
     return review

for e in range(NUM_OF_ITEM_TO_PRODUCE):
     fake_review = generate_fake_data()
     print(fake_review)
     producer.send(topic, value=fake_review)
     num_of_millis_to_sleep = PRODUCTION_FREQUENCY_IN_MILLIS
     time.sleep(num_of_millis_to_sleep * 10**(-3))

print('exiting...')
