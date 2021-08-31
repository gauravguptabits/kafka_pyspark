from kafka import KafkaConsumer
from json import loads
import uuid
import pandas as pd

myuuid = str(uuid.uuid4())
topic = 'processedPosting'
server = '172.18.0.2:9092'

print(f'''
          ============================================================================
          |                        Configuration [Consumer]                                     |       
          ============================================================================
          | Group id                 {myuuid}
          | Topic                    {topic}                                        
          | Server                   {server}                      
          ============================================================================
     ''')
consumer = KafkaConsumer(
     topic,
     bootstrap_servers=[server],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group')


for message in consumer:
    message = message.value
    print('Message: {}'.format(message))