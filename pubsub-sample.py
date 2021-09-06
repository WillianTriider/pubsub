import os
import apache_beam as beam
from google.cloud import pubsub_v1

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/my_project.json'
pl = beam.Pipeline()

#-----------------------------Create Topic----------------------------------------------------

project_id = 'my_project'
topic = 'teste-api'
sub = 'teste-api-sub'

publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=project_id,
    topic=topic, 
)
publisher.create_topic(name=topic_name)
future = publisher.publish(topic_name, b'Test Message!', spam='eggs')
future.result()

#-----------------------------Read Subsctription-----------------------------------------------

topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=project_id,
    topic=topic, 
)

subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
    project_id=project_id,
    sub=sub, 
)

def callback(message):
    print(message.data)
    message.ack()

with pubsub_v1.SubscriberClient() as subscriber:
    subscriber.create_subscription(
        name=subscription_name, topic=topic_name)
    future = subscriber.subscribe(subscription_name, callback)
    future.result()