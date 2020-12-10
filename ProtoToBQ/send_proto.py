import time

from google.cloud import pubsub_v1
from protos import event_pb2, client_pb2, actor_pb2
from google.protobuf.json_format import MessageToJson

project_id = "geometric-ocean-284614"
topic_id = "dynamic_etl"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

futures = dict()


def get_callback(f, data):
    def callback(f):
        try:
            print(f.result())
            futures.pop(data)
        except:  # noqa
            print("Please handle {} for {}.".format(f.exception(), data))

    return callback

tags = ["hi", "there"]

# Construct event
event = event_pb2.Event()

client = client_pb2.Client()
#client.tenantId = 1337
client.name = "LVI"
event.client.MergeFrom(client)

start = 80080
for i in range(6):
    batch_event = event.events.add()

    actor = actor_pb2.Actor()
    actor.userId = start
    actor.email = "laurens@hotmail.com"

    address = actor_pb2.Address()
    address.street = "Maastrichterpoort"
    address.number = "2"
    address.country = "Belgium"
    actor.address.MergeFrom(address)
    batch_event.actor.MergeFrom(actor)
    start = start + 1

    for tag in tags:
        etag = event_pb2.Tag()
        etag.tag_code = tag
        etag.tag_namespace = "default"
        etag.tag_name = tag
        batch_event.tags.append(etag)


print(MessageToJson(event))
raw_bytes = event.SerializeToString()


for i in range(2):
    futures.update({i: None})
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, raw_bytes)
    futures[i] = future
    # Publish failures shall be handled in the callback function.
    future.add_done_callback(get_callback(future, i))

# Wait for all the publish futures to resolve before exiting.
while futures:
    time.sleep(5)

print(f"Published messages with error handler to {topic_path}.")