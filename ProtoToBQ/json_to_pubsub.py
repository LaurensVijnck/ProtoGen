import time
from google.cloud import pubsub_v1
import json

project_id = "instant-shard-200915"
topic_id = "selligent.dev.cortex.sto.updates.v1.eu.ta"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

tenant_id = "E7E0C950-F256-44E4-AE60-4CE9C8C74805"
event = {
   "cortex":{
      "version":2
   },
   "meta":{
      "tenant_id":tenant_id,
      "organization_id":""
   },
   "actor":{
      "type":"audience",
      "audience_id":"abcdefg"
   },
   "event":{
      "event_type":"SGNT_CORTEX_STO_AUDIENCE_UPDATE",
      "version":1
   },
   "data":{
      "sto_hour":10
   }
}

futures = dict()


def get_callback(f, data):
    def callback(f):
        try:
            print(f.result())
            futures.pop(data)
        except:  # noqa
            print("Please handle {} for {}.".format(f.exception(), data))

    return callback


print(json.dumps(event, indent=2))

futures.update({0: None})
# When you publish a message, the client returns a future.
data = json.dumps(event).encode()
future = publisher.publish(topic_path, data, tenant_id=tenant_id)
futures[0] = future
# Publish failures shall be handled in the callback function.
future.add_done_callback(get_callback(future, 0))

# Wait for all the publish futures to resolve before exiting.
while futures:
    time.sleep(5)

print(f"Published messages with error handler to {topic_path}.")