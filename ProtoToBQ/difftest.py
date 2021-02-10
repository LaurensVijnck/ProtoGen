from jsondiff import diff
import json

first = {".lvi.Event": {
    "package": "lvi",
    "filename": "event",
    "ambiguous_file_name": True,
    "name": "Event",
    "table_root": True,
    "batch_table": True,
    "cluster_fields": [
      "tenant_id"
    ],
    "time_partitioning": True,
    "partitioning_expiration": 0,
    "partition_field": "event_time",
    "table_name": "event_table",
    "table_description": "ProtoToBQ generated table for events",
    "fields": [
      {
        "index": 1,
        "name": "client",
        "alias": None,
        "description": "Owner of the event",
        "type": "TYPE_MESSAGE",
        "type_value": ".lvi.Client",
        "required": False,
        "batch_field": False,
        "clustering_field": False,
        "partitioning_field": False,
        "timestamp": False,
        "optional_field": False,
        "repeated_field": False,
        "default_value": None
      },
      {
        "index": 2,
        "name": "events",
        "alias": None,
        "description": "",
        "type": "TYPE_MESSAGE",
        "type_value": ".lvi.BatchEvent",
        "required": True,
        "batch_field": True,
        "clustering_field": False,
        "partitioning_field": False,
        "timestamp": False,
        "optional_field": False,
        "repeated_field": True,
        "default_value": None
      },
    ]
  }
}

second = {".lvi.Event": {
    "package": "lvi",
    "filename": "event",
    "ambiguous_file_name": True,
    "name": "Event",
    "table_root": True,
    "batch_table": True,
    "cluster_fields": [
      "tenant_id"
    ],
    "time_partitioning": True,
    "partitioning_expiration": 0,
    "partition_field": "event_time",
    "table_name": "event_table",
    "table_description": "ProtoToBQ generated table for events",
    "fields": [
      {
        "index": 1,
        "name": "client",
        "alias": None,
        "description": "Owner of the event",
        "type": "TYPE_MESSAGE",
        "type_value": ".lvi.Client",
        "required": False,
        "batch_field": False,
        "clustering_field": False,
        "partitioning_field": False,
        "timestamp": False,
        "optional_field": False,
        "repeated_field": False,
        "default_value": None
      },
      {
        "index": 2,
        "name": "events",
        "alias": None,
        "description": "",
        "type": "TYPE_MESSAGE",
        "type_value": ".lvi.BatchEvent",
        "required": False,
        "batch_field": True,
        "clustering_field": False,
        "partitioning_field": False,
        "timestamp": False,
        "optional_field": False,
        "repeated_field": True,
        "default_value": None
      },
      {
        "index": 3,
        "name": "events",
        "alias": None,
        "description": "",
        "type": "TYPE_MESSAGE",
        "type_value": ".lvi.BatchEvent",
        "required": True,
        "batch_field": True,
        "clustering_field": False,
        "partitioning_field": False,
        "timestamp": False,
        "optional_field": False,
        "repeated_field": True,
        "default_value": None
      },
    ]
  }
}

print(diff(first, second))