# Sqlite Checkpoint Store

A basic implementation of the Checkpoint interface from the Azure EventHub SDK for Python.
It can be used for quick dev testing on a single machine without the need for creating a blob storage.

## Usage

```Python
    checkpoint_store = SqliteCheckpointStore("checkpoint.db")
    client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENTHUB_NAME,
        checkpoint_store=checkpoint_store,
    )
```

I welcome comments and improvements  :)
