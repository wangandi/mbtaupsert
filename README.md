# Demo of Upsert using MBTA real-time API

## What is Upsert?

Upsert is a general convention where you send a bunch of records with a key and payload:  

```
{key: xxx, payload: {stuff:"foo"}}
```
If the key `xxx` does not already exist in the stream, the record above is treated as an insert.

If the key `xxx` already exists in the stream, the record above is treated essentially as an
```
UPDATE stream SET payload='{stuff:"foo"}' WHERE key='xxx';
```

If the payload is `null`, then the record is treated as a delete command.

## Why do we care about upsert?

For one, this is the standard format that kafka topics are in.

Supporting this format requires a paradigm shift from the typical differential-dataflow one.
Differential-dataflow expects additions to look like `(row, time, 1)` and deletions to look like `(row, time, -1)`. But in the upsert format, additions look like `((key, value), time, 1)` but deletions look like `((key, null), time, -1)`. In other words, differential-dataflow expects a deletion request to contain the full record being deleted, but upsert only provides the key of the record to be deleted. Due to not having the full record, ordinary differential operators do not work on an upsert.

## What is the MBTA real-time API?

Massachusetts Bay Transportation Authority manages public transit in the Boston area. It has a bunch of live JSON streams whose format are roughly like this:

```
event:reset
[{id: ..., type: ..., other_fields: ...},
 {id: ..., type: ..., other_fields: ...},
 {id: ..., type: ..., other_fields: ...}, ...]

event: update
{id: ..., type: ..., other_fields: ...}

event: remove
{id: ..., type: ...}
```
Which, as you can see, bears a decent resemblance to the upsert format.

## What does this code do?

This code takes a file where the MBTA live stream is being written to and converts the data inside into a Kafka stream of the key-value format that way we can try out Materialize support for topics of the key-value format. Look in [doc](/doc) for more information.

Technically, this code is not MBTA stream-specific. With a few lines of changes, it should be able to take any stream of json objects, parse out the desired key, and then produce a key-value Kafka topic out of it. 
