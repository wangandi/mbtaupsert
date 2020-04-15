# Reference for how to create a view on data from the MBTA sets

These are a set of commands you can copy/paste to build views on MBTA sets.

## Static metadata

```
CREATE SOURCE mbta_x_metadata
  FROM FILE '/path-to-file' FORMAT CSV WITH HEADER;
```

## Live streams

First, create a source from the kafka topic.

```
CREATE SOURCE x
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'x-live'
  FORMAT TEXT ENVELOPE UPSERT FORMAT TEXT;
```

For some of the streams, such as predictions or trips, the mbta requires that you use a filter in the API, so the data you want may be separated into multiple streams, and you may want create a view to union them together. For example, if you want to have predictions for the entirety of the Green Line, you need to combine the streams for Green-B, Green-C, Green-D, and Green-E like this: 
```
CREATE VIEW all-green-pred as
  SELECT * FROM green-b-pred UNION
  SELECT * FROM green-c-pred UNION
  SELECT * FROM green-d-pred UNION
  SELECT * FROM green-e-pred;
```

Then, you build a view on top of your source or view combining multiple sources that parses the json data that has come in from the stream. For your convenience, sample json parsing view creation queries are listed below for some of the live streams. In general, you should only parse out the fields that are of interest to you.

### Predictions

```
CREATE VIEW parsed_x_pred as 
SELECT id,
CAST(payload->'attributes'->>'arrival_time' AS timestamp) arrival_time,
CAST(payload->'attributes'->>'departure_time'  AS timestamp) departure_time,
CAST(CAST(payload->'attributes'->>'direction_id' AS DECIMAL(5,1)) AS INT) direction_id,
payload->'attributes'->>'schedule_relationship' schedule_relationship,
payload->'attributes'->>'status' status,
CAST(CAST(payload->'attributes'->>'stop_sequence' AS DECIMAL(5,1)) AS INT) stop_sequence,
payload->'relationships'->'route'->'data'->>'id' route_id,
payload->'relationships'->'stop'->'data'->>'id' stop_id,
payload->'relationships'->'trip'->'data'->>'id' trip_id,
payload->'relationships'->'vehicle'->'data'->>'id' vehicle_id
FROM (SELECT key0 as id, cast ("text" as jsonb) AS payload FROM x_pred);
```

### Trips

```
CREATE VIEW parsed_x_trips as 
SELECT id,
payload->'attributes'->>'bikes_allowed' bikes_allowed,
CAST(CAST(payload->'attributes'->>'direction_id' AS DECIMAL(5,1)) AS INT) direction_id,
payload->'attributes'->>'headsign' headsign,
payload->'attributes'->>'wheelchair_accessible' wheelchair_accessible,
payload->'relationships'->'route'->'data'->>'id' route_id,
payload->'relationships'->'route_pattern'->'data'->>'id' route_pattern_id,
payload->'relationships'->'service'->'data'->>'id' service_id,
payload->'relationships'->'shape'->'data'->>'id' shape_id
FROM (SELECT key0 as id, cast ("text" as jsonb) AS payload FROM x_trips);
```

### Vehicles

```
CREATE VIEW parsed_vehicles as 
SELECT id,
payload->'attributes'->>'current_status' status,
CAST(CAST(payload->'attributes'->>'direction_id' AS DECIMAL(5,1)) AS INT) direction_id,
payload->'relationships'->'route'->'data'->>'id' route_id,
payload->'relationships'->'stop'->'data'->>'id' stop_id,
payload->'relationships'->'trip'->'data'->>'id' trip_id
FROM (SELECT key0 as id, cast ("text" as jsonb) AS payload FROM vehicles);
```
