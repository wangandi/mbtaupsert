# Reference for how to create a view on data from the MBTA sets

These are a set of commands you can copy/paste to build views on MBTA sets. Note: This does not break out every field from the JSON, just fields that are potentially useful.

## Static metadata

```
CREATE MATERIALIZED SOURCE mbta_x_metadata
  FROM FILE '/path-to-file' FORMAT CSV WITH HEADER;
```

## Live prediction stream

```
CREATE MATERIALIZED SOURCE x_pred
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'x-live-pred'
  FORMAT TEXT ENVELOPE UPSERT FORMAT TEXT;
```

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

Because the predictions stream requires a filter for any results to be returned, it is likely that it is desirable to combine multiple streams into one. For example, you get predictions for the entirety of the Green Line, you need to combine the streams for Green-B, Green-C, Green-D, and Green-E. 

In that case, to minimize the number of commands you have to enter, create a view to union all the streams.
```
CREATE VIEW all-green-pred as
  SELECT * FROM green-b-pred UNION
  SELECT * FROM green-c-pred UNION
  SELECT * FROM green-d-pred UNION
  SELECT * FROM green-e-pred;
```

Then create the view `parsed-all-green-pred to parse the json.

## Live trips stream

```
CREATE MATERIALIZED SOURCE x_trips
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'x-live-trips'
  FORMAT TEXT ENVELOPE UPSERT FORMAT TEXT;
```
