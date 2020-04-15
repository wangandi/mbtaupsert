# Overview

In this demo, we use live MBTA data to recreate the experience of going from stop A to stop B
via the MBTA subway system.

## Setup

It is assumed that base streams have been set up according to [mbta-setup.md](/doc/mbta-setup.md) and base views have already been created in Materialize according to [mbta-reference.md](/doc/mbta-reference.md).

## One-line travel

Suppose you were at Kendall/MIT, which is on the Red Line and you want to go to South Station, which is also on the Red Line.

So you load up the base view `parsed_red_pred` for Red Line predictions. The first thing you'd notice is that the view only has `stop_id`s and not stop names. Likewise, there's only `direction_id` and not the name of the direction.

You could memorize which `stop_id`s correspond to which stop, but it would probably be easier to join the Red Line predictions with some metadata that way you can see the stop names and direction names in the view directly. 
```
CREATE MATERIALIZED VIEW enriched_red_pred AS 
SELECT id, arrival_time, departure_time, direction, schedule_relationship,
       status, stop_sequence, stop_name, trip_id, vehicle_id
FROM parsed_red_pred r, mbta_stops s, mbta_directions d
WHERE r.stop_id = s.stop_id
  AND r.route_id = d.route_id
  AND r.direction_id = d.direction_id;
```

### Upcoming Train Display

Normally, if you were at Kendall/MIT waiting at the southbound platform, you would see a digital display of when the next several trains are coming. To get when the next trains are coming, enter this query:

```
SELECT status, arrival_time, departure_time, trip_id
FROM enriched_red_pred
WHERE stop_name='Kendall/MIT' and direction='South'
ORDER BY departure_time asc; 
```

Provided your stream is live and nothing has happened to your connection, the first row you see should be the next train departing from Kendall/MIT because the MBTA stream sends deletion requests for predictions as soon as the event being predicted has passed.

If you repeatedly call this query, you may notice the arrival time and departure time for a particular trip id change by a few seconds. Over the lifetime of a prediction, the MBTA stream typically sends a dozen to hundreds of updates to the prediction.

If you call this query again after the first train has left, you'll see that the next train is in the first row.

What this query returns is a bit off from what would be displayed at an actual station. For one, it's missing the headsign, a.k.a. the final destination of the trip. It doesn't matter too much when your destination is South Station, but it would matter if your destination were south of JFK/UMass because the Red Line splits into two forks from there. Another thing it is missing is the status 'Boarding' when the train is in the station.

Let's replace `enriched_red_pred` with a version that also joins to 1) a stream of MBTA Red Line trips that way we can see the headsign for each trip and 2) a stream of vehicle positions that way we can see when a train is in the station.

```
CREATE OR REPLACE MATERIALIZED VIEW enriched_red_pred AS 
SELECT r.id, arrival_time, departure_time, direction, headsign, r.route_id, schedule_relationship,
       r.status, stop_sequence, r.stop_id, stop_name, r.trip_id, v.status as vehicle_status,
       v.stop_id as vehicle_stop_id
FROM parsed_red_pred r, mbta_stops s, mbta_directions d, parsed_red_trips t, parsed_vehicles v
WHERE r.stop_id = s.stop_id
  AND r.route_id = d.route_id
  AND r.trip_id = t.id
  AND r.direction_id = d.direction_id
  AND r.vehicle_id = v.id;
``` 

From there it's a few SQL row transformations to get a query whose results match exactly what would be displayed at a station and conform to the [MBTA countdown display standard](https://www.mbta.com/developers/v3-api/best-practices).

```
SELECT 
  headsign, 
  CASE WHEN raw_status IS NOT NULL THEN raw_status ELSE 
    CASE WHEN vehicle_status = 'STOPPED_AT'
      AND stop_id=vehicle_stop_id
      AND seconds_away <= 90
      THEN 'Boarding' ELSE
    CASE WHEN seconds_away <= 30 THEN 'Arriving' ELSE
    CASE WHEN seconds_away <= 60 THEN 'Approaching' ELSE
    CASE WHEN seconds_away <=89 THEN '1 minute' ELSE
    round(CAST(seconds_away AS FLOAT)/60) || ' minutes' END END END END END status,
  seconds_away
FROM (
  SELECT status as raw_status,
    ROUND(
      EXTRACT(
        EPOCH FROM 
        CASE WHEN arrival_time IS NOT NULL THEN arrival_time ELSE departure_time END
          + INTERVAL '4' HOUR)
      - EXTRACT(EPOCH FROM now())) seconds_away,
    headsign,
    vehicle_status,
    stop_id,
    vehicle_stop_id
  FROM enriched_red_pred
  WHERE stop_name='Kendall/MIT' and direction='South')
ORDER BY seconds_away asc;
```

### Travel prediction

To predict when you will arrive at South Station if you take the next train from Kendall/MIT, you can do join `enriched_red_pred` with a copy of itself like this:

```
SELECT r1.status, r1.departure_time, r2.arrival_time, r1.trip_id 
FROM enriched_red_pred r1, enriched_red_pred r2
WHERE r1.trip_id = r2.trip_id
  AND r1.stop_name='Kendall/MIT' and r2.stop_name='South Station'
  AND r1.stop_sequence < r2.stop_sequence
ORDER BY r2.arrival_time; 
```

The MBTA data guarantees that, if two predictions `x` and `y` have the `trip_id` but `x.stop_sequence < y.stop_sequence`, that the particular trip involves the vehicle stopping at `x` before it arrives at `y`. So the clause `r1.stop_sequence < r2.stop_sequence` is a convenient way of filtering for only trips from Kendall/MIT to South Station as opposed to the other way around. 

You can confirm the correctness of your live results by cross-checking with Google Maps if you like. 

## Travel involving transfers

Suppose that instead of wanting to go South Station from Kendall/MIT, you wanted to go to North Station instead. Besides being not being on the Red Line, there are two ways you can go to North Station via subway: transfering to the Green Line via Park St or transfering to the Orange Line via Downtown Crossing.

What should you do to figure out when you would get to North Station? First you will need two copies of the query to calculate travel times between two stops on the same line: one for the trip form Kendall/MIT to the transfer stop, and one for the trip from the transfer stop to the North Station. When you join the two queries, you want to make sure
 * the transfer stop in both queries in the same.
 * there is enough time between the arrival time of the first trip and the departure time of the second trip to transfer. Let's assume that transfer time is two minutes. 

Based on these requirements, let's create `enriched_og_pred`. Create the kafka topics for predictions for the Orange, Green-C, and Green-E lines. (Green-B and Green-D don't go to North Station) Create sources in materialize for those kafka topics and create a view that is the union of those sources. Then create a view that parses the json.  

Once you have `parsed_og_pred`, you can add stop names by joining it to the list of stops.

```
CREATE MATERIALIZED VIEW enriched_og_pred AS 
SELECT id, arrival_time, departure_time, direction_id, headsign, route_id,
       schedule_relationship, status, stop_sequence, stop_name, trip_id, vehicle_id
FROM parsed_og_pred og, mbta_stops s 
WHERE og.stop_id = s.stop_id;
```

Then you can calculate travel time using this join query using two copies of `enriched_red_pred` and two copies of `enriched_og_pred`. 

```
SELECT
departure_time, min(arrival_time) as arrival_time FROM
(SELECT
max(r1.departure_time) as departure_time, og2.arrival_time 
FROM enriched_red_pred r1, enriched_red_pred r2, enriched_og_pred og1, enriched_og_pred og2
WHERE r1.trip_id = r2.trip_id 
AND r1.stop_name='Kendall/MIT'
AND r2.stop_name=og1.stop_name
AND r1.stop_sequence < r2.stop_sequence 
AND og2.stop_name='North Station'
AND og1.stop_sequence < og2.stop_sequence 
AND og1.trip_id = og2.trip_id 
AND (r2.arrival_time + INTERVAL '2' MINUTE) < og1.departure_time
group by og2.arrival_time)
group by departure_time
order by arrival_time;
```

The reason the query has the two aggregations is because there are multiple combinations of routes that would result in arriving in North Station at the same time. For example, it could be that no Orange Line train left from Downtown Crossing until after two Red Line trains arrived. Likewise, it is possible for multiple arrival times to correspond to a single departure time, and in that case, we only care about the earliest arrival time corresponding to a particular departure time. 

#TODO: add information about where to change lines to the above query.

Once again, you can check the results with google maps.

## Further things to try

Try setting up and creating a query that would predict the travel time between two arbitrary MBTA subway stops.
