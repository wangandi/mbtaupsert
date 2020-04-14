# Overview

In this demo, we use live MBTA data to recreate the experience of going from stop A to stop B
via the MBTA subway system.

## Setup

It is assumed that base streams have been set up according to [mbta-setup.md](/doc/mbta-setup.md) and base views have already been created in Materialize according to [mbta-reference.md](/doc/mbta-reference.md).

## One-line travel

Suppose you were at Kendall/MIT, which is on the Red Line and you want to go to South Station, which is also on the Red Line.

So you load up the base view `parsed_red_live` for Red Line predictions. The first thing you'd notice is that the view only has `stop_id`s and not stop names. Likewise, there's only `direction_id` and not the name of the direction.

You could memorize which `stop_id`s correspond to which stop, but it would probably be easier to join the Red Line predictions with some metadata that way you can see the stop names and direction names in the view directly. 
```
CREATE MATERIALIZED VIEW enriched_red_live AS 
SELECT id, arrival_time, departure_time, direction, schedule_relationship, status, stop_sequence, stop_name, trip_id, vehicle_id FROM parsed_red_pred r, mbta_stops s, mbta_directions d WHERE r.stop_id = s.stop_id AND r.route_id = d.route_id AND r.direction_id = d.direction_id;
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

What this query returns is a bit off from what would be displayed at an actual station. For one, it's missing the final destination of the trip.
 


To match exactly what would be displayed at a station and conform to the [MBTA countdown display standard](https://www.mbta.com/developers/v3-api/best-practices), we just need some SQL row transformations.



### Travel prediction


To predict when you will arrive at South Station if you take the next train from Kendall/MIT, you can do join `enriched_red_pred` with itself like this:

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

## Travel involve transfers

## Challenge analysis
