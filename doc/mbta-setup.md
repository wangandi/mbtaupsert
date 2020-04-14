# Setting up the MBTA Stream

## Notes

The Boston public transit system goes to sleep at around midnight and generally does not wake up until 5-6 am, so there may be no data in the stream when the system has closed for the night. 

## One time setup

1. Get an API key from https://api-v3.mbta.com/ . This will enable you to send 1000 requests per minute to MBTA website.
2. Download the metadata about the streams from https://www.mbta.com/developers/gtfs .

## Setting up a single stream in Materialize

1. Open connection to start writing the stream to a file like this:

  ```
  curl -sN -H "accept: text/event-stream" -H "x-api-key:INSERT_API_KEY_HERE" \
    "https://api-v3.mbta.com/INSERT_DESIRED_API" > name-of-file.log
  ```

  List of the live streams is here: https://api-v3.mbta.com/docs/swagger/index.html

  Example:

  The following command opens a connection to a stream of predicted arrival and departure times
  of Red Line trains allow all Red Line stops:

  ```
  curl -sN -H "accept: text/event-stream" -H "x-api-key:INSERT_API_KEY_HERE" \
    "https://api-v3.mbta.com/predictions/?filter\\[route\\]=Red" > red-stream.log
  ```

2. Convert the file into a key-value kafka topic like so: 

  ```
  cargo run -- -f path_to_stream_file.log -t topic_name
  ``` 

  You can use `kafka-console-consumer` to verify the correctness of your stream like this:

  ```
  kafka-console-consumer --bootstrap-server localhost:9092 --topic topic_name \
     --property print.key=true --from-beginning
  ```

  The code does not auto-delete old kafka topics. To delete a kafka topic, enter:

  ```
  kafka-topics --zookeeper localhost:2181 --delete --topic topic_name
  ```

3. Create the views corresponding to the kafka topics.

  Check out the reference for creating a view to parse each type of stream [here](/doc/mbta-reference.md). 
