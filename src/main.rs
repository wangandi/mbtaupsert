use getopts::Options;

use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::SeekFrom;

mod kafka_interaction;

extern crate futures;
extern crate json;
extern crate rdkafka;
extern crate reqwest;

fn parse_entry(parsed_object: &mut json::JsonValue) -> (String, Option<String>) {
    assert!(parsed_object.is_object());
    parsed_object.remove("type");
    let key = parsed_object["id"].take_string().unwrap();
    parsed_object.remove("id");
    let value = if parsed_object.len() == 0 {
        None
    } else {
        Some(parsed_object.to_string())
    };
    (key, value)
}

fn parse_line(line: &str) -> Vec<(String, Option<String>)> {
    let mut results = Vec::new();
    if line.starts_with("data: ") {
        let mut parsed_json = json::parse(&line[6..]);
        if let Ok(parsed_json) = &mut parsed_json {
            if parsed_json.is_array() {
                for member in parsed_json.members_mut() {
                    results.push(parse_entry(member))
                }
            } else {
                results.push(parse_entry(parsed_json))
            }
        } else {
            println!("broken line: {}", line);
        }
    } 
    results
}

fn run_stream() -> Result<(), String> {
  let args: Vec<_> = env::args().collect();

  let mut opts = Options::new();
  opts.optopt("", "kafka-addr", "kafka bootstrap address", "HOST:PORT");
  opts.reqopt("f", "file-name", "path to file where information is being logged", "ABSOLUTE_PATH");
  opts.optflag("h", "help", "show this usage information");
  opts.optopt("t", "topic-name", "name of topic to write to (default mbta)", "STRING");
  opts.optopt("c", "consistency-name", "name of consistency topic to write to (default TOPIC_NAME-consistency topic)", "STRING");
  let usage_details = opts.usage("usage: mbta-to-compacted-kafka [options] FILE");
  let opts = opts
      .parse(&args[1..])
      .map_err(|e| format!("{}\n{}\n", usage_details, e))?;

  if opts.opt_present("h") {
    return Err(usage_details);
  }

  if !opts.opt_present("f") {
    return Err(usage_details);
  }

  let config = kafka_interaction::Config {
      kafka_addr: opts.opt_str("kafka-addr"),
  };

  let mut state = kafka_interaction::State::new(config)?;

  let topic_name = opts.opt_str("t").unwrap();//.unwrap_or("mbta".to_string());
  state.create_topic(&topic_name, 1)?;
  let consistency_name = opts.opt_str("c").unwrap_or(format!("{}-data-consistency", topic_name));
  state.create_topic(&consistency_name, 1)?;

  //read off of the stream file
  let filename = opts.opt_str("f").unwrap();
  let f = match File::open(filename.clone()) {
      Ok(x) => x,
      Err(err) => {
          return Err(err.to_string());
      }
  };

  let mut pos = 0;
  let mut reader = BufReader::new(f);
  let mut timestamp = 0;

  loop {
      let mut line = String::new();
      let resp = reader.read_line(&mut line);
      match resp {
          Ok(len) => {
              if len > 0 {
                  pos += len as u64;
                  reader.seek(SeekFrom::Start(pos)).unwrap();
                  for (key, value) in parse_line(&line) {
                    state.ingest(&topic_name, 0, Some(key), value)?;
                    state.ingest(&consistency_name, 0, None, Some(format!("{},1,0,{},{}", &topic_name, timestamp, timestamp+1)))?;
                    timestamp += 1;
                  }
                  line.clear();
              }
          }
          Err(err) => {
              println!("{}", err);
          }
      }
  }
}

fn main() {
  match run_stream() {
    Ok(()) => {},
    Err(err) => println!("{}", err.to_string()),
  }
}
