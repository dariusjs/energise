use chrono::DateTime;
use chrono::FixedOffset;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use influx_db_client::{points, Point, Points, Precision, Value};
use serde::{Deserialize, Serialize};
use serialport::SerialPortSettings;
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::ErrorKind;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

const BAUD_RATE: u32 = 115_200;
const TIMEOUT: u64 = 1000;

#[derive(Debug)]
pub struct DsmrClient {
    pub serial_device: String,
    pub influx_db: influx_db_client::Client,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct UsageData {
    #[serde(rename(deserialize = "0-0:1.0.0"))]
    electricity_timestamp: Reading,
    #[serde(rename(deserialize = "1-0:1.7.0"))]
    power_receiving: Reading,
    #[serde(rename(deserialize = "1-0:2.7.0"))]
    power_returning: Reading,
    #[serde(rename(deserialize = "1-0:2.8.1"))]
    electricity_returned_reading_low_tariff: Reading,
    #[serde(rename(deserialize = "1-0:2.8.2"))]
    electricity_returned_reading_normal_tariff: Reading,
    #[serde(rename(deserialize = "1-0:1.8.1"))]
    electricity_reading_low_tariff: Reading,
    #[serde(rename(deserialize = "1-0:1.8.2"))]
    electricity_reading_normal_tariff: Reading,
    #[serde(rename(deserialize = "0-1:24.2.1"))]
    gas_reading: Reading,
    gas_timestamp: Reading,
    #[serde(rename(deserialize = "1-0:32.7.0"))]
    voltage: Reading,
    #[serde(rename(deserialize = "1-0:31.7.0"))]
    current: Reading,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
enum Reading {
    Measurement(Measurement),
    Timestamp(Timestamp),
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct Measurement {
    value: f64,
    unit: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct Timestamp {
    timestamp: chrono::DateTime<FixedOffset>,
}

impl DsmrClient {
    pub async fn send_to_influxdb(self) {
        let (sender, receiver): (Sender<UsageData>, Receiver<UsageData>) = mpsc::channel();
        let mut settings: SerialPortSettings = Default::default();
        settings.timeout = Duration::from_millis(TIMEOUT);
        settings.baud_rate = BAUD_RATE;
        let port = serialport::open_with_settings(&self.serial_device, &settings).unwrap();
        println!(
            "Receiving data on {} at {} baud:",
            &self.serial_device, &settings.baud_rate
        );
        //
        //  raspberrypi energise[6162]: thread '<unnamed>' panicked at 'called `Result::unwrap()` on an `Err` value: Custom { kind: InvalidData, error: "stream did not contain valid UTF-8" }', src/dsmrlib/dsmrlib.rs:75:64
        //
        let data_iter = BufReader::new(port).lines().map(|lines| lines.unwrap());
        let data_thread = thread::spawn(|| get_meter_data(Box::new(data_iter), sender));
        loop {
            let data = receiver.recv();
            match data {
                Ok(data) => {
                    self.influx_db
                        .write_points(
                            usage_to_points(&data).unwrap(),
                            Some(Precision::Seconds),
                            None,
                        )
                        .await
                        .unwrap();
                }
                Err(_) => continue,
            }
            data_thread.thread().unpark();
        }
    }
}

fn get_meter_data(
    mut lines_iter: Box<dyn Iterator<Item = String>>,
    sender: Sender<UsageData>,
) -> Result<(), ErrorKind> {
    println!("Reading meter data");
    loop {
        let message: Vec<std::string::String> = lines_iter
            .by_ref()
            .skip_while(|l| !l.starts_with('/'))
            .take_while(|l| !l.starts_with('!'))
            .collect();
        let result = deserialise_p1_message(message);
        sender
            .send(result.unwrap())
            .map_err(|_| ErrorKind::BrokenPipe)?;
        thread::park();
    }
}

pub fn deserialise_p1_message(message: Vec<std::string::String>) -> Result<UsageData, ErrorKind> {
    let mut hash: HashMap<String, Reading> = HashMap::new();
    for item in message.iter() {
        let a = item.replace(")", "");
        let x: Vec<&str> = a.split('(').collect();
        if x.len() > 1 {
            // Timestamps have a different format than the rest of P1 the records so we need to catch it and parse it first
            if x[0].to_string() == "0-0:1.0.0" {
                let timestamp = parse_date(x[1], "%y%m%d%H%M%S");
                // println!("timestamp {:?}", timestamp);
                hash.insert(
                    x[0].to_string(),
                    Reading::Timestamp(Timestamp {
                        timestamp: timestamp.unwrap(),
                    }),
                );
            }
            // Gas is an exception because it posts two values of timestamp and reading instead of just a reading
            if x[0].to_string() == "0-1:24.2.1" {
                let timestamp = parse_date(x[1], "%y%m%d%H%M%S");
                hash.insert(
                    "gas_timestamp".to_string(),
                    Reading::Timestamp(Timestamp {
                        timestamp: timestamp.unwrap(),
                    }),
                );
                let gas_volume: Vec<&str> = x[2].split('*').collect();
                hash.insert(
                    x[0].to_string(),
                    Reading::Measurement(Measurement {
                        value: gas_volume[0].parse::<f64>().unwrap(),
                        unit: gas_volume[1].to_string(),
                    }),
                );
            } else {
                if x[1].to_string().contains("*") {
                    let z: Vec<&str> = x[1].split('*').collect();
                    hash.insert(
                        x[0].to_string(),
                        Reading::Measurement(Measurement {
                            value: z[0].parse::<f64>().unwrap(),
                            unit: z[1].to_string(),
                        }),
                    );
                }
            }
        }
    }
    let v = serde_json::to_value(&hash).unwrap();
    let deserialised: UsageData = serde_json::from_value(v).unwrap();
    Ok(deserialised)
}

fn parse_date(date: &str, fmt: &str) -> Result<DateTime<FixedOffset>, ErrorKind> {
    let cest: FixedOffset = FixedOffset::east(2 * 3600);
    let cet: FixedOffset = FixedOffset::east(3600);
    if let Ok(naive_date) = NaiveDateTime::parse_from_str(&date[0..date.len() - 1], fmt) {
        let offset = match date.chars().last() {
            Some('W') => cet,
            Some('S') => cest,
            _ => return Err(ErrorKind::InvalidData),
        };
        let datetime = offset.from_local_datetime(&naive_date).single();
        match datetime {
            Some(d) => Ok(d),
            _ => Err(ErrorKind::InvalidInput),
        }
    } else {
        println!("Error in date parsing");
        Err(ErrorKind::InvalidData)
    }
}

fn usage_to_points(data: &UsageData) -> Result<Points, ErrorKind> {
    let electricity_reading_low_tariff = create_point(
        "dsmr",
        "electricity",
        "low_tariff",
        &data.electricity_reading_low_tariff,
        &data.electricity_timestamp,
    );
    let electricity_reading_normal_tariff = create_point(
        "dsmr",
        "electricity",
        "normal_tariff",
        &data.electricity_reading_normal_tariff,
        &data.electricity_timestamp,
    );
    let electricity_returned_reading_low_tariff = create_point(
        "dsmr",
        "electricity",
        "returned_reading_low_tariff",
        &data.electricity_returned_reading_low_tariff,
        &data.electricity_timestamp,
    );
    let electricity_returned_reading_normal_tariff = create_point(
        "dsmr",
        "electricity",
        "returned_reading_normal_tariff",
        &data.electricity_returned_reading_normal_tariff,
        &data.electricity_timestamp,
    );
    let power_receiving = create_point(
        "dsmr",
        "electricity",
        "receiving",
        &data.power_receiving,
        &data.electricity_timestamp,
    );
    let power_returning = create_point(
        "dsmr",
        "electricity",
        "returning",
        &data.power_returning,
        &data.electricity_timestamp,
    );
    let gas_reading = create_point(
        "dsmr",
        "gas",
        "receiving",
        &data.gas_reading,
        &data.gas_timestamp,
    );
    let voltage = create_point(
        "dsmr",
        "electricity",
        "voltage",
        &data.voltage,
        &data.electricity_timestamp,
    );
    let current = create_point(
        "dsmr",
        "electricity",
        "current",
        &data.current,
        &data.electricity_timestamp,
    );
    let points = points!(
        electricity_reading_low_tariff,
        electricity_reading_normal_tariff,
        electricity_returned_reading_low_tariff,
        electricity_returned_reading_normal_tariff,
        power_receiving,
        power_returning,
        gas_reading,
        voltage,
        current
    );
    Ok(points)
}

fn create_point(
    name: &str,
    energy_type: &str,
    reading: &str,
    value: &Reading,
    _timestamp: &Reading,
) -> Point {
    Point::new(name)
        .add_tag("energy_type", Value::String(energy_type.to_string()))
        .add_tag("reading", Value::String(reading.to_string()))
        // // influx_db_client expects an i64 but we have a Datetime so this is broken for now
        // .add_timestamp(
        //     match timestamp {
        //         Reading::Timestamp(timestamp) => timestamp.timestamp,
        //         _ => "asdad",
        //     }
        // )
        .add_field(
            "value",
            Value::Float(match value {
                Reading::Measurement(value) => value.value,
                _ => 0.0,
            }),
        )
        .add_tag(
            "unit",
            Value::String(match value {
                Reading::Measurement(value) => value.unit.to_string(),
                _ => "Nothing".to_string(),
            }),
        )
}
