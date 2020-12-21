use serialport::SerialPortSettings;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::ErrorKind;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone};
use influx_db_client::{points, Point, Points, Precision, Value};

const ELECTRICITY_READING_LOW_IDENT: &str = "1-0:1.8.1";
const ELECTRICITY_READING_NORMAL_IDENT: &str = "1-0:1.8.2";
const ELECTRICITY_READING_RETURNED_LOW: &str = "1-0:2.8.1";
const ELECTRICITY_READING_RETURNED_NORMAL: &str = "1-0:2.8.2";
const ELECTRICITY_TIMESTAMP: &str = "0-0:1.0.0";
const ELECTRICITY_POWER_DELIVERED: &str = "1-0:1.7.0";
const ELECTRICITY_POWER_RECEIVED: &str = "1-0:2.7.0";
const GAS_READING: &str = "0-1:24.2.1";
const DATE_FORMAT: &str = "%y%m%d%H%M%S";
const HOUR: i32 = 3600;

#[derive(Debug)]
pub struct DsmrClient {
    pub serial_device: String,
    pub influx_db: influx_db_client::Client,
}

#[derive(Debug)]
struct UsageData {
    electricity_timestamp: DateTime<FixedOffset>,
    power_receiving: Measurement,
    power_returning: Measurement,
    electricity_returned_reading_low_tariff: Measurement,
    electricity_returned_reading_normal_tariff: Measurement,
    electricity_reading_low_tariff: Measurement,
    electricity_reading_normal_tariff: Measurement,
    gas_reading: Measurement,
    gas_timestamp: DateTime<FixedOffset>,
}

#[derive(Debug, PartialEq)]
struct Measurement {
    value: f64,
    unit: String,
}

const BAUD_RATE: u32 = 115_200;
const TIMEOUT: u64 = 1000;

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
        let data_iter = BufReader::new(port).lines().map(|l| l.unwrap());
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

fn usage_to_points(data: &UsageData) -> Result<Points, ErrorKind> {
    println!(
        "Received message with timestamp {}",
        data.electricity_timestamp
    );
    let electricity_reading_low_tariff = create_point(
        "dsmr",
        "electricity",
        "low_tariff",
        &data.electricity_reading_low_tariff,
        data.electricity_timestamp,
    );
    let electricity_reading_normal_tariff = create_point(
        "dsmr",
        "electricity",
        "normal_tariff",
        &data.electricity_reading_normal_tariff,
        data.electricity_timestamp,
    );
    let electricity_returned_reading_low_tariff = create_point(
        "dsmr",
        "electricity",
        "returned_reading_low_tariff",
        &data.electricity_returned_reading_low_tariff,
        data.electricity_timestamp,
    );
    let electricity_returned_reading_normal_tariff = create_point(
        "dsmr",
        "electricity",
        "returned_reading_normal_tariff",
        &data.electricity_returned_reading_normal_tariff,
        data.electricity_timestamp,
    );
    let power_receiving = create_point(
        "dsmr",
        "electricity",
        "receiving",
        &data.power_receiving,
        data.electricity_timestamp,
    );
    let power_returning = create_point(
        "dsmr",
        "electricity",
        "returning",
        &data.power_returning,
        data.electricity_timestamp,
    );
    let gas_reading = create_point(
        "dsmr",
        "gas",
        "receiving",
        &data.gas_reading,
        data.gas_timestamp,
    );
    let points = points!(
        electricity_reading_low_tariff,
        electricity_reading_normal_tariff,
        electricity_returned_reading_low_tariff,
        electricity_returned_reading_normal_tariff,
        power_receiving,
        power_returning,
        gas_reading
    );
    Ok(points)
}

fn create_point(
    name: &str,
    energy_type: &str,
    reading: &str,
    value: &Measurement,
    timestamp: DateTime<FixedOffset>,
) -> Point {
    Point::new(name)
        .add_tag("energy_type", Value::String(energy_type.to_string()))
        .add_tag("reading", Value::String(reading.to_string()))
        .add_timestamp(timestamp.timestamp())
        .add_field("value", Value::Float(value.value))
        .add_tag("unit", Value::String(value.unit.clone()))
}

fn get_meter_data(
    mut lines_iter: Box<dyn Iterator<Item = String>>,
    sender: Sender<UsageData>,
) -> Result<(), ErrorKind> {
    // println!("Reading meter data");
    loop {
        let message = lines_iter
            .by_ref()
            .skip_while(|l| !l.starts_with('/'))
            .take_while(|l| !l.starts_with('!'))
            .collect();
        // println!("Got meter data {:?}", message);
        let result = parse_message(message)?;
        sender.send(result).map_err(|_| ErrorKind::BrokenPipe)?;
        // println!("Sent result, parking thread");
        thread::park();
    }
}

fn parse_message(message: Vec<String>) -> Result<UsageData, ErrorKind> {
    let electricity_timestamp =
        parse_date(find_message(&message, ELECTRICITY_TIMESTAMP)?, DATE_FORMAT)?;
    let electricity_reading_low_tariff =
        parse_measurement(find_message(&message, ELECTRICITY_READING_LOW_IDENT)?)?;
    let electricity_reading_normal_tariff =
        parse_measurement(find_message(&message, ELECTRICITY_READING_NORMAL_IDENT)?)?;
    let electricity_returned_reading_low_tariff =
        parse_measurement(find_message(&message, ELECTRICITY_READING_RETURNED_LOW)?)?;
    let electricity_returned_reading_normal_tariff =
        parse_measurement(find_message(&message, ELECTRICITY_READING_RETURNED_NORMAL)?)?;
    let power_receiving = parse_measurement(find_message(&message, ELECTRICITY_POWER_DELIVERED)?)?;
    let power_returning = parse_measurement(find_message(&message, ELECTRICITY_POWER_RECEIVED)?)?;
    let gas = find_message(&message, GAS_READING)?;
    let (gas_reading, gas_timestamp) = split_gas(gas)?;
    let result = UsageData {
        electricity_timestamp,
        power_receiving,
        power_returning,
        electricity_reading_low_tariff,
        electricity_reading_normal_tariff,
        electricity_returned_reading_normal_tariff,
        electricity_returned_reading_low_tariff,
        gas_reading,
        gas_timestamp,
    };
    Ok(result)
}

fn parse_measurement(value: &str) -> Result<Measurement, ErrorKind> {
    let deliminator = value.find('*').ok_or(ErrorKind::InvalidData)?;
    Ok(Measurement {
        value: value[0..deliminator]
            .parse::<f64>()
            .map_err(|_| ErrorKind::InvalidData)?,
        unit: value[deliminator + 1..value.len()].to_string(),
    })
}

fn parse_date(date: &str, fmt: &str) -> Result<DateTime<FixedOffset>, ErrorKind> {
    let cest: FixedOffset = FixedOffset::east(2 * HOUR);
    let cet: FixedOffset = FixedOffset::east(HOUR);
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

fn split_gas(gas: &str) -> Result<(Measurement, DateTime<FixedOffset>), ErrorKind> {
    let gas_offset = gas.find(')').ok_or(ErrorKind::InvalidData)?;
    let gas_timestamp = parse_date(&gas[0..gas_offset], DATE_FORMAT)?;
    let gas_reading = parse_measurement(&gas[gas_offset + 2..gas.len()])?;
    Ok((gas_reading, gas_timestamp))
}

fn find_message<'a>(message: &'a [String], ident: &str) -> Result<&'a str, ErrorKind> {
    let mut message_iter = message.iter();
    match message_iter.find(|m| m.starts_with(ident)) {
        Some(s) => {
            if let Some(offset) = &s.find('(') {
                Ok(&s[offset + 1..s.len() - 1])
            } else {
                Err(ErrorKind::InvalidData)
            }
        }
        None => Err(ErrorKind::InvalidData),
    }
}
