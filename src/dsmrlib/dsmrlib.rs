use chrono::DateTime;
use chrono::FixedOffset;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use influx_db_client::{points, Point, Points, Precision, Value};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serialport::SerialPortSettings;
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::ErrorKind;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;
extern crate env_logger;

const BAUD_RATE: u32 = 115_200;
const TIMEOUT: u64 = 1000;

#[derive(Debug)]
pub struct DsmrClient {
    pub serial_device: String,
    pub influx_db: influx_db_client::Client,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct UsageData {
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
        let port = serialport::open_with_settings(&self.serial_device, &settings);

        match port {
            Ok(p) => {
                info!(
                    "Receiving data on {} at {} baud:",
                    &self.serial_device, &settings.baud_rate
                );

                let data_iter = BufReader::new(p).lines().map(|lines| {
                    debug!("lines mapping: {:?}", lines);
                    lines.unwrap()
                });
                let data_thread = thread::spawn(|| get_meter_data(Box::new(data_iter), sender));
                loop {
                    let data = receiver.recv();
                    match data {
                        Ok(d) => {
                            self.influx_db
                                .write_points(
                                    usage_to_points(&d).unwrap(),
                                    Some(Precision::Seconds),
                                    None,
                                )
                                .await
                                .ok();
                        }
                        Err(e) => error!("Failed to write to InfluxDB: {}", e),
                    }
                    data_thread.thread().unpark();
                }
            }
            Err(e) => error!("Unable to connect to Serial Port: {}, retrying", e),
        }
    }
}

fn get_meter_data(
    mut lines_iter: Box<dyn Iterator<Item = String>>,
    sender: Sender<UsageData>,
) -> Result<(), ErrorKind> {
    info!("Reading meter data");
    loop {
        let message: Vec<std::string::String> = lines_iter
            .by_ref()
            .skip_while(|l| !l.starts_with('/'))
            .take_while(|l| !l.starts_with('!'))
            .collect();
        let result = deserialise_p1_message(message);
        match result {
            Ok(r) => sender.send(r).map_err(|_| ErrorKind::BrokenPipe)?,
            Err(e) => {
                error!("Failure to deserialise p1 message: {}", e);
                continue;
            }
        }
        thread::park();
    }
}

fn deserialise_p1_message(
    message: Vec<std::string::String>,
) -> Result<UsageData, serde_json::Error> {
    let mut hash: HashMap<String, Reading> = HashMap::new();
    for item in message.iter() {
        let a = item.replace(")", "");
        let x: Vec<&str> = a.split('(').collect();
        if x.len() > 1 {
            // Timestamps have a different format than the rest of P1 the records so we need to catch it and parse it first
            if x[0].to_string() == "0-0:1.0.0" {
                let timestamp = parse_date(x[1], "%y%m%d%H%M%S");
                match timestamp {
                    Ok(t) => {
                        hash.insert(
                            x[0].to_string(),
                            Reading::Timestamp(Timestamp { timestamp: t }),
                        );
                    }
                    Err(e) => error!("Failed to parse: {}", e),
                }
            }
            // Gas is an exception because it posts two values of timestamp and reading instead of just a reading
            if x[0].to_string() == "0-1:24.2.1" {
                let timestamp = parse_date(x[1], "%y%m%d%H%M%S");
                match timestamp {
                    Ok(t) => {
                        hash.insert(
                            "gas_timestamp".to_string(),
                            Reading::Timestamp(Timestamp { timestamp: t }),
                        );
                    }
                    Err(e) => error!("{}", e),
                }
                let gas_volume: Vec<&str> = x[2].split('*').collect();
                let gas_vol_reading = gas_volume[0].parse::<f64>();
                match gas_vol_reading {
                    Ok(g) => {
                        hash.insert(
                            x[0].to_string(),
                            Reading::Measurement(Measurement {
                                value: g,
                                unit: gas_volume[1].to_string(),
                            }),
                        );
                    }
                    Err(e) => error!("{}", e),
                }
            } else {
                if x[1].to_string().contains("*") {
                    let z: Vec<&str> = x[1].split('*').collect();
                    let reading = z[0].parse::<f64>();
                    match reading {
                        Ok(r) => {
                            hash.insert(
                                x[0].to_string(),
                                Reading::Measurement(Measurement {
                                    value: r,
                                    unit: z[1].to_string(),
                                }),
                            );
                        }
                        Err(e) => error!("{}", e),
                    }
                }
            }
        }
    }
    let deserialised: UsageData = serde_json::from_value(match serde_json::to_value(&hash) {
        Ok(it) => it,
        Err(err) => return Err(err),
    })?;
    Ok(deserialised)
}

fn parse_date(date: &str, fmt: &str) -> Result<DateTime<FixedOffset>, ErrorKind> {
    let cest: FixedOffset = {
        let secs = 2 * 3600;
        FixedOffset::east_opt(secs).expect("FixedOffset::east out of bounds")
    };
    let cet: FixedOffset = FixedOffset::east_opt(3600).expect("FixedOffset::east out of bounds");
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
        error!("Error in date parsing: {}", date);
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

#[cfg(test)]

mod tests {
    use super::*;
    // use chrono::FixedOffset;
    // use chrono::TimeZone;

    // lines mapping data
    // Ok("/ISK5\\2M550E-1012")
    // Ok("")
    // Ok("1-3:0.2.8(50)")
    // Ok("0-0:1.0.0(230129232331W)")
    // Ok("0-0:96.1.1(4530303433303036393939363136373137)")
    // Ok("1-0:1.8.1(005348.844*kWh)")
    // Ok("1-0:1.8.2(008077.564*kWh)")
    // Ok("1-0:2.8.1(000748.939*kWh)")
    // Ok("1-0:2.8.2(001517.612*kWh)")
    // Ok("0-0:96.14.0(0001)")
    // Ok("1-0:1.7.0(00.208*kW)")
    // Ok("1-0:2.7.0(00.000*kW)")
    // Ok("0-0:96.7.21(00009)")
    // Ok("0-0:96.7.9(00004)")
    // Ok("1-0:99.97.0(2)(0-0:96.7.19)(220306205206W)(0000003909*s)(221224033820W)(0000016195*s)")
    // Ok("1-0:32.32.0(00010)")
    // Ok("1-0:32.36.0(00001)")
    // Ok("0-0:96.13.0()")
    // Ok("1-0:32.7.0(236.6*V)")
    // Ok("1-0:31.7.0(001*A)")
    // Ok("1-0:21.7.0(00.208*kW)")
    // Ok("1-0:22.7.0(00.000*kW)")
    // Ok("0-1:24.1.0(003)")
    // Ok("0-1:96.1.0(4730303332353635353231343231383137)")
    // Ok("0-1:24.2.1(230129232007W)(06664.357*m3)")
    // Ok("!5C6B")

    #[test]
    fn test_p1_deserialise() {
        let message: Vec<String> = vec![
            "/ISK5\\2M550E-1012".to_string(),
            "".to_string(),
            "1-3:0.2.8(50)".to_string(),
            "0-0:1.0.0(201221010833W)".to_string(),
            "0-0:96.1.1(123456)".to_string(),
            "1-0:1.8.1(002134.177*kWh)".to_string(),
            "1-0:1.8.2(003448.211*kWh)".to_string(),
            "1-0:2.8.1(000000.000*kWh)".to_string(),
            "1-0:2.8.2(000000.000*kWh)".to_string(),
            "0-0:96.14.0(0001)".to_string(),
            "1-0:1.7.0(00.229*kW)".to_string(),
            "1-0:2.7.0(00.000*kW)".to_string(),
            "0-0:96.7.21(00012)".to_string(),
            "0-0:96.7.9(00002)".to_string(),
            "1-0:99.97.0()".to_string(),
            "1-0:32.32.0(00012)".to_string(),
            "1-0:32.36.0(00001)".to_string(),
            "0-0:96.13.0()".to_string(),
            "1-0:32.7.0(236.7*V)".to_string(),
            "1-0:31.7.0(001*A)".to_string(),
            "1-0:21.7.0(00.220*kW)".to_string(),
            "1-0:22.7.0(00.000*kW)".to_string(),
            "0-1:24.1.0(003)".to_string(),
            "0-1:96.1.0(123456)".to_string(),
            "0-1:24.2.1(101221010511W)(03799.479*m3)".to_string(),
        ];

        // sample data
        // ["/ISK5\\2M550E-1012", "", "1-3:0.2.8(50)", "0-0:1.0.0(230123150731W)", "0-0:96.1.1(4530303433303036393939363136373137)", "1-0:1.8.1(005327.526*kWh)", "1-0:1.8.2(008037.510*kWh)", "1-0:2.8.1(000747.103*kWh)", "1-0:2.8.2(001516.970*kWh)", "0-0:96.14.0(0002)", "1-0:1.7.0(00.060*kW)", "1-0:2.7.0(00.000*kW)", "0-0:96.7.21(00009)", "0-0:96.7.9(00004)", "1-0:99.97.0(2)(0-0:96.7.19)(220306205206W)(0000003909*s)(221224033820W)(0000016195*s)", "1-0:32.32.0(00010)", "1-0:32.36.0(00001)", "0-0:96.13.0()", "1-0:32.7.0(233.3*V)", "1-0:31.7.0(001*A)", "1-0:21.7.0(00.062*kW)", "1-0:22.7.0(00.000*kW)", "0-1:24.1.0(003)", "0-1:96.1.0(4730303332353635353231343231383137)", "0-1:24.2.1(230123150502W)(06621.391*m3)"]

        // message ["/ISK5\\2M550E-1012", "", "1-3:0.2.8(50)", "0-0:1.0.0(230123170310W)", "0-0:96.1.1(4530303433303036393939363136373137)", "1-0:1.8.1(005327.526*kWh)", "1-0:1.8.2(008038.139*kWh)", "1-0:2.8.1(000747.103*kWh)", "1-0:2.8.2(001517.024*kWh)", "0-0:96.14.0(0002)", "1-0:1.7.0(02.781*kW)", "1-0:2.7.0(00.000*kW)", "0-0:96.7.21(00009)", "0-0:96.7.9(00004)", "1-0:99.97.0(2)(0-0:96.7.19)(220306205206W)(0000003909*s)(221224033820W)(0000016195*s)", "1-0:32.32.0(00010)", "1-0:32.36.0(00001)", "0-0:96.13.0()", "1-0:32.7.0(232.4*V)", "1-0:31.7.0(011*A)", "1-0:21.7.0(02.771*kW)", "1-0:22.7.0(00.000*kW)", "0-1:24.1.0(003)", "0-1:96.1.0(4730303332353635353231343231383137)", "0-1:24.2.1(230123170004W)(06622.103*m3)"]
        // resultOk(UsageData { electricity_timestamp: Timestamp(Timestamp { timestamp: 2023-01-23T17:03:10+01:00 }), power_receiving: Measurement(Measurement { value: 2.781, unit: "kW" }), power_returning: Measurement(Measurement { value: 0.0, unit: "kW" }), electricity_returned_reading_low_tariff: Measurement(Measurement { value: 747.103, unit: "kWh" }), electricity_returned_reading_normal_tariff: Measurement(Measurement { value: 1517.024, unit: "kWh" }), electricity_reading_low_tariff: Measurement(Measurement { value: 5327.526, unit: "kWh" }), electricity_reading_normal_tariff: Measurement(Measurement { value: 8038.139, unit: "kWh" }), gas_reading: Measurement(Measurement { value: 6622.103, unit: "m3" }), gas_timestamp: Timestamp(Timestamp { timestamp: 2023-01-23T17:00:04+01:00 }), voltage: Measurement(Measurement { value: 232.4, unit: "V" }), current: Measurement(Measurement { value: 11.0, unit: "A" }) })

        // let message = Vec::new();
        // message.push("/ISK5\\2M550E-1012");

        let result = deserialise_p1_message(message);

        let expected_data = UsageData {
            electricity_timestamp: Reading::Timestamp(Timestamp {
                timestamp: FixedOffset::east_opt(1 * 3600)
                    .unwrap()
                    .with_ymd_and_hms(2020, 12, 21, 1, 8, 33)
                    .unwrap(),
            }),
            power_receiving: Reading::Measurement(Measurement {
                value: 0.229,
                unit: "kW".to_string(),
            }),
            power_returning: Reading::Measurement(Measurement {
                value: 0.0,
                unit: "kW".to_string(),
            }),
            electricity_returned_reading_low_tariff: Reading::Measurement(Measurement {
                value: 0.0,
                unit: "kWh".to_string(),
            }),
            electricity_returned_reading_normal_tariff: Reading::Measurement(Measurement {
                value: 0.0,
                unit: "kWh".to_string(),
            }),
            electricity_reading_low_tariff: Reading::Measurement(Measurement {
                value: 2134.177,
                unit: "kWh".to_string(),
            }),
            electricity_reading_normal_tariff: Reading::Measurement(Measurement {
                value: 3448.211,
                unit: "kWh".to_string(),
            }),
            gas_reading: Reading::Measurement(Measurement {
                value: 3799.479,
                unit: "m3".to_string(),
            }),
            gas_timestamp: Reading::Timestamp(Timestamp {
                timestamp: FixedOffset::east_opt(1 * 3600)
                    .unwrap()
                    .with_ymd_and_hms(2010, 12, 21, 1, 5, 11)
                    .unwrap(),
            }),
            voltage: Reading::Measurement(Measurement {
                value: 236.7,
                unit: "V".to_string(),
            }),
            current: Reading::Measurement(Measurement {
                value: 1.0,
                unit: "A".to_string(),
            }),
        };
        match result {
            Ok(status) => assert_eq!(status, expected_data),
            Err(e) => error!("{}", e),
        };
    }
}
