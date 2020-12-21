use chrono::DateTime;
use chrono::FixedOffset;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::ErrorKind;

#[test]

fn it_adds_two() {
    let message: [&str; 25] = [
        "/ISK5\\2M550E-1012",
        "",
        "1-3:0.2.8(50)",
        "0-0:1.0.0(201221010833W)",
        "0-0:96.1.1(1530303433303036393939363136373137)",
        "1-0:1.8.1(002134.177*kWh)",
        "1-0:1.8.2(003448.211*kWh)",
        "1-0:2.8.1(000000.000*kWh)",
        "1-0:2.8.2(000000.000*kWh)",
        "0-0:96.14.0(0001)",
        "1-0:1.7.0(00.229*kW)",
        "1-0:2.7.0(00.000*kW)",
        "0-0:96.7.21(00009)",
        "0-0:96.7.9(00002)",
        "1-0:99.97.0()",
        "1-0:32.32.0(00009)",
        "1-0:32.36.0(00001)",
        "0-0:96.13.0()",
        "1-0:32.7.0(236.7*V)",
        "1-0:31.7.0(001*A)",
        "1-0:21.7.0(00.220*kW)",
        "1-0:22.7.0(00.000*kW)",
        "0-1:24.1.0(003)",
        "0-1:96.1.0(1730303332353635353231343231383137)",
        "0-1:24.2.1(101221010511W)(03799.479*m3)",
    ];

    #[derive(Debug, Deserialize, Serialize)]
    pub struct UsageData2 {
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

    #[derive(Debug, Deserialize, Serialize)]
    enum Reading {
        Measurement(Measurement),
        Timestamp(Timestamp),
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct Measurement {
        value: f64,
        unit: String,
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct Timestamp {
        timestamp: DateTime<FixedOffset>,
    }

    let mut hash: HashMap<String, Reading> = HashMap::new();
    for item in message.iter() {
        let a = item.replace(")", "");
        let x: Vec<&str> = a.split('(').collect();
        println!("x {:?}", x);
        if x.len() > 1 {
            // Timestamps have a different format than the rest of P1 the records so we need to catch it and parse it first
            if x[0].to_string() == "0-0:1.0.0" {
                let timestamp = parse_date(x[1], "%y%m%d%H%M%S");
                println!("timestamp {:?}", timestamp);
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
    println!("hash {:?} \n", hash);
    let v = serde_json::to_value(&hash).unwrap();
    println!("v {:#?} \n", v);
    let deserialised: UsageData2 = serde_json::from_value(v).unwrap();
    println!("deserialised {:?} \n", deserialised);

    assert_eq!(4, 2);
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
