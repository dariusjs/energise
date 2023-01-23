use chrono::{FixedOffset, TimeZone};

use crate::dsmrlib::dsmrlib::{deserialise_p1_message, Measurement, Reading, Timestamp, UsageData};

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
            timestamp: FixedOffset::east(1 * 3600)
                .ymd(2020, 12, 21)
                .and_hms(1, 8, 33),
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
            timestamp: FixedOffset::east(1 * 3600)
                .ymd(2010, 12, 21)
                .and_hms(1, 5, 11),
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
        Err(e) => println!("{}", e),
    };
}
