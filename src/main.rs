use std::time::Duration;
use std::thread;
use std::sync::mpsc::{self, Sender, Receiver};
use serialport::{SerialPortSettings};
use std::io::BufReader;
use std::io::prelude::*;
use influx_db_client::Precision;
mod dsmrlib;
mod influx_wrapper;
// use dsmrlib::{get_meter_data, UsageData};
// use energise::*;

const BAUD_RATE: u32 = 115_200;
const TIMEOUT: u64 = 1000;



#[tokio::main]
async fn main() {
  let influxdb_client = influx_wrapper::InfluxDbClient {
    ..Default::default()
  };
  let influx_db = influxdb_client.setup_database().await.unwrap();
  println!("influx_db: {:?}", influx_db);
  // let influxdb_client = Client::default();

  // let influx_db = setup_database(dbhost, dbport, dbname).await.unwrap();

  let (sender, receiver): (Sender<dsmrlib::UsageData>, Receiver<dsmrlib::UsageData>) = mpsc::channel();
  let mut settings: SerialPortSettings = Default::default();
  settings.timeout = Duration::from_millis(TIMEOUT);
  settings.baud_rate = BAUD_RATE;
  let port = serialport::open_with_settings("/dev/ttyUSB0", &settings).unwrap();
  println!("Receiving data on {} at {} baud:", "/dev/ttyUSB0", &settings.baud_rate);

  let data_iter = BufReader::new(port).lines().map(|l| l.unwrap());
  let data_thread = thread::spawn(|| dsmrlib::get_meter_data(Box::new(data_iter), sender));
  loop {
      let data = receiver.recv();
      match data {
          Ok(data) => {
            influx_db.write_points(dsmrlib::usage_to_points(&data).unwrap(), Some(Precision::Seconds), None).await.unwrap();
            // println!("Data: {:?}", &data);
            // let _response = influxdb_client.send_payload(data).await;
            // let _response = influxdb_client.

          },
          Err(_) => continue
      }
      data_thread.thread().unpark();
  }
}
