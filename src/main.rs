mod dsmrlib;
mod influx_wrapper;

#[tokio::main]
async fn main() {
    let influxdb_client = influx_wrapper::InfluxDbClient {
        ..Default::default()
    };
    let influx_db = influxdb_client.setup_database().await.unwrap();
    println!("influx_db: {:?}", influx_db);

    let serial_device: String = "/dev/ttyUSB0".to_string();
    dsmrlib::DsmrClient {
        serial_device: serial_device,
        influx_db: influx_db,
    }
    .send_to_influxdb()
    .await;
}
