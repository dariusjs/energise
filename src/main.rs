mod dsmrlib;
mod influx_wrapper;

#[tokio::main]
async fn main() {
    let influxdb_client = influx_wrapper::InfluxDbClient {
        ..Default::default()
    };
    let influx_db = influxdb_client.setup_database().await;
    let serial_device: String = "/dev/ttyUSB0".to_string();

    match influx_db {
        Ok(client) => {
            println!("influx_db: {:?}", client);
            dsmrlib::DsmrClient {
                serial_device: serial_device,
                influx_db: client,
            }
            .send_to_influxdb()
            .await
        }
        Err(e) => println!("{}", e),
    }
}
