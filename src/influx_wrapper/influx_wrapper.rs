use influx_db_client::Client;
use log::info;
use reqwest::Url;
use std::env;
use std::error::Error;
use std::string::String;

pub struct InfluxDbClient {
    pub address: String,
    pub port: String,
    pub database_name: String,
}

impl InfluxDbClient {
    pub async fn setup_database(&self) -> Result<Client, Box<dyn Error>> {
        let url = Url::parse(&format!("{}:{}", self.address, self.port))?;
        info!("influx: {}", url);
        let mut client = Client::new(url, &self.database_name);
        client.switch_database(&self.database_name);
        let db_exists = client.ping().await;
        if !db_exists {
            return Err(Box::new(influx_db_client::Error::Communication(
                String::from("Cannot connect to Influx DB"),
            )));
        }
        match client.create_database(&self.database_name).await {
            Ok(_) => Ok(client),
            Err(e) => Err(Box::new(e)),
        }
    }
}

impl Default for InfluxDbClient {
    fn default() -> Self {
        InfluxDbClient {
            address: get_env_var("INFLUX_DB_ADDRESS"),
            port: get_env_var("INFLUX_DB_PORT"),
            database_name: get_env_var("INFLUX_DB_NAME"),
        }
    }
}

fn get_env_var(key: &str) -> String {
    match env::var(key) {
        Ok(val) => val.to_string(),
        Err(error) => error.to_string(),
    }
}
