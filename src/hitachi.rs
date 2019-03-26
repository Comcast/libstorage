#![allow(non_snake_case)]

/**
* Copyright 2019 Comcast Cable Communications Management, LLC
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* SPDX-License-Identifier: Apache-2.0
*/
use std::collections::HashMap;
use std::str;
use std::str::FromStr;

use crate::error::{MetricsResult, StorageError};
use crate::ir::{TsPoint, TsValue};
use crate::IntoPoint;

use chrono::offset::Utc;
use chrono::DateTime;
use csv::Reader;
use log::{error, warn};
use reqwest::header::ACCEPT;

#[derive(Deserialize, Debug)]
pub struct Collection {
    pub items: Vec<HashMap<String, serde_json::Value>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigManagerStorage {
    pub storage_device_id: String,
    pub model: String,
    pub serial_number: u64,
    pub svp_ip: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LdevPort {
    pub port_id: String,
    pub host_group_number: u64,
    pub host_group_name: String,
    pub lun: u64,
}

impl IntoPoint for LdevPort {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut p = TsPoint::new(name.unwrap_or_else(|| "hitachi_ldev_port"), is_time_series);
        p.add_tag("port_id", TsValue::String(self.port_id.clone()));
        p.add_field("host_group_number", TsValue::Long(self.host_group_number));
        p.add_tag(
            "host_group_name",
            TsValue::String(self.host_group_name.clone()),
        );
        p.add_field("lun", TsValue::String(convert_to_base16(self.lun)));

        vec![p]
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ServerResult<T> {
    #[serde(bound(deserialize = "T: serde::de::Deserialize<'de>"))]
    pub data: Vec<T>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageLdev {
    pub ldev_id: u64,
    pub clpr_id: u64,
    pub emulation_type: String,
    pub byte_format_capacity: String,
    pub block_capacity: u64,
    pub num_of_ports: u64,
    pub ports: Vec<LdevPort>,
    pub attributes: Vec<String>,
    pub status: String,
    pub mp_blade_id: u64,
    pub ssid: String,
    pub pool_id: u64,
    pub num_of_used_block: u64,
    pub is_relocation_enabled: bool,
    pub tier_level: String,
    pub used_capacity_per_tier_level1: u64,
    pub used_capacity_per_tier_level2: u64,
    pub used_capacity_per_tier_level3: Option<u64>,
    pub tier_level_for_new_page_allocation: String,
    pub resource_group_id: u64,
    pub data_reduction_status: String,
    pub data_reduction_mode: String,
    pub is_alua_enabled: bool,
}

impl IntoPoint for StorageLdev {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        let mut p = TsPoint::new(name.unwrap_or_else(|| "hitachi_ldev"), is_time_series);

        p.add_field("ldev_id", TsValue::String(convert_to_base16(self.ldev_id)));
        p.add_field("clpr_id", TsValue::Long(self.clpr_id));
        p.add_tag(
            "emulation_type",
            TsValue::String(self.emulation_type.clone()),
        );
        p.add_tag(
            "byte_format_capacity",
            TsValue::String(self.byte_format_capacity.clone()),
        );
        p.add_field(
            "block_capacity",
            TsValue::Long((self.block_capacity * 512) / 1024u64.pow(3)),
        );
        p.add_field("num_of_ports", TsValue::Long(self.num_of_ports));
        for port in &self.ports {
            let port_points: Vec<TsPoint> = port
                .into_point(Some("hitachi_ldev_port"), is_time_series)
                .into_iter()
                // Tag each port with ldev_id
                .map(|mut point| {
                    point.add_field("ldev_id", TsValue::Long(self.ldev_id.clone()));
                    point
                })
                .collect();
            points.extend(port_points);
        }
        p.add_tag("attributes", TsValue::StringVec(self.attributes.clone()));
        p.add_tag("status", TsValue::String(self.status.clone()));
        p.add_field("mp_blade_id", TsValue::Long(self.mp_blade_id));
        p.add_tag("ssid", TsValue::String(self.ssid.clone()));
        p.add_field("pool_id", TsValue::Long(self.pool_id));
        p.add_field(
            "num_of_used_block",
            TsValue::Long((self.num_of_used_block * 512) / 1024u64.pow(3)),
        );
        p.add_field(
            "is_relocation_enabled",
            TsValue::Boolean(self.is_relocation_enabled),
        );
        p.add_tag("tier_level", TsValue::String(self.tier_level.clone()));
        p.add_field(
            "used_capacity_per_tier_level1",
            TsValue::Long(self.used_capacity_per_tier_level1),
        );
        p.add_field(
            "used_capacity_per_tier_level2",
            TsValue::Long(self.used_capacity_per_tier_level2),
        );
        if let Some(tier_level3) = self.used_capacity_per_tier_level3 {
            p.add_field("used_capacity_per_tier_level3", TsValue::Long(tier_level3));
        }
        p.add_tag(
            "tier_level_for_new_page_allocation",
            TsValue::String(self.tier_level_for_new_page_allocation.clone()),
        );
        p.add_field("resource_group_id", TsValue::Long(self.resource_group_id));
        p.add_tag(
            "data_reduction_status",
            TsValue::String(self.data_reduction_status.clone()),
        );
        p.add_tag(
            "data_reduction_mode",
            TsValue::String(self.data_reduction_mode.clone()),
        );
        p.add_field("is_alua_enabled", TsValue::Boolean(self.is_alua_enabled));

        points.push(p);

        points
    }
}

#[derive(Deserialize, Debug)]
pub struct Version {
    pub productName: String,
    pub productVersion: String,
    pub apiVersion: String,
    pub description: String,
}

/// Convert a comma separated value vec of ByteRecords into timescaledb Value's
/// The headers field is used to figure out which type to convert into
fn into_values(
    records: &csv::ByteRecord,
    types: &csv::ByteRecord,
    headers: &csv::StringRecord,
) -> Vec<(String, TsValue)> {
    let mut ret_vals: Vec<(String, TsValue)> = Vec::new();
    for (pos, val) in types.iter().enumerate() {
        let v = String::from_utf8_lossy(val);
        let record = match records.get(pos) {
            Some(r) => r,
            None => {
                error!(
                    "Unable to get csv record at position {} from {:?}. Skipping",
                    pos, records
                );
                continue;
            }
        };
        if record.is_empty() {
            //warn!("Skipping column {} because the value is blank.", v);
            continue;
        }
        let header = match headers.get(pos) {
            Some(r) => r,
            None => {
                error!(
                    "Unable to get csv header at position {} from {:?}. Skipping",
                    pos, records
                );
                continue;
            }
        };
        // Need to convert this record into a String first before we can convert it into a value
        let r = String::from_utf8_lossy(record);
        // Using verbose matching here to log errors so we're aware of parsing problems. Otherwise
        // I'd use an if let
        // Added checking for value "short" in parsing the csv to convert
        if v.contains("string") {
            ret_vals.push((header.into(), TsValue::String(r.into_owned())));
        } else if v.contains("double") {
            match f64::from_str(&r) {
                Ok(f) => {
                    ret_vals.push((header.into(), TsValue::Float(f)));
                }
                Err(_) => {
                    error!("unable to convert {} to f64. Skipping", r);
                }
            }
        } else if v.contains("float") {
            match f64::from_str(&r) {
                Ok(f) => {
                    ret_vals.push((header.into(), TsValue::Float(f)));
                }
                Err(_) => {
                    error!("unable to convert {} to f64. Skipping", r);
                }
            }
        } else if v.contains("time_t") {
            ret_vals.push((header.into(), TsValue::String(r.into_owned())));
        } else if v.contains("ulong") {
            match u64::from_str(&r) {
                Ok(f) => {
                    ret_vals.push((header.into(), TsValue::Long(f)));
                }
                Err(_) => {
                    error!("unable to convert {} to u64. Skipping", r);
                }
            };
        } else if v.contains("long") {
            match i64::from_str(&r) {
                Ok(f) => {
                    ret_vals.push((header.into(), TsValue::SignedLong(f)));
                }
                Err(_) => {
                    error!("unable to convert {} to i64. Skipping", r);
                }
            };
        } else if v.contains("short") {
            match u16::from_str(&r) {
                Ok(f) => {
                    ret_vals.push((header.into(), TsValue::Short(f)));
                }
                Err(_) => {
                    error!("unable to convert {} to u16. Skipping", r);
                }
            };
        } else if v.contains("unsigned char") {
            match u16::from_str(&r) {
                Ok(f) => {
                    ret_vals.push((header.into(), TsValue::Short(f)));
                }
                Err(_) => {
                    error!("unable to convert {} to u8. Skipping", r);
                }
            };
        } else {
            warn!("Unknown type: {}, value: {}. Skipping", v, r);
            continue;
        }
    }

    ret_vals
}

#[test]
fn test_get_storage() {
    let json = include_str!("../tests/hitachi/config_manager.json");
    let s: ServerResult<ConfigManagerStorage> = serde_json::from_str(json).unwrap();
    println!("Result: {:?}", s);
}

#[test]
fn test_get_ldev() {
    let json = include_str!("../tests/hitachi/storage_ldev.json");
    let storage_id = "2038467351";
    let s: ServerResult<StorageLdev> = serde_json::from_str(json).unwrap();
    println!("Result: {:?}", s);
    let points: Vec<TsPoint> = s
        .data
        .iter()
        // Flatten all the Vec<TsPoint>'s
        .flat_map(|s| s.into_point(Some("hitachi_ldev"), false))
        .into_iter()
        // Tag each with storage_id
        .map(|mut point| {
            point.add_tag("storage_id", TsValue::String(storage_id.to_string()));
            point
        })
        .collect();
    println!("Result: {:#?}", points);
}

#[test]
fn test_new_parser() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/raid_pd_plc.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    println!("buff: {}", buff);
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[test]
fn test_raid_pi_lda() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/raid_pi_lda.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[test]
fn test_raid_pi_lds() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/raid_pi_lds.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[test]
fn test_raid_pi() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/raid_pi.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[test]
fn test_raid_pi_prcs() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/raid_pi_prcs.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[test]
fn test_raid_pi_pts() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/raid_pi_pts.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[test]
fn test_raid_pd_plts() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/raid_pd_plts.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[test]
fn test_raid_pd_plc() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/raid_pd_plc.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[test]
fn test_raid_pi_chs() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/raid_pi_chs.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

// This is the for the HDS HNAS since it is accessed in the same way as the HDS block arrays, different instancename
#[test]
fn test_nas_pd_hplc() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/nas_pd_hplc.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[test]
fn test_nas_pd_hsmu() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/nas_pd_hsmu.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[test]
fn test_nas_pd_hnc() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/nas_pd_hnc.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[test]
fn test_nas_pd_hfsc() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/nas_pd_hfsc.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[test]
fn test_nas_pi_hns() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/nas_pi_hns.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[test]
fn test_nas_pi_hnhs() {
    use std::fs::File;
    use std::io::Read;

    let mut buff = String::new();
    let mut f = File::open("tests/hitachi/nas_pi_hnhs.csv").unwrap();
    f.read_to_string(&mut buff).unwrap();
    let result = csv_to_points(&buff, "test", None).unwrap();
    println!("Result: {:?}", result);
}

#[derive(Clone, Deserialize, Debug)]
pub struct HitachiConfig {
    /// The hitachi endpoint to use
    pub endpoint: String,
    pub user: String,
    pub password: String,
    /// The region this cluster is located in
    pub region: String,
}
#[test]
fn test_convert_base() {
    let res = convert_to_base16(139);
    assert_eq!(res, "8B");
}

// Algorithm from: http://codeofthedamned.com/index.php/number-base-conversion
fn convert_to_base16(num: u64) -> String {
    let symbols: [char; 36] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
        'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    ];
    let mut remainder: [u64; 32] = [0; 32];
    let mut quotient: u64 = num;
    let mut place: u8 = 0;
    let mut output = String::new();

    while 0 != quotient {
        let value: u64 = quotient;
        remainder[place as usize] = value % 16;
        quotient = (value - remainder[place as usize]) / 16;
        place += 1;
    }
    for i in 1..=place {
        output.push(symbols[remainder[(place - i) as usize] as usize]);
    }
    output
}

/// This request obtains the detailed version of the API
pub fn get_version(client: &reqwest::Client, config: &HitachiConfig) -> MetricsResult<Version> {
    let version: Version = client
        .get(&format!(
            "http://@{endpoint}/TuningManager/v1/configuration/Version",
            endpoint = config.endpoint
        ))
        .basic_auth(config.user.clone(), Some(config.password.clone()))
        .header(ACCEPT, "application/json")
        .send()?
        .error_for_status()?
        .json()?;
    Ok(version)
}

pub fn get_agent_for_raid(
    client: &reqwest::Client,
    config: &HitachiConfig,
) -> MetricsResult<Collection> {
    let agents: Collection = client
        .get(&format!(
            "http://{}/TuningManager/v1/objects/AgentForRAID",
            config.endpoint
        ))
        .basic_auth(config.user.clone(), Some(config.password.clone()))
        .header(ACCEPT, "application/json")
        .send()?
        .error_for_status()?
        .json()?;

    Ok(agents)
}

// HDS NAS have a specific search criteria for the instance_name than what AgentFroRaid is. Alternate is to use agentType=ALL and will see everything
pub fn get_agent_for_nas(
    client: &reqwest::Client,
    config: &HitachiConfig,
) -> MetricsResult<Collection> {
    let agentnas: Collection = client
        .get(&format!(
            "http://{}/TuningManager/v1/objects/Agents?agentType=NAS",
            config.endpoint
        ))
        .basic_auth(&config.user, Some(&config.password))
        .header(ACCEPT, "application/json")
        .send()?
        .error_for_status()?
        .json()?;

    Ok(agentnas)
}

fn get_server_response(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    api_call: &str,
) -> MetricsResult<String> {
    let content = client
        .get(&format!(
            "http://{}/TuningManager/v1/objects/{}?hostName={}&agentInstanceName={}",
            config.endpoint, api_call, hostname, agent_instance_name,
        ))
        .basic_auth(&config.user, Some(&config.password))
        .send()?
        .error_for_status()?
        .text()?;
    Ok(content)
}

/// Note this only works with ConfigurationManager
pub fn get_storage(
    client: &reqwest::Client,
    config: &HitachiConfig,
) -> MetricsResult<ServerResult<ConfigManagerStorage>> {
    let endpoint = format!(
        "http://{}/ConfigurationManager/v1/objects/storages",
        config.endpoint
    );
    let s: ServerResult<ConfigManagerStorage> =
        super::get(&client, &endpoint, &config.user, Some(&config.password))?;
    Ok(s)
}

/// Note this only works with ConfigurationManager
pub fn get_ldev(
    client: &reqwest::Client,
    config: &HitachiConfig,
    storage_id: &str,
) -> MetricsResult<Vec<TsPoint>> {
    let endpoint = format!(
        "http://{}/ConfigurationManager/v1/objects/storages/{}/ldevs?ldevOption=dpVolume",
        config.endpoint, storage_id
    );
    let s: ServerResult<StorageLdev> =
        super::get(&client, &endpoint, &config.user, Some(&config.password))?;
    let points = s
        .data
        .iter()
        // Flatten all the Vec<TsPoint>'s
        .flat_map(|s| s.into_point(Some("hitachi_ldev"), false))
        .into_iter()
        // Tag each with storage_device_id
        .map(|mut point| {
            point.add_tag("storage_device_id", TsValue::String(storage_id.to_string()));
            point
        })
        .collect();

    Ok(points)
}

pub fn csv_to_points(
    text: &str,
    point_name: &str,
    t: Option<DateTime<Utc>>,
) -> MetricsResult<Vec<TsPoint>> {
    let mut points: Vec<TsPoint> = Vec::new();
    let mut rdr = Reader::from_reader(text.as_bytes());
    let headers = {
        let headers = rdr.headers()?;
        headers.clone()
    };
    let mut iter = rdr.byte_records();
    let fields = iter.next();
    if fields.is_none() {
        // We cannot continue without the fields
        error!(
            "Unable to discover csv fields.  Cannot parse record: {}",
            text
        );
        return Err(StorageError::new("CSV Parsing failure".into()));
    }
    let fields = fields.unwrap()?;

    for result in iter {
        let record = result?;
        let values = into_values(&record, &fields, &headers);
        let mut p = TsPoint::new(point_name, true);
        p.timestamp = t;
        for (name, value) in values {
            match value {
                TsValue::String(_) => {
                    p.add_tag(name, value);
                }
                _ => {
                    p.add_field(name, value);
                }
            };
        }
        points.push(p);
    }

    Ok(points)
}

pub fn get_raid_pi_prcs(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let result = get_server_response(
        client,
        config,
        hostname,
        agent_instance_name,
        "RAID_PI_PRCS",
    )?;
    let points = csv_to_points(&result, "raid_pi_prcs", Some(t))?;
    Ok(points)
}

pub fn get_raid_pi_lda(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let result = get_server_response(client, config, hostname, agent_instance_name, "RAID_PI_LDA")?;
    let points = csv_to_points(&result, "raid_pi_lda", Some(t))?;
    Ok(points)
}

pub fn get_raid_pi(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let result = get_server_response(client, config, hostname, agent_instance_name, "RAID_PI")?;
    let points = csv_to_points(&result, "raid_pi", Some(t))?;
    Ok(points)
}

pub fn get_raid_pd_plc(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let result = get_server_response(client, config, hostname, agent_instance_name, "RAID_PD_PLC")?;
    let points = csv_to_points(&result, "raid_pd_plc", Some(t))?;
    Ok(points)
}

pub fn get_raid_pi_chs(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let result = get_server_response(client, config, hostname, agent_instance_name, "RAID_PI_CHS")?;
    let points = csv_to_points(&result, "raid_pi_chs", Some(t))?;
    Ok(points)
}

pub fn get_raid_pd_plts(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let result = get_server_response(
        client,
        config,
        hostname,
        agent_instance_name,
        "RAID_PD_PLTS",
    )?;
    let points = csv_to_points(&result, "raid_pd_plts", Some(t))?;
    Ok(points)
}

pub fn get_raid_pi_pts(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let result = get_server_response(client, config, hostname, agent_instance_name, "RAID_PI_PTS")?;
    let points = csv_to_points(&result, "raid_pi_pts", Some(t))?;
    Ok(points)
}

pub fn get_nas_pd_hplc(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let result = get_server_response(client, config, hostname, agent_instance_name, "NAS_PD_HPLC")?;
    let points = csv_to_points(&result, "nas_pd_hplc", Some(t))?;
    Ok(points)
}

pub fn get_nas_pd_hsmu(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let result = get_server_response(client, config, hostname, agent_instance_name, "NAS_PD_HSMU")?;
    let points = csv_to_points(&result, "nas_pd_hsmu", Some(t))?;
    Ok(points)
}

pub fn get_nas_pd_hnc(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let result = get_server_response(client, config, hostname, agent_instance_name, "NAS_PD_HNC")?;
    let points = csv_to_points(&result, "nas_pd_hnc", Some(t))?;
    Ok(points)
}

pub fn get_nas_pd_hfsc(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let result = get_server_response(client, config, hostname, agent_instance_name, "NAS_PD_HFSC")?;
    let points = csv_to_points(&result, "nas_pd_hfsc", Some(t))?;
    Ok(points)
}

pub fn get_nas_pi_hns(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let result = get_server_response(client, config, hostname, agent_instance_name, "NAS_PI_HNS")?;
    let points = csv_to_points(&result, "nas_pi_hns", Some(t))?;
    Ok(points)
}

pub fn get_nas_pi_hnhs(
    client: &reqwest::Client,
    config: &HitachiConfig,
    hostname: &str,
    agent_instance_name: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let result = get_server_response(client, config, hostname, agent_instance_name, "NAS_PI_HNHS")?;
    let points = csv_to_points(&result, "nas_pi_hnhs", Some(t))?;
    Ok(points)
}
