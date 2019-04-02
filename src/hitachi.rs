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

use crate::error::{MetricsResult, StorageError};

use std::collections::HashMap;
use std::str;
use std::str::FromStr;

use crate::ir::{TsPoint, TsValue};
use chrono::offset::Utc;
use chrono::DateTime;
use csv::Reader;
use log::{error, trace, warn};
use reqwest::header::ACCEPT;

#[derive(Deserialize, Debug)]
pub struct Collection {
    pub items: Vec<HashMap<String, serde_json::Value>>,
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
            // Short is i16 but we're going to upsize it here to i32
            // since TsValue doesn't have an i16 variant
            match i32::from_str(&r) {
                Ok(f) => {
                    ret_vals.push((header.into(), TsValue::Integer(f)));
                }
                Err(_) => {
                    error!("unable to convert {} to i32. Skipping", r);
                }
            };
        } else if v.contains("unsigned char") {
            match u8::from_str(&r) {
                Ok(f) => {
                    ret_vals.push((header.into(), TsValue::Byte(f)));
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
        .basic_auth(config.user.clone(), Some(config.password.clone()))
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
        .basic_auth(config.user.clone(), Some(config.password.clone()))
        .send()?
        .error_for_status()?
        .text()?;
    trace!("server response: {}", content);
    Ok(content)
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
