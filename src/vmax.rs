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
use crate::error::MetricsResult;
use crate::ChildPoint;
use crate::IntoPoint;

use std::fmt::Debug;
use std::str;

use crate::ir::{TsPoint, TsValue};
use log::{debug, trace};
use reqwest::header::ACCEPT;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_json::{json, Value};

#[derive(Deserialize, Debug)]
pub struct VmaxConfig {
    /// The scaleio endpoint to use
    pub endpoint: String,
    pub user: String,
    /// This gets replaced with the token at runtime
    pub password: String,
    /// Optional certificate file to use against the server
    /// der encoded
    pub certificate: Option<String>,
    /// The region this cluster is located in
    pub region: String,
}

/* These are the GET and POST functions depending upon the output needed
*/
fn get_data<T>(
    client: &reqwest::Client,
    config: &VmaxConfig,
    api_endpoint: &str,
    point_name: &str,
) -> MetricsResult<Vec<TsPoint>>
where
    T: DeserializeOwned + Debug + IntoPoint,
{
    let url = format!(
        "https://{}/univmax/restapi/{}",
        config.endpoint, api_endpoint,
    );
    let j: T = crate::get(&client, &url, &config.user, Some(&config.password))?;

    Ok(j.into_point(Some(point_name), true))
}

/* Changed the GET to POST and added body parameter to pass additional fields to
*/
fn post_data_to_points<T, U>(
    client: &reqwest::Client,
    config: &VmaxConfig,
    api_endpoint: &str,
    body: &U,
    point_name: &str,
) -> MetricsResult<Vec<TsPoint>>
where
    T: DeserializeOwned + Debug + IntoPoint,
    U: Serialize + ?Sized,
{
    let array_output = client
        .post(&format!(
            "https://{}/univmax/restapi/{}",
            config.endpoint, api_endpoint,
        ))
        .basic_auth(config.user.clone(), Some(config.password.clone()))
        .header(ACCEPT, "application/json")
        .json(body)
        .send()?
        .error_for_status()?
        .text()?;
    trace!("{}", array_output);
    let json_res: Result<T, serde_json::Error> = serde_json::from_str(&array_output);
    trace!("json result: {:?}", json_res);
    let json_res = json_res?;
    Ok(json_res.into_point(Some(point_name), true))
}

/* Changed the GET to POST and added body parameter to pass additional field to w/o IntoPoint
So this is good for capturing info w/o reporting into the back-end
*/
fn post_data<T, U>(
    client: &reqwest::Client,
    config: &VmaxConfig,
    api_endpoint: &str,
    body: &U,
) -> MetricsResult<T>
where
    T: DeserializeOwned + Debug,
    U: Serialize + ?Sized,
{
    let array_output = client
        .post(&format!(
            "https://{}/univmax/restapi/{}",
            config.endpoint, api_endpoint,
        ))
        .basic_auth(config.user.clone(), Some(config.password.clone()))
        .header(ACCEPT, "application/json")
        .json(body)
        .send()?
        .error_for_status()?
        .text()?;
    trace!("{}", array_output);
    let json_res: Result<T, serde_json::Error> = serde_json::from_str(&array_output);
    trace!("json result: {:?}", json_res);
    let json_res = json_res?;
    Ok(json_res)
}

// Grab the list of things to operate on from vmax
fn get_list(
    client: &reqwest::Client,
    config: &VmaxConfig,
    api_endpoint: &str,
    key: &str,
) -> MetricsResult<Vec<String>> {
    let data: Value = client
        .get(&format!(
            "https://{}/univmax/restapi/{}",
            config.endpoint, api_endpoint,
        ))
        .basic_auth(config.user.clone(), Some(config.password.clone()))
        .send()?
        .error_for_status()?
        .json()?;

    // Grab the list from the json
    match data[key].as_array() {
        Some(v) => Ok(v
            .iter()
            .map(|val| val.as_str().unwrap().to_string())
            .collect::<Vec<String>>()),
        None => Ok(vec![]),
    }
}

#[derive(Debug, Deserialize)]
pub struct Srps {
    pub srp: Vec<Srp>,
    pub success: bool,
}

impl IntoPoint for Srps {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        for s in &self.srp {
            points.extend(s.into_point(name, is_time_series));
        }
        points
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize, IntoPoint)]
pub struct Srp {
    pub srpId: String,
    pub num_of_disk_groups: Option<i64>,
    pub description: Option<String>,
    pub emulation: Option<String>,
    pub overall_efficiency: Option<String>,
    pub compression_state: Option<String>,
    pub reserved_cap_percent: Option<i64>,
    pub total_usable_cap_gb: Option<f64>,
    pub total_subscribed_cap_gb: Option<f64>,
    pub total_allocated_cap_gb: Option<f64>,
    pub total_snapshot_allocated_cap_gb: Option<f64>,
    pub total_srdf_dse_allocated_cap_gb: Option<f64>,
    pub rdfa_dse: Option<bool>,
    pub num_of_srp_slo_demands: Option<i64>,
    pub num_of_srp_sg_demands: Option<i64>,
    pub diskGroupId: Option<Vec<String>>,
    pub srpSgDemandId: Option<Vec<String>>,
    pub srpSloDemandId: Option<Vec<String>>,
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct StorageGroups {
    pub storageGroup: Vec<StorageGroup>,
    pub success: bool,
}

impl IntoPoint for StorageGroups {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        for s in &self.storageGroup {
            points.extend(s.into_point(name, is_time_series));
        }
        points
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize, IntoPoint)]
pub struct StorageGroup {
    pub storageGroupId: String,
    pub slo: Option<String>,
    pub srp: Option<String>,
    pub workload: Option<String>,
    pub slo_compliance: Option<String>,
    pub num_of_vols: Option<i64>,
    pub num_of_child_sgs: Option<i64>,
    pub num_of_parent_sgs: Option<i64>,
    pub num_of_masking_views: Option<i64>,
    pub num_of_snapshots: Option<i64>,
    pub cap_gb: Option<f64>,
    pub device_emulation: Option<String>,
    #[serde(rename = "type")]
    pub group_type: Option<String>,
    pub child_storage_group: Option<Vec<String>>,
    pub parent_storage_group: Option<Vec<String>>,
    pub maskingview: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct SloArray {
    pub symmetrix: Vec<Symmetrix>,
    pub success: bool,
}

impl IntoPoint for SloArray {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        for s in &self.symmetrix {
            points.extend(s.into_point(name, is_time_series));
        }
        points
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct Symmetrix {
    pub symmetrixId: String,
    pub device_count: Option<i64>,
    pub ucode: Option<String>,
    pub model: Option<String>,
    pub local: Option<bool>,
    pub sloCompliance: Option<SloCompliance>,
    pub virtualCapacity: Option<VirtualCapacity>,
}

impl IntoPoint for Symmetrix {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut p = TsPoint::new(name.unwrap_or("vmax_symmetrix"), is_time_series);
        p.add_tag("symmetrixId", TsValue::String(self.symmetrixId.clone()));
        if let Some(d) = self.device_count {
            p.add_field("device_count", TsValue::SignedLong(d));
        }
        if let Some(ref s) = self.ucode {
            p.add_tag("device_count", TsValue::String(s.clone()));
        }
        if let Some(ref s) = self.model {
            p.add_tag("model", TsValue::String(s.clone()));
        }
        if let Some(b) = self.local {
            p.add_field("model", TsValue::Boolean(b));
        }
        if let Some(ref s) = self.sloCompliance {
            if let Some(stable) = s.slo_stable {
                p.add_field("slo_stable", TsValue::SignedLong(stable));
            }
            if let Some(marginal) = s.slo_marginal {
                p.add_field("slo_marginal", TsValue::SignedLong(marginal));
            }
            if let Some(critical) = s.slo_critical {
                p.add_field("slo_critical", TsValue::SignedLong(critical));
            }
        }
        if let Some(ref v) = self.virtualCapacity {
            p.add_field("used_capacity_gb", TsValue::Float(v.used_capacity_gb));
            p.add_field("total_capacity_gb", TsValue::Float(v.total_capacity_gb));
        }

        vec![p]
    }
}

#[derive(Debug, Deserialize)]
pub struct SloCompliance {
    pub slo_stable: Option<i64>,
    pub slo_marginal: Option<i64>,
    pub slo_critical: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct VirtualCapacity {
    pub used_capacity_gb: f64,
    pub total_capacity_gb: f64,
}

#[test]
fn test_get_slo_arrays() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/vmax/slo_array.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: SloArray = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
    println!("point: {:#?}", i.into_point(Some("slo_arrays"), true));
}

pub fn get_slo_arrays(client: &reqwest::Client, config: &VmaxConfig) -> MetricsResult<Vec<String>> {
    let arrays = get_list(client, config, "sloprovisioning/symmetrix", "symmetrixId")?;
    Ok(arrays)
}

pub fn get_slo_array(
    client: &reqwest::Client,
    config: &VmaxConfig,
    id: &str,
) -> MetricsResult<Vec<TsPoint>> {
    let points = get_data::<Symmetrix>(
        client,
        config,
        &format!("sloprovisioning/symmetrix/{}", id),
        "symmetrix",
    )?;
    Ok(points)
}

#[test]
fn test_get_slo_array_srps() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/vmax/slo_array_srp.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: Srps = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
    println!("point: {:#?}", i.into_point(Some("srp"), true));
}

pub fn get_slo_array_srps(
    client: &reqwest::Client,
    config: &VmaxConfig,
    id: &str,
) -> MetricsResult<Vec<String>> {
    let srps = get_list(
        client,
        config,
        &format!("sloprovisioning/symmetrix/{}/srp", id),
        "srpId",
    )?;
    Ok(srps)
}

pub fn get_slo_array_srp(
    client: &reqwest::Client,
    config: &VmaxConfig,
    id: &str,
    srp: &str,
) -> MetricsResult<Vec<TsPoint>> {
    let points = get_data::<Srps>(
        client,
        config,
        &format!("sloprovisioning/symmetrix/{}/srp/{}", id, srp),
        "slo_array_srp",
    )?;
    Ok(points)
}

#[test]
fn test_get_slo_array_storagegroups() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/vmax/slo_array_storagegroup.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: StorageGroups = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
    println!("point: {:#?}", i.into_point(Some("storage_group"), true));
}

pub fn get_slo_array_storagegroups(
    client: &reqwest::Client,
    config: &VmaxConfig,
    id: &str,
) -> MetricsResult<Vec<String>> {
    let groups = get_list(
        client,
        config,
        &format!("sloprovisioning/symmetrix/{}/storagegroup", id),
        "storageGroupId",
    )?;
    Ok(groups)
}

pub fn get_slo_array_storagegroup(
    client: &reqwest::Client,
    config: &VmaxConfig,
    id: &str,
    group: &str,
) -> MetricsResult<Vec<TsPoint>> {
    let points = get_data::<StorageGroups>(
        client,
        config,
        &format!("sloprovisioning/symmetrix/{}/storagegroup/{}", id, group),
        "slo_array_storagegroup",
    )?;
    Ok(points)
}

//START Section for Collecting VMAX Front-End Port list and Front-End Port Metrics
//For Collecting the VMAX Array Front-end Directors
#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize, IntoPoint)]
pub struct FeDirector {
    pub director_id: String,
    pub first_available_date: u64,
    pub last_available_date: u64,
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct FedArray {
    pub fe_director_info: Vec<FeDirector>,
}

impl IntoPoint for FedArray {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        for s in &self.fe_director_info {
            points.extend(s.into_point(name, is_time_series));
        }
        points
    }
}

//For Collecting the VMAX Array Front-end Directors Metrics
#[serde(rename_all = "PascalCase")]
#[derive(Debug, Deserialize, IntoPoint)]
pub struct FeDirectorMetrics {
    #[serde(rename = "AvgWPDiscTime")]
    pub avg_wpdisc_time: f64,
    #[serde(rename = "AvgReadMissResponseTime")]
    pub avg_read_miss_response_time: f64,
    pub percent_busy: f64,
    #[serde(rename = "HostIOs")]
    pub host_ios: f64,
    #[serde(rename = "HostMBs")]
    pub host_mbs: f64,
    pub reqs: f64,
    pub read_reqs: f64,
    pub write_reqs: f64,
    pub hit_reqs: f64,
    pub read_hit_reqs: f64,
    pub write_hit_reqs: f64,
    pub miss_reqs: f64,
    pub read_miss_reqs: f64,
    pub write_miss_reqs: f64,
    pub percent_re4ad_reqs: f64,
    pub percent_write_reqs: f64,
    pub percent_hit_reqs: f64,
    pub percent_read_req_hit: f64,
    #[serde(rename = "SystemWPEvents")]
    pub system_device_wpevents: f64,
    #[serde(rename = "DeviceWPEvents")]
    pub device_wpevents: f64,
    pub avg_time_per_syscall: f64,
    pub slot_collisions: f64,
    pub percent_write_req_hit: f64,
    pub total_read_count: f64,
    pub total_write_count: f64,
    pub read_response_time: f64,
    pub write_response_time: f64,
    #[serde(rename = "HostIOLimitIOs")]
    pub host_io_limit_ios: f64,
    #[serde(rename = "HostIOLimitMBs")]
    pub host_io_limit_mbs: f64,
    pub optimized_read_misses: f64,
    #[serde(rename = "OptimizedMBReadMisses")]
    pub optimized_mb_read_misses: f64,
    pub avg_optimized_read_miss_size: f64,
    pub queue_depth_utilization: f64,
    #[serde(rename = "timestamp")]
    pub timestamp: u64,
}

//Since the returned value are an Object--> Object-Array of values, resultlist-result
#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct FedArrayMetrics {
    pub result_list: FedResult,
}

impl IntoPoint for FedArrayMetrics {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        self.result_list.into_point(name, is_time_series)
    }
}

impl IntoPoint for FedResult {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        for r in &self.result {
            let res = r.into_point(name, is_time_series);
            points.extend(res);
        }
        points
    }
}

//Since the returned value are Object-Object-Array of values, result-array of objects
#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct FedResult {
    pub result: Vec<FeDirectorMetrics>,
}

/*Setting up new function for Metrics Collection. StartDate and endDate are in milliseconds
https://{server}/univmax/restapi/performance/FEDirector/metrics
{
  "startDate"   : "1522104538975",
  "endDate"     : "1522104573437",
  "symmetrixId" : "000196702346",
  "dataFormat"  : "Average",
  "metrics"     : [ "AvgRDFSWriteResponseTime","AvgReadMissResponseTime","AvgWPDiscTime", "AvgTimePerSyscall", "DeviceWPEvents","HostMBs","HitReqs","HostIOs","MissReqs","PercentBusy","PercentWriteReqs","PercentReadReqs" ],
  "directorId"  : "FA-1D"
}
The startDate and endDate are timestamp in milliseconds, the dataFormat is set to static "Average"
*/
pub fn get_fed_metrics(
    client: &reqwest::Client,
    config: &VmaxConfig,
    startdate: u64,
    enddate: u64,
    symmetrix_id: &str,
    director_id: &str,
    _dataformat: &str,
) -> MetricsResult<Vec<TsPoint>> {
    let vmaxmetrics = json! ({
        "startDate" : startdate.to_string(),
        "endDate" : enddate.to_string(),
        "symmetrixId" : symmetrix_id,
        "dataFormat" : "Average",
        "directorId" : director_id,
        "metrics" : [
            "AvgReadMissResponseTime","AvgWPDiscTime", "AvgTimePerSyscall", "DeviceWPEvents","HostMBs","HitReqs","HostIOs","MissReqs","PercentBusy","PercentWriteReqs","PercentReadReqs","PercentHitReqs","HostIOLimitMBs","AvgOptimizedReadMissSize","OptimizedMBReadMisses","OptimizedReadMisses","PercentReadReqHit","PercentWriteReqHit","QueueDepthUtilization","HostIOLimitIOs","ReadReqs","ReadHitReqs","ReadMissReqs","Reqs","ReadResponseTime","WriteResponseTime","SlotCollisions","SystemWPEvents","TotalReadCount","TotalWriteCount","WriteReqs","WriteHitReqs","WriteMissReqs"
        ]
    });
    debug!("Sending: {} to array", vmaxmetrics);
    let points = post_data_to_points::<FedArrayMetrics, Value>(
        client,
        config,
        "performance/FEDirector/metrics/",
        &vmaxmetrics,
        "fedvmaxmetrics",
    )?;
    Ok(points)
}

/*This is for adding the information for the Directors based upon the symmetrixId
https://{server}/univmax/restapi/performance/FEDirector/keys
{
  "symmetrixId" : "000196702346"
}
*/
pub fn get_fed_directors(
    client: &reqwest::Client,
    config: &VmaxConfig,
    symmetrixid: &str,
) -> MetricsResult<Vec<String>> {
    let vmaxdirectors = json! ({
        "symmetrixId" : symmetrixid,
    });
    let fedmet: FedArray = post_data::<FedArray, Value>(
        client,
        config,
        "performance/FEDirector/keys/",
        &vmaxdirectors,
    )?;
    let ids: Vec<String> = fedmet
        .fe_director_info
        .iter()
        .map(|f| f.director_id.clone())
        .collect();
    Ok(ids)
}
//END Section for Collecting VMAX Front-End Port list and Front-End Port Metrics

//START Section for Collecting VMAX PortGroup list and PortGroup Metrics
//For Collecting the VMAX Array PortGroup Listings
/* https://{server}/univmax/restapi/performance/PortGroup/keys
{
  "symmetrixId" : "000196702346"
}
*/
#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize, IntoPoint)]
pub struct PortGroup {
    pub port_group_id: String,
    pub first_available_date: u64,
    pub last_available_date: u64,
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct PortGroupArray {
    pub port_group_info: Vec<PortGroup>,
}

impl IntoPoint for PortGroupArray {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        for s in &self.port_group_info {
            points.extend(s.into_point(name, is_time_series));
        }
        points
    }
}

//For Collecting the VMAX Array PortGroup Metrics
#[serde(rename_all = "PascalCase")]
#[derive(Debug, Deserialize, IntoPoint)]
pub struct PortGroupMetrics {
    pub reads: f64,
    pub writes: f64,
    #[serde(rename = "IOs")]
    pub ios: f64,
    #[serde(rename = "MBRead")]
    pub mb_read: f64,
    #[serde(rename = "MBWritten")]
    pub mb_written: f64,
    #[serde(rename = "MBs")]
    pub mbs: f64,
    #[serde(rename = "AvgIOSize")]
    pub avg_io_size: f64,
    pub percent_busy: f64,
    #[serde(rename = "timestamp")]
    pub timestamp: u64,
}

//Since the returned value are an Object--> Object-Array of values, resultlist-result
#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct PortGroupArrayMetrics {
    pub result_list: PgResult,
}

impl IntoPoint for PortGroupArrayMetrics {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        self.result_list.into_point(name, is_time_series)
    }
}

impl IntoPoint for PgResult {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        for r in &self.result {
            let res = r.into_point(name, is_time_series);
            points.extend(res);
        }
        points
    }
}

//Since the returned value are Object-Object-Array of values, result-array of objects
#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct PgResult {
    pub result: Vec<PortGroupMetrics>,
}

pub fn get_portgroup_metrics(
    client: &reqwest::Client,
    config: &VmaxConfig,
    startdate: u64,
    enddate: u64,
    symmetrix_id: &str,
    port_group_id: &str,
    _dataformat: &str,
) -> MetricsResult<Vec<TsPoint>> {
    let vmaxpgmetrics = json! ({
        "startDate" : startdate.to_string(),
        "endDate" : enddate.to_string(),
        "symmetrixId" : symmetrix_id,
        "dataFormat" : "Average",
        "portGroupId" : port_group_id,
        "metrics" : [
            "Reads","Writes","IOs","MBRead","MBWritten","MBs","AvgIOSize","PercentBusy"
        ]
    });
    let points = post_data_to_points::<PortGroupArrayMetrics, Value>(
        client,
        config,
        "performance/PortGroup/metrics/",
        &vmaxpgmetrics,
        "portgroupvmaxmetrics",
    )?;
    Ok(points)
}

pub fn get_portgroups(
    client: &reqwest::Client,
    config: &VmaxConfig,
    symmetrix_id: &str,
) -> MetricsResult<Vec<String>> {
    let vmaxportgroups = json! ({
        "symmetrixId" : symmetrix_id,
    });
    let pgmet: PortGroupArray = post_data::<PortGroupArray, Value>(
        client,
        config,
        "performance/PortGroup/keys/",
        &vmaxportgroups,
    )?;
    let ids: Vec<String> = pgmet
        .port_group_info
        .iter()
        .map(|f| f.port_group_id.clone())
        .collect();
    Ok(ids)
}
//END Section for Collecting VMAX PortGroup list and PortGroup Metrics

//START Section for Collecting VMAX StorageGroup list and StorageGroup Metrics different from the SLO information
//For Collecting the VMAX Array StorageGroup Listings
/* https://{server}/univmax/restapi/performance/StorageGroup/keys
{
  "symmetrixId" : "000196702346"
}
*/
#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize, IntoPoint)]
pub struct StorageGroupsInfo {
    pub storage_group_id: String,
    pub first_available_date: u64,
    pub last_available_date: u64,
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct StorageGroupArray {
    pub storage_group_info: Vec<StorageGroupsInfo>,
}

impl IntoPoint for StorageGroupArray {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        for s in &self.storage_group_info {
            points.extend(s.into_point(name, is_time_series));
        }
        points
    }
}

//For Collecting the VMAX Array StorageGroup Metrics
#[serde(rename_all = "PascalCase")]
#[derive(Debug, Deserialize, IntoPoint)]
pub struct StorageGroupMetrics {
    #[serde(rename = "HostIOs")]
    pub host_ios: f64,
    pub host_reads: f64,
    pub host_writes: f64,
    pub host_hits: f64,
    pub host_read_hits: f64,
    pub host_write_hits: f64,
    pub host_misses: f64,
    pub host_read_misses: f64,
    pub host_write_misses: f64,
    #[serde(rename = "HostMBs")]
    pub host_mbs: f64,
    #[serde(rename = "HostMBReads")]
    pub host_mb_reads: f64,
    #[serde(rename = "HostMBWritten")]
    pub host_mb_written: f64,
    pub read_response_time: f64,
    pub write_response_time: f64,
    pub read_miss_response_time: f64,
    pub write_miss_response_time: f64,
    pub percent_read: f64,
    pub percent_write: f64,
    pub percent_read_hit: f64,
    pub percent_write_hit: f64,
    pub percent_read_miss: f64,
    pub percent_write_miss: f64,
    #[serde(rename = "SeqIOs")]
    pub seq_ios: f64,
    #[serde(rename = "RandomIOs")]
    pub random_ios: f64,
    #[serde(rename = "AvgIOSize")]
    pub avg_io_size: f64,
    pub avg_read_size: f64,
    pub avg_write_size: f64,
    pub percent_hit: f64,
    pub percent_misses: f64,
    pub response_time: f64,
    pub allocated_capacity: f64,
    #[serde(rename = "PercentRandomIO")]
    pub percent_random_io: f64,
    #[serde(rename = "timestamp")]
    pub timestamp: u64,
}

//Since the returned value are an Object--> Object-Array of values, resultlist-result
#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct StorageGroupArrayMetrics {
    pub result_list: SgResult,
}

impl IntoPoint for StorageGroupArrayMetrics {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        self.result_list.into_point(name, is_time_series)
    }
}

impl IntoPoint for SgResult {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        for r in &self.result {
            let res = r.into_point(name, is_time_series);
            points.extend(res);
        }
        points
    }
}

//Since the returned value are Object-Object-Array of values, result-array of objects
#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct SgResult {
    pub result: Vec<StorageGroupMetrics>,
}

pub fn get_storagegroup_metrics(
    client: &reqwest::Client,
    config: &VmaxConfig,
    startdate: u64,
    enddate: u64,
    symmetrix_id: &str,
    storage_group_id: &str,
    _dataformat: &str,
) -> MetricsResult<Vec<TsPoint>> {
    let vmaxsgmetrics = json! ({
        "startDate" : startdate.to_string(),
        "endDate" : enddate.to_string(),
        "symmetrixId" : symmetrix_id,
        "dataFormat" : "Average",
        "storageGroupId" : storage_group_id,
        "metrics" : [
            "HostIOs", "HostReads", "HostWrites", "HostHits", "HostReadHits", "HostWriteHits", "HostMisses", "HostReadMisses", "HostWriteMisses", "HostMBs", "HostMBReads", "HostMBWritten", "ReadResponseTime", "WriteResponseTime", "ReadMissResponseTime", "WriteMissResponseTime", "PercentRead", "PercentWrite", "PercentReadHit", "PercentWriteHit", "PercentReadMiss", "PercentWriteMiss", "SeqIOs", "RandomIOs", "AvgIOSize", "AvgReadSize", "AvgWriteSize", "PercentHit", "PercentMisses", "ResponseTime", "AllocatedCapacity", "PercentRandomIO"
        ]
    });
    let points = post_data_to_points::<StorageGroupArrayMetrics, Value>(
        client,
        config,
        "performance/StorageGroup/metrics/",
        &vmaxsgmetrics,
        "storagegroupvmaxmetrics",
    )?;
    Ok(points)
}

pub fn get_storagegroups(
    client: &reqwest::Client,
    config: &VmaxConfig,
    symmetrix_id: &str,
) -> MetricsResult<Vec<String>> {
    let vmaxstoragegroups = json! ({
        "symmetrixId" : symmetrix_id,
    });
    let sgmet: StorageGroupArray = post_data::<StorageGroupArray, Value>(
        client,
        config,
        "performance/StorageGroup/keys/",
        &vmaxstoragegroups,
    )?;
    let ids: Vec<String> = sgmet
        .storage_group_info
        .iter()
        .map(|f| f.storage_group_id.clone())
        .collect();
    Ok(ids)
}
//END Section for Collecting VMAX StorageGroup list and StorageGroup Metrics

//START Section for Test Functions
//For Collecting the VMAX Array Front-end Directors Listing
#[test]
fn test_get_per_array_fed() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/vmax/per_array_fed.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: FedArray = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

//For Collecting the VMAX Array Metrics based upon the FED Listing
#[test]
fn test_get_per_array_fed_metrics() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/vmax/per_array_fed_metrics.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: FedArrayMetrics = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

//For Collecting the VMAX Array PortGroup Listings
#[test]
fn test_get_per_array_portgroup() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/vmax/per_array_portgroup.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: PortGroupArray = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

//For Collecting the VMAX Array PortGroups Metrics
#[test]
fn test_get_per_array_portgroup_metrics() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/vmax/per_array_portgroup_metrics.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: PortGroupArrayMetrics = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

//For Collecting the VMAX Array StorageGroup Listings
#[test]
fn test_get_per_array_storagegroup() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/vmax/per_array_storagegroup.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: StorageGroupArray = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

//For Collecting the VMAX Array StorageGroups Metrics
#[test]
fn test_get_per_array_storagegroup_metrics() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/vmax/per_array_storagegroup_metrics2.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: StorageGroupArrayMetrics = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

//For Collecting the VMAX Array System Properties Metrics. This includes the RAW array values for capacity
//which is found in the GUI Unisphere --> Array --> System --> Symmetrix Properties
//https://[username]:[password]@[Unisphere Server]:8443/univmax/restapi/90/sloprovisioning/symmetrix/[Array S/N]
#[test]
fn test_get_slo_provisioning_system_properties() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/vmax/slo_provisioning_system_properties.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: VmaxSystemCapacity = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}
// END Section for Test Functions

// This function is for get_data only, the get_list was not needed. Note the '90' for v9 of the EMC Unisphere software
pub fn get_vmax_array_raw(
    client: &reqwest::Client,
    config: &VmaxConfig,
    symmetrixid: &str,
) -> MetricsResult<Vec<TsPoint>> {
    let vmax_raw = get_data::<VmaxSystemCapacity>(
        client,
        config,
        &format!("90/sloprovisioning/symmetrix/{}", symmetrixid),
        "vmax_array_raw",
    )?;
    debug!("result: {:#?}", vmax_raw);
    Ok(vmax_raw)
}

// This is split into objects and sub-objects based upon the new Unisphere release v9
// Added Option here since some of the arrays polled are not returning the same output, may need to be updated again down the road
#[derive(Debug, Deserialize)]
pub struct VmaxSystemCapacity {
    #[serde(rename = "symmetrixId")]
    pub symmetrix_id: String,
    pub device_count: u64,
    pub ucode: String,
    #[serde(rename = "targetUcode")]
    pub targetucode: Option<String>,
    pub model: String,
    pub local: bool,
    pub default_fba_srp: String,
    pub host_visible_device_count: u64,
    pub system_capacity: SystemCapacity,
    pub system_efficiency: SystemEfficiency,
    pub meta_data_usage: MetaDataUsage,
    #[serde(rename = "sloCompliance")]
    pub slo_compliance: SloComplianceSys,
    #[serde(rename = "physicalCapacity")]
    pub physical_capacity: PhysicalCapacity,
}

impl IntoPoint for VmaxSystemCapacity {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut p = TsPoint::new(name.unwrap_or("vmax_system_capacity"), is_time_series);
        p.add_tag("symmetrix_id", TsValue::String(self.symmetrix_id.clone()));
        p.add_field("device_count", TsValue::Long(self.device_count));
        p.add_field("ucode", TsValue::String(self.ucode.clone()));
        if let Some(ref target_ucode) = self.targetucode {
            p.add_field("targetucode", TsValue::String(target_ucode.clone()));
        }
        p.add_field("model", TsValue::String(self.model.clone()));
        p.add_field("local", TsValue::Boolean(self.local));
        p.add_tag(
            "default_fba_srp",
            TsValue::String(self.default_fba_srp.clone()),
        );
        p.add_field(
            "host_visible_device_count",
            TsValue::Long(self.host_visible_device_count),
        );
        self.system_capacity.sub_point(&mut p);
        self.system_efficiency.sub_point(&mut p);
        self.meta_data_usage.sub_point(&mut p);
        self.slo_compliance.sub_point(&mut p);
        self.physical_capacity.sub_point(&mut p);
        vec![p]
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct SystemCapacity {
    pub subscribed_allocated_tb: f64,
    pub subscribed_total_tb: f64,
    pub snapshot_modified_tb: f64,
    pub snapshot_total_tb: f64,
    pub usable_used_tb: f64,
    pub usable_total_tb: f64,
    pub subscribed_usable_capacity_percent: f64,
}

// This implements a new impl for sub_point based upon EMC metrics
// Use a tags for the those metrics defined as String value and field for the remaining, but not both
impl ChildPoint for SystemCapacity {
    fn sub_point(&self, p: &mut TsPoint) {
        p.add_field(
            "subscribed_allocated_tb",
            TsValue::Float(self.subscribed_allocated_tb),
        );
        p.add_field(
            "subscribed_total_tb",
            TsValue::Float(self.subscribed_total_tb),
        );
        p.add_field(
            "snapshot_modified_tb",
            TsValue::Float(self.snapshot_modified_tb),
        );
        p.add_field("snapshot_total_tb", TsValue::Float(self.snapshot_total_tb));
        p.add_field("usable_used_tb", TsValue::Float(self.usable_used_tb));
        p.add_field("usable_total_tb", TsValue::Float(self.usable_total_tb));
        p.add_field(
            "subscribed_usable_capacity_percent",
            TsValue::Float(self.subscribed_usable_capacity_percent),
        );
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct SystemEfficiency {
    pub overall_efficiency_ratio_to_one: f64,
    pub data_reduction_enabled_percent: f64,
    pub virtual_provisioning_savings_ratio_to_one: f64,
}

impl ChildPoint for SystemEfficiency {
    fn sub_point(&self, p: &mut TsPoint) {
        p.add_field(
            "overall_efficiency_ratio_to_one",
            TsValue::Float(self.overall_efficiency_ratio_to_one),
        );
        p.add_field(
            "data_reduction_enabled_percent",
            TsValue::Float(self.data_reduction_enabled_percent),
        );
        p.add_field(
            "virtual_provisioning_savings_ratio_to_one",
            TsValue::Float(self.virtual_provisioning_savings_ratio_to_one),
        );
    }
}

// Added Option here since some of the arrays polled are not returning the same output, may need to be updated again down the road
#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct MetaDataUsage {
    pub system_meta_data_used_percent: Option<f64>,
    pub replication_cache_used_percent: u64,
}

impl ChildPoint for MetaDataUsage {
    fn sub_point(&self, p: &mut TsPoint) {
        if let Some(system_meta_data) = self.system_meta_data_used_percent {
            p.add_field(
                "system_meta_data_used_percent",
                TsValue::Float(system_meta_data),
            );
        }
        p.add_field(
            "replication_cache_used_percent",
            TsValue::Long(self.replication_cache_used_percent),
        );
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct SloComplianceSys {
    pub slo_stable: i64,
    pub slo_marginal: i64,
    pub slo_critical: i64,
    pub no_slo: i64,
}

impl ChildPoint for SloComplianceSys {
    fn sub_point(&self, p: &mut TsPoint) {
        p.add_field("slo_stable", TsValue::SignedLong(self.slo_stable));
        p.add_field("slo_marginal", TsValue::SignedLong(self.slo_marginal));
        p.add_field("slo_critical", TsValue::SignedLong(self.slo_critical));
        p.add_field("no_slo", TsValue::SignedLong(self.no_slo));
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct PhysicalCapacity {
    pub used_capacity_gb: f64,
    pub total_capacity_gb: f64,
}

impl ChildPoint for PhysicalCapacity {
    fn sub_point(&self, p: &mut TsPoint) {
        p.add_field("used_capacity_gb", TsValue::Float(self.used_capacity_gb));
        p.add_field("total_capacity_gb", TsValue::Float(self.total_capacity_gb));
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize, IntoPoint)]
pub struct Volume {
    #[serde(rename = "volumeId")]
    pub volume_id: String,
    #[serde(rename = "type")]
    pub volume_type: Option<String>,
    pub emulation: Option<String>,
    pub ssid: Option<String>,
    pub allocated_percent: Option<u64>,
    pub cap_gb: Option<f64>,
    pub cap_mb: Option<f64>,
    pub cap_cyl: Option<u64>,
    pub status: Option<String>,
    pub reserved: Option<bool>,
    pub pinned: Option<bool>,
    pub physical_name: Option<String>,
    pub volume_identifier: Option<String>,
    pub wwn: Option<String>,
    pub encapsulated: Option<bool>,
    pub num_of_storage_groups: Option<u64>,
    pub num_of_fron_end_paths: Option<u64>,
    #[serde(rename = "storageGroupId")]
    pub storage_group_id: Option<Vec<String>>,
    // ignoring these two fields as they are not needed,
    // and don't have to store an array unnecessarily
    //#[serde(rename = "symmetrixPortKey")]
    //pub symmetrix_port_key: Option<Vec<SymmetrixPortKey>>,
    //#[serde(rename = "rdfGroupId")]
    //pub rdf_group_id: Option<Vec<u64>>,
    pub snapvx_source: Option<bool>,
    pub snapvx_target: Option<bool>,
    pub cu_image_base_address: Option<String>,
    pub has_effective_wwn: Option<bool>,
    pub effective_wwn: Option<String>,
    pub encapsulated_wwn: Option<String>,
}

#[test]
fn test_get_vmax_json_volume() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/vmax/slo_volume.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: Volume = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
    println!("point: {:#?}", i.into_point(Some("vmax_slo_volume")));
}

/// Returns a list of volume IDs for this array
// TODO: combine this with the other getters and generalize along with other metrics
pub fn get_all_slo_volumes(
    client: &reqwest::Client,
    config: &VmaxConfig,
    symmetrixid: &str,
) -> MetricsResult<Vec<String>> {
    let data: Value = client
        .get(&format!(
            "https://{}/univmax/restapi/90/sloprovisioning/symmetrix/{}/volume",
            config.endpoint, symmetrixid
        ))
        .basic_auth(&config.user, Some(&config.password))
        .send()?
        .error_for_status()?
        .json()?;

    let vol_count = data["count"].as_u64().unwrap();
    let iterator_id = data["id"].as_str().unwrap();
    let max_count_per_page = data["max_page_size"].as_u64().unwrap();
    let mut num_iterations = vol_count / max_count_per_page;

    if vol_count == 0 {
        return Ok(vec![]);
    }

    // grab the list from what was returned before calling the iterator
    let mut all_volume_ids = match data["resultList"]["result"]["volumeId"].as_array() {
        Some(v) => v
            .iter()
            .map(|val| val.as_str().unwrap().to_string())
            .collect::<Vec<String>>(),
        None => vec![],
    };

    while num_iterations != 0 {
        let from = all_volume_ids.len() + 1;
        let to = from as u64 + max_count_per_page;
        let body = json! ({
             "from" : from,
             "to"   : to
        });
        let data: Value = client
            .post(&format!(
                "https://{}/univmax/restapi/common/Iterator/{}/page",
                config.endpoint, iterator_id
            ))
            .basic_auth(&config.user, Some(&config.password))
            .header(ACCEPT, "application/json")
            .json(&body)
            .send()?
            .error_for_status()?
            .json()?;

        let v = match data["resultList"]["result"]["volumeId"].as_array() {
            Some(v) => v
                .iter()
                .map(|val| val.as_str().unwrap().to_string())
                .collect::<Vec<String>>(),
            None => vec![],
        };
        all_volume_ids.extend(v);
        num_iterations -= 1;
    }
    Ok(all_volume_ids)
}

pub fn get_slo_volume(
    client: &reqwest::Client,
    config: &VmaxConfig,
    volume_id: &str,
    symmetrixid: &str,
) -> MetricsResult<Vec<TsPoint>> {
    let volume = get_data::<Volume>(
        client,
        config,
        &format!(
            "90/sloprovisioning/symmetrix/{}/volume/{}",
            symmetrixid, volume_id
        ),
        "vmax_slo_volume",
    )?;
    Ok(volume)
}
