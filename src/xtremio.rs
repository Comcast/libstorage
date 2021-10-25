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
use crate::deserialize_string_or_float;
use crate::deserialize_string_or_int;
use crate::error::MetricsResult;
use crate::IntoPoint;

use std::collections::HashMap;
use std::fmt::Debug;
use std::str;

use crate::ir::{TsPoint, TsValue};
use serde::de::DeserializeOwned;
use serde_json::Value;

#[derive(Clone, Deserialize, Debug)]
pub struct XtremIOConfig {
    /// The scaleio endpoint to use
    pub endpoint: String,
    pub user: String,
    /// This gets replaced with the token at runtime
    pub password: String,
    /// Optional certificate file to use against the server
    /// der encoded
    pub certificate: Option<String>,
    /// Optional root certificate file to use against the server
    /// der encoded
    pub root_certificate: Option<String>,
    /// The region this cluster is located in
    pub region: String,
}

pub struct XtremIo {
    client: reqwest::blocking::Client,
    config: XtremIOConfig,
}

impl XtremIo {
    pub fn new(client: &reqwest::blocking::Client, config: XtremIOConfig) -> Self {
        XtremIo {
            client: client.clone(),
            config,
        }
    }
}

#[test]
fn test_get_xtremio_volumes() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/xtremio/volumes.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: Volumes = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[derive(Deserialize, Debug)]
pub struct Link {
    pub href: String,
    pub rel: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum SmallIOAlert {
    Enabled,
    Disabled,
}

#[derive(Deserialize, Debug)]
pub struct Volumes {
    pub params: HashMap<String, String>,
    pub volumes: Vec<Volume>,
    pub links: Vec<Link>,
}

impl IntoPoint for Volumes {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        let n = name.unwrap_or("volume");

        for v in &self.volumes {
            points.extend(v.into_point(Some(n), is_time_series));
        }

        points
    }
}

#[derive(Deserialize, Debug, IntoPoint)]
#[serde(rename_all = "kebab-case")]
pub struct Volume {
    pub small_io_alerts: SmallIOAlert,
    pub created_by_app: Option<String>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub small_iops: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency: i64,
    pub vol_id: Vec<Value>,
    pub obj_severity: String,
    pub unaligned_io_alerts: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub unaligned_rd_bw: i64,
    pub num_of_dest_snaps: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_size_of_wr: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub iops: i64,
    pub small_io_ratio_level: String,
    pub dest_snap_list: Vec<Value>,
    pub guid: String,
    pub snapshot_type: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub logical_space_in_use: i64,
    pub unaligned_io_ratio_level: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_num_of_rd: i64,
    pub index: i64,
    pub lb_size: i64,
    pub naa_name: String,
    pub snapset_list: Vec<Value>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub unaligned_wr_bw: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_num_of_small_rd: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub unaligned_rd_iops: i64,
    pub snapgrp_id: Vec<Value>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_num_of_small_wr: i64,
    pub created_from_volume: String,
    pub ancestor_vol_id: Vec<Value>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub small_wr_iops: i64,
    pub creation_time: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw: i64,
    pub xms_id: Vec<Value>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_num_of_unaligned_rd: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub small_wr_bw: i64,
    pub tag_list: Vec<Value>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub unaligned_iops: i64,
    pub num_of_lun_mappings: i64,
    pub vol_access: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub small_rd_iops: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub unaligned_io_ratio: i64,
    pub lun_mapping_list: Vec<Value>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub vol_size: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops: i64,
    pub manager_guid: Option<String>,
    pub sys_id: Vec<Value>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency: i64,
    pub name: String,
    pub vaai_tp_alerts: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub unaligned_bw: i64,
    pub related_consistency_groups: Vec<Value>,
    pub certainty: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_num_of_unaligned_wr: i64,
    pub vol_type: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_num_of_wr: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub small_io_ratio: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_size_of_rd: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub unaligned_wr_iops: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub bw: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub small_rd_bw: i64,
    pub alignment_offset: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub small_bw: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw: i64,
}

#[derive(Deserialize, Debug)]
pub struct Ssds {
    pub ssds: Vec<Ssd>,
    pub params: HashMap<String, String>,
    pub links: Vec<Link>,
}

impl IntoPoint for Ssds {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        let n = name.unwrap_or("ssd");

        for v in &self.ssds {
            points.extend(v.into_point(Some(n), is_time_series));
        }

        points
    }
}

#[test]
fn test_get_xtremio_ssds() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/xtremio/ssds.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: Ssds = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[derive(Deserialize, Debug, IntoPoint)]
#[serde(rename_all = "kebab-case")]
pub struct Ssd {
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub ssd_size: i64,
    pub fru_lifecycle_state: String,
    pub smart_error_ascq: i64,
    pub ssd_failure_reason: String,
    pub percent_endurance_remaining: i64,
    pub obj_severity: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw: i64,
    pub part_number: String,
    pub serial_number: String,
    pub rg_id: Vec<Value>,
    pub health_state: Option<Value>,
    pub guid: String,
    pub index: i64,
    pub ssd_id: Vec<Value>,
    pub model_name: String,
    pub fw_version_error: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub ssd_space_in_use: i64,
    pub last_io_error_timestamp: i64,
    pub slot_num: i64,
    pub identify_led: String,
    pub io_error_ascq: i64,
    pub hw_revision: String,
    pub ssd_link2_health_state: String,
    pub io_error_asc: i64,
    pub num_bad_sectors: i64,
    pub xms_id: Vec<Value>,
    pub ssd_link1_health_state: String,
    pub percent_endurance_remaining_level: String,
    pub io_error_vendor_specific: i64,
    pub tag_list: Vec<Value>,
    pub io_error_sense_code: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub bw: i64,
    pub ssd_uid: String,
    pub smart_error_asc: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops: i64,
    pub swap_led: String,
    pub sys_id: Vec<Value>,
    pub last_io_error_type: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub iops: i64,
    pub diagnostic_health_state: String,
    pub name: String,
    pub brick_id: Vec<Value>,
    pub ssd_size_in_kb: i64,
    pub certainty: String,
    pub status_led: String,
    pub enabled_state: String,
    pub encryption_status: String,
    pub ssd_rg_state: String,
    pub fw_version: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub useful_ssd_space: i64,
}

#[derive(Deserialize, Debug)]
pub struct Psus {
    #[serde(rename = "storage-controller-psus")]
    pub storage_controller_psus: Vec<Psu>,
    pub params: HashMap<String, String>,
    pub links: Vec<Link>,
}

impl IntoPoint for Psus {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        let n = name.unwrap_or("psu");

        for v in &self.storage_controller_psus {
            points.extend(v.into_point(Some(n), is_time_series));
        }

        points
    }
}

#[test]
fn test_get_xtremio_psus() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/xtremio/psus.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: Psus = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[derive(Deserialize, Debug, IntoPoint)]
#[serde(rename_all = "kebab-case")]
pub struct Psu {
    pub index: i64,
    pub guid: String,
    pub name: String,
    pub brick_id: Vec<Value>,
    pub node_psu_id: Vec<Value>,
    pub obj_severity: String,
    pub fw_version_error: String,
    pub status_led: String,
    pub enabled_state: String,
    pub node_id: Vec<Value>,
    pub power_feed: String,
    pub serial_number: String,
    pub fru_lifecycle_state: String,
    pub location: String,
    pub part_number: String,
    pub input: String,
    pub fru_replace_failure_reason: String,
    pub model_name: String,
    pub hw_revision: String,
    pub sys_id: Vec<Value>,
    pub power_failure: String,
}

#[test]
fn test_get_xtremio_clusters() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/xtremio/clusters.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: Clusters = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[derive(Deserialize, Debug)]
pub struct Clusters {
    pub clusters: Vec<Cluster>,
    pub params: HashMap<String, String>,
    pub links: Vec<Link>,
}

impl IntoPoint for Clusters {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        let n = name.unwrap_or("cluster");

        for v in &self.clusters {
            points.extend(v.into_point(Some(n), is_time_series));
        }

        points
    }
}

#[derive(Deserialize, Debug, IntoPoint)]
#[serde(rename_all = "kebab-case")]
pub struct Cluster {
    pub compression_factor_text: String,
    pub os_upgrade_in_progress: String,
    pub ssh_firewall_mode: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_64kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_64kb: i64,
    pub free_ud_ssd_space_level: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_by_block: i64,
    pub num_of_rgs: i64,
    pub total_memory_in_use_in_percent: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub iops: i64,
    pub last_upgrade_attempt_version: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency_64kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency_512kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency_8kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_32kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_num_of_small_rd: i64,
    pub num_of_nodes: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_by_block: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency_512b: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_1kb: i64,
    pub iscsi_port_speed: String,
    pub memory_recovery_status: String,
    pub debug_create_timeout: String,
    pub num_of_minor_alerts: i64,
    pub gates_open: Option<bool>,
    pub compression_factor: f64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub unaligned_rd_iops: i64,
    pub shared_memory_in_use_recoverable_ratio_level: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency_2kb: i64,
    pub obj_severity: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_2kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_8kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency_16kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_64kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency_256kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency_512kb: i64,
    pub tag_list: Vec<Value>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_128kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub unaligned_rd_bw: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops: i64,
    pub sys_start_timestamp: i64,
    pub cluster_expansion_in_progress: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_gt1mb: i64,
    pub num_of_ib_switches: i64,
    pub num_of_tars: i64,
    pub name: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_num_of_unaligned_wr: i64,
    pub dedup_ratio_text: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_4kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_16kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency_1mb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_size_of_rd: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency_4kb: i64,
    pub dedup_ratio: f64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency_1mb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_8kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency_512b: i64,
    pub brick_list: Vec<Value>,
    pub sys_sw_version: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency_16kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_512b: i64,
    pub max_data_transfer_percent_done: i64,
    pub fc_port_speed: String,
    pub shared_memory: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_num_of_wr: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency_2kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_128kb: i64,
    pub free_ud_ssd_space_in_percent: i64,
    pub index: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_256kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency_gt1mb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency_8kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency_256kb: i64,
    pub upgrade_failure_reason: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_1kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_gt1mb: i64,
    pub last_upgrade_attempt_timestamp: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency_256kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency_32kb: i64,
    pub num_of_xenvs: i64,
    pub sys_stop_type: String,
    pub stopped_reason: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_32kb: i64,
    pub configurable_vol_type_capability: String,
    pub xms_id: Vec<Value>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_gt1mb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency_gt1mb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub small_wr_bw: i64,
    pub num_of_ssds: i64,
    pub mode_switch_status: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_512b: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub bw: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency_64kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_512kb: i64,
    pub send_snmp_heartbeat: Option<bool>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency: i64,
    pub total_memory_in_use: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_128kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency_1kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub unaligned_bw: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub ud_ssd_space_in_use: i64,
    pub num_of_jbods: i64,
    pub license_id: String,
    pub sys_health_state: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency_8kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_128kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub unaligned_wr_iops: i64,
    pub data_reduction_ratio_text: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_1kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_32kb: i64,
    pub obfuscate_debug: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency: i64,
    pub psnt_part_number: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub small_iops: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_16kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency_1kb: i64,
    pub odx_mode: Option<String>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_by_block: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub ud_ssd_space: i64,
    pub num_of_vols: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_512b: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_512b: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_num_of_small_wr: i64,
    pub guid: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub useful_ssd_space_per_ssd: i64,
    pub space_saving_ratio: f64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_num_of_rd: i64,
    pub data_reduction_ratio: f64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_1mb: i64,
    pub ssd_very_high_utilization_thld_crossing: String,
    pub vaai_tp_limit_crossing: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_size_of_wr: i64,
    pub shared_memory_in_use_ratio_level: String,
    pub num_of_internal_vols: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_2kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_by_block: i64,
    pub under_maintenance: bool,
    pub chap_authentication_mode: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub bw_by_block: i64,
    pub num_of_tgs: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub small_wr_iops: i64,
    pub chap_discovery_mode: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_1mb: i64,
    pub device_connectivity_mode: Option<String>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_16kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub acc_num_of_unaligned_rd: i64,
    pub iscsi_tcp_port: Option<i64>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_2kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_8kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_512kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub dedup_space_in_use: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency_128kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency_512kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_64kb: i64,
    pub sys_id: Vec<Value>,
    pub size_and_capacity: String,
    pub is_any_c_mdl_lazy_load_in_progress: bool,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_1mb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency_4kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency_4kb: i64,
    pub sys_activation_timestamp: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency_512b: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_256kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub unaligned_wr_bw: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_4kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub small_rd_bw: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency_64kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_32kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_256kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency_2kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency_128kb: i64,
    pub ssd_high_utilization_thld_crossing: String,
    pub compression_mode: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_16kb: i64,
    pub sc_fp_temperature_monitor_mode: String,
    pub is_any_d_mdl_lazy_load_in_progress: bool,
    pub upgrade_state: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_512kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency_128kb: i64,
    pub space_in_use: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub logical_space_in_use: i64,
    pub vaai_tp_limit: Option<i64>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_1mb: i64,
    pub ib_switch_list: Vec<Value>,
    pub sys_psnt_serial_number: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_1kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_2kb: i64,
    pub encryption_mode: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_8kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency_gt1mb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_256kb: i64,
    pub thin_provisioning_ratio: f64,
    pub sys_mgr_conn_error_reason: String,
    pub num_of_upses: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency_1mb: i64,
    pub num_of_major_alerts: i64,
    pub num_of_initiators: i64,
    pub sys_mgr_conn_state: String,
    pub naa_sys_id: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_4kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub unaligned_iops: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_gt1mb: i64,
    pub encryption_supported: bool,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub small_rd_iops: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub vol_size: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_512kb: i64,
    pub thin_provisioning_savings: Option<f64>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_4kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops: i64,
    pub num_of_bricks: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency_32kb: i64,
    pub max_num_of_ssds_per_rg: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency_32kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub iops_by_block: i64,
    pub mode_switch_new_mode: String,
    pub sys_state: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency_16kb: i64,
    pub consistency_state: String,
    pub num_of_critical_alerts: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency_1kb: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub small_bw: i64,
}

#[test]
fn test_get_xtremio_xmss() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/xtremio/xmss.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: Xmss = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[derive(Deserialize, Debug)]
pub struct Xmss {
    pub xmss: Vec<Xms>,
    pub params: HashMap<String, String>,
    pub links: Vec<Link>,
}

impl IntoPoint for Xmss {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        let n = name.unwrap_or("xms");

        for xms in &self.xmss {
            points.extend(xms.into_point(Some(n), is_time_series));
        }

        points
    }
}

#[derive(Deserialize, Debug, IntoPoint)]
#[serde(rename_all = "kebab-case")]
pub struct Xms {
    pub max_repeating_alert: Option<i64>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_latency: i64,
    pub max_recs_in_event_log: i64,
    pub top_n_igs_by_iops: Vec<Value>,
    pub xms_gw: String,
    pub datetime: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw_by_block: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub iops: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub logs_size: i64,
    pub guid: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops_by_block: i64,
    pub index: i64,
    pub uptime: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw_by_block: i64,
    pub db_version: String,
    pub server_name: Option<String>,
    pub num_of_iscsi_routes: i64,
    pub allow_empty_password: bool,
    pub ip_version: String,
    pub name: String,
    pub xms_num_of_volumes_level: Option<String>,
    pub top_n_volumes_by_latency: Vec<Value>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops_by_block: i64,
    pub version: String,
    pub obj_severity: String,
    pub top_n_igs_by_bw: Vec<Value>,
    pub max_xms_tags_per_object: Option<i64>,
    pub disk_space_secondary_utilization_level: String,
    #[serde(deserialize_with = "deserialize_string_or_float")]
    pub overall_efficiency_ratio: f64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub bw_by_block: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub build: i64,
    pub num_of_igs: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_bw: i64,
    pub xms_id: Vec<Value>,
    pub max_xms_clusters: Option<i64>,
    pub sw_version: String,
    pub num_of_systems: i64,
    pub recs_in_event_log: i64,
    pub num_of_user_accounts: i64,
    pub disk_space_utilization_level: String,
    pub default_user_inactivity_timeout: i64,
    pub restapi_protocol_version: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_iops: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub thin_provisioning_savings: i64,
    pub memory_utilization_level: String,
    pub xms_ip_sn: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub avg_latency: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub ram_usage: i64,
    pub xms_ip: String,
    pub max_xms_objects_per_tag: Option<i64>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_latency: i64,
    pub days_in_num_event: i64,
    pub wrong_cn_in_csr: bool,
    pub mgmt_interface: String,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub iops_by_block: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub bw: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub ram_total: i64,
    pub mode: String,
    pub ntp_servers: Vec<Value>,
    pub max_xms_volumes: Option<i64>,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub rd_iops: i64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub wr_bw: i64,
    #[serde(deserialize_with = "deserialize_string_or_float")]
    pub cpu: f64,
}

impl XtremIo {
    fn get_data<T>(&self, api_endpoint: &str, point_name: &str) -> MetricsResult<Vec<TsPoint>>
    where
        T: DeserializeOwned + Debug + IntoPoint,
    {
        let url = format!(
            "https://{}/api/json/v2/types/{}?full=1",
            self.config.endpoint, api_endpoint,
        );
        let j: T = crate::get(
            &self.client,
            &url,
            &self.config.user,
            Some(&self.config.password),
        )?;

        Ok(j.into_point(Some(point_name), true))
    }

    pub fn get_clusters(&self) -> MetricsResult<Vec<TsPoint>> {
        let points = self.get_data::<Clusters>("clusters", "cluster")?;
        Ok(points)
    }

    pub fn get_psus(&self) -> MetricsResult<Vec<TsPoint>> {
        let points = self.get_data::<Psus>("storage-controller-psus", "psu")?;
        Ok(points)
    }
    pub fn get_ssds(&self) -> MetricsResult<Vec<TsPoint>> {
        let points = self.get_data::<Ssds>("ssds", "ssd")?;
        Ok(points)
    }

    pub fn get_xms(&self) -> MetricsResult<Vec<TsPoint>> {
        let points = self.get_data::<Xmss>("xms", "xms")?;
        Ok(points)
    }

    pub fn get_volumes(&self) -> MetricsResult<Vec<TsPoint>> {
        let points = self.get_data::<Volumes>("volumes", "volume")?;
        Ok(points)
    }
}
