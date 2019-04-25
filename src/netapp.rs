//! Netapp uses a series of XML request/response queries to interact with the server.
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
use std::io::Write;
use std::str::FromStr;

use crate::error::*;
use crate::IntoPoint;

use crate::ir::{TsPoint, TsValue};
use chrono::offset::Utc;
use chrono::DateTime;
use log::debug;
use reqwest::header::{HeaderValue, CONTENT_TYPE};
use reqwest::Client;
use treexml::Document;
use uuid::Uuid;
use xml::writer::{EventWriter, XmlEvent};

//static AGENT_URL: &str = "/apis/XMLrequest";
//static DFM_URL: &str = "/apis/XMLrequest";
static FILER_URL: &str = "servlets/netapp.servlets.admin.XMLrequest_filer";
//static NETCACHE_URL: &str = "/servlets/netapp.servlets.admin.XMLrequest";
//static ZAPI_xmlns: &str = "http://www.netapp.com/filer/admin";

#[derive(Clone, Deserialize, Debug)]
pub struct NetappConfig {
    /// The netapp endpoint to use
    pub endpoint: String,
    pub user: String,
    /// This gets replaced with the token at runtime
    pub password: String,
    /// The region this cluster is located in
    pub region: String,
    /// Optional certificate file to use against the server
    /// der encoded
    pub certificate: Option<String>,
}

pub struct Netapp {
    client: reqwest::Client,
    config: NetappConfig,
}

impl Netapp {
    pub fn new(client: &reqwest::Client, config: NetappConfig) -> Self {
        Netapp {
            client: client.clone(),
            config,
        }
    }
}

pub trait FromXml {
    fn from_xml(data: &str) -> MetricsResult<Self>
    where
        Self: Sized;
}

// Recursive Decent
fn get_str_key(e: &treexml::Element, tag: &str) -> Option<String> {
    if e.children.is_empty() {
        return None;
    }
    for child in &e.children {
        if child.name == tag {
            return child.clone().text;
        }
        if !child.children.is_empty() {
            match get_str_key(child, tag) {
                None => {}
                Some(s) => return Some(s),
            }
        }
    }
    None
}

// Note: this can't recurse.  I don't know how to work that out
fn get_key<T>(e: &treexml::Element, tag: &str) -> MetricsResult<T>
where
    T: FromStr,
{
    if e.children.is_empty() {
        return Err(StorageError::new(format!("{} not found", tag)));
    }
    for child in &e.children {
        if child.name == tag {
            let res = T::from_str(&child.clone().text.unwrap_or_else(|| "".to_string()));
            match res {
                Ok(val) => return Ok(val),
                Err(_) => return Err(StorageError::new(format!("parsing {} failed", child.name))),
            }
        }
    }
    Err(StorageError::new(format!("{} not found", tag)))
}

fn check_failure(e: &treexml::Element) -> MetricsResult<()> {
    if let Some(s) = e.attributes.get("status") {
        if s == "failed" {
            return Err(StorageError::new(format!(
                "netapp query failed: {}",
                &e.attributes["reason"]
            )));
        }
    }

    Ok(())
}

#[test]
fn test_netapp_perf_list() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/netapp/ha_interconnect_perf_stats.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = HaPerformanceStats::from_xml(&data).unwrap();
    println!("res: {:#?}", res);
}

#[derive(Debug)]
pub struct HaPerformanceStats {
    pub perf: Vec<PerformanceStat>,
}

#[derive(Debug, IntoPoint)]
pub struct PerformanceStat {
    pub average_bytes_per_transfer: u64,
    pub average_megabytes_per_second: f64,
    pub average_remote_nv_msgs_time: u64,
    pub average_remote_nv_transfer_size: u64,
    pub average_remote_nv_transfer_time: u64,
    pub avg_misc_queue_length: u64,
    pub avg_nvlog_sync_time: u64,
    pub avg_raid_queue_length: u64,
    pub avg_wafl_queue_length: u64,
    pub elapsed_time: u64,
    pub ic_16k_writes: u64,
    pub ic_4k_writes: u64,
    pub ic_8k_writes: u64,
    pub ic_data_aligned: u64,
    pub ic_data_misaligned: u64,
    pub ic_discontiguous_writes: u64,
    pub ic_isdone: u64,
    pub ic_isdone_fail: u64,
    pub ic_isdone_pass: u64,
    pub ic_metadata_aligned: u64,
    pub ic_metadata_misaligned: u64,
    pub ic_small_writes: u64,
    pub ic_waitdone_time: u64,
    pub ic_waits: u64,
    pub ic_xorder_reads: u64,
    pub ic_xorder_writes: u64,
    pub max_nvlog_sync_time: u64,
    pub max_sgl_length: u64,
    pub misc_data_io: u64,
    pub misc_metadata_io: u64,
    pub node_name: String,
    pub nv_conn_failover_time: u64,
    pub queue_max_wait_count: u64,
    pub queue_max_wait_time: u64,
    pub raid_data_io: u64,
    pub raid_metadata_io: u64,
    pub rdma_read: u64,
    pub rdma_read_waitdone_time: u64,
    pub remote_nv_transfers: u64,
    pub total_receive_queue_waits: u64,
    pub total_transfers: u64,
    pub wafl_data_io: u64,
    pub wafl_metadata_io: u64,
}

impl FromXml for HaPerformanceStats {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        let doc = Document::parse(data.as_bytes())?;
        let root = doc
            .root
            .ok_or_else(|| StorageError::new(format!("root xml not found for {}", data)))?;
        let results = root
            .find_child(|tag| tag.name == "results")
            .ok_or_else(|| StorageError::new(format!("results tag not found in {:?}", root)))?;
        check_failure(&results)?;

        let attribute_list = results
            .find_child(|tag| tag.name == "attributes-list")
            .ok_or_else(|| StorageError::new(format!("results tag not found in {:?}", root)))?;
        let mut perf_stats: Vec<PerformanceStat> = Vec::new();
        for perf_child in &attribute_list.children {
            perf_stats.push(PerformanceStat {
                average_bytes_per_transfer: get_key::<u64>(
                    &perf_child,
                    "average-bytes-per-transfer",
                )?,
                average_megabytes_per_second: get_key::<f64>(
                    &perf_child,
                    "average-megabytes-per-second",
                )?,
                average_remote_nv_msgs_time: get_key::<u64>(
                    &perf_child,
                    "average-remote-nv-msgs-time",
                )?,
                average_remote_nv_transfer_size: get_key::<u64>(
                    &perf_child,
                    "average-remote-nv-transfer-size",
                )?,
                average_remote_nv_transfer_time: get_key::<u64>(
                    &perf_child,
                    "average-remote-nv-transfer-time",
                )?,
                avg_misc_queue_length: get_key::<u64>(&perf_child, "avg-misc-queue-length")?,
                avg_nvlog_sync_time: get_key::<u64>(&perf_child, "avg-nvlog-sync-time")?,
                avg_raid_queue_length: get_key::<u64>(&perf_child, "avg-raid-queue-length")?,
                avg_wafl_queue_length: get_key::<u64>(&perf_child, "avg-wafl-queue-length")?,
                elapsed_time: get_key::<u64>(&perf_child, "elapsed-time")?,
                ic_16k_writes: get_key::<u64>(&perf_child, "ic-16k-writes")?,
                ic_4k_writes: get_key::<u64>(&perf_child, "ic-4k-writes")?,
                ic_8k_writes: get_key::<u64>(&perf_child, "ic-8k-writes")?,
                ic_data_aligned: get_key::<u64>(&perf_child, "ic-data-aligned")?,
                ic_data_misaligned: get_key::<u64>(&perf_child, "ic-data-misaligned")?,
                ic_discontiguous_writes: get_key::<u64>(&perf_child, "ic-discontiguous-writes")?,
                ic_isdone: get_key::<u64>(&perf_child, "ic-isdone")?,
                ic_isdone_fail: get_key::<u64>(&perf_child, "ic-isdone-fail")?,
                ic_isdone_pass: get_key::<u64>(&perf_child, "ic-isdone-pass")?,
                ic_metadata_aligned: get_key::<u64>(&perf_child, "ic-metadata-aligned")?,
                ic_metadata_misaligned: get_key::<u64>(&perf_child, "ic-metadata-misaligned")?,
                ic_small_writes: get_key::<u64>(&perf_child, "ic-small-writes")?,
                ic_waitdone_time: get_key::<u64>(&perf_child, "ic-waitdone-time")?,
                ic_waits: get_key::<u64>(&perf_child, "ic-waits")?,
                ic_xorder_reads: get_key::<u64>(&perf_child, "ic-xorder-reads")?,
                ic_xorder_writes: get_key::<u64>(&perf_child, "ic-xorder-writes")?,
                max_nvlog_sync_time: get_key::<u64>(&perf_child, "max-nvlog-sync-time")?,
                max_sgl_length: get_key::<u64>(&perf_child, "max-sgl-length")?,
                misc_data_io: get_key::<u64>(&perf_child, "misc-data-io")?,
                misc_metadata_io: get_key::<u64>(&perf_child, "misc-metadata-io")?,
                node_name: get_str_key(&perf_child, "node-name").unwrap_or_else(|| "".to_string()),
                nv_conn_failover_time: get_key::<u64>(&perf_child, "nv-conn-failover-time")?,
                queue_max_wait_count: get_key::<u64>(&perf_child, "queue-max-wait-count")?,
                queue_max_wait_time: get_key::<u64>(&perf_child, "queue-max-wait-time")?,
                raid_data_io: get_key::<u64>(&perf_child, "raid-data-io")?,
                raid_metadata_io: get_key::<u64>(&perf_child, "raid-metadata-io")?,
                rdma_read: get_key::<u64>(&perf_child, "rdma-read")?,
                rdma_read_waitdone_time: get_key::<u64>(&perf_child, "rdma-read-waitdone-time")?,
                remote_nv_transfers: get_key::<u64>(&perf_child, "remote-nv-transfers")?,
                total_receive_queue_waits: get_key::<u64>(
                    &perf_child,
                    "total-receive-queue-waits",
                )?,
                total_transfers: get_key::<u64>(&perf_child, "total-transfers")?,
                wafl_data_io: get_key::<u64>(&perf_child, "wafl-data-io")?,
                wafl_metadata_io: get_key::<u64>(&perf_child, "wafl-metadata-io")?,
            })
        }

        Ok(HaPerformanceStats { perf: perf_stats })
    }
}

#[test]
fn test_netapp_vol_list() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/netapp/volume_list.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = NetappVolumes::from_xml(&data).unwrap();
    println!("res: {:#?}", res);
}

#[derive(Debug)]
pub struct NetappVolumes {
    pub vols: Vec<NetappVolume>,
}

impl IntoPoint for NetappVolumes {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        for vol in &self.vols {
            points.extend(vol.into_point(name, is_time_series));
        }
        points
    }
}

#[derive(Debug, IntoPoint)]
pub struct NetappVolume {
    pub encrypted: bool,
    pub grow_threshold_percent: u8,
    pub autosize_is_enabled: bool,
    pub autosize_maximum_size: u64,
    pub autosize_minimum_size: u64,
    pub autosize_mode: String,
    pub autosize_shrink_threshold_percent: u8,
    pub volume_id_aggr_list: Vec<String>,
    pub containing_aggregate_name: String,
    pub containing_aggregate_uuid: Uuid,
    pub creation_time: u64,
    pub fsid: u64,
    pub instance_uuid: Uuid,
    pub name: String,
    pub name_ordinal: String,
    pub node: String,
    pub node_list: Vec<String>,
    pub owning_vserver_name: String,
    pub owning_vserver_uuid: Uuid,
    pub provenance_uuid: Uuid,
    pub volume_style: String, // example: flex
    pub volume_type: String,  // example: rw
    pub volume_uuid: Uuid,

    pub inode_block_type: String,
    pub inode_files_private_used: u64,
    pub inode_files_total: u64,
    pub inode_files_used: u64,
    pub inodefile_private_capacity: u64,
    pub inodefile_public_capacity: u64,
    pub inofile_version: u64,

    pub compression_space_saved: u64,
    pub deduplication_space_saved: u64,
    pub deduplication_space_shared: u64,
    pub is_sis_logging_enabled: bool,
    pub is_sis_state_enabled: bool,
    pub is_sis_volume: bool,
    pub percentage_compression_space_saved: u8,
    pub percentage_deduplication_space_saved: u8,
    pub percentage_total_space_saved: u8,
    pub total_space_saved: u64,

    pub filesystem_size: u64,
    pub is_filesys_size_fixed: bool,
    pub is_space_guarantee_enabled: bool,
    pub is_space_slo_enabled: bool,
    pub overwrite_reserve: u64,
    pub overwrite_reserve_required: u64,
    pub overwrite_reserve_used: u8,
    pub overwrite_reserve_used_actual: u8,
    pub percentage_fractional_reserve: u8,
    pub percentage_size_used: u8,
    pub percentage_snapshot_reserve: u8,
    pub percentage_snapshot_reserve_used: u8,
    pub physical_used: u64,
    pub physical_used_percent: u8,
    pub size: u64,
    pub size_available: u64,
    pub size_available_for_snapshots: u64,
    pub size_total: u64,
    pub size_used: u64,
    pub size_used_by_snapshots: u64,
    pub snapshot_reserve_size: u64,
    pub space_full_threshold_percent: u8,
    pub space_guarantee: String,
    pub space_mgmt_option_try_first: String,
    pub space_nearly_full_threshold_percent: u8,
    pub space_slo: String,

    pub become_node_root_after_reboot: bool,
    pub force_nvfail_on_dr: bool,
    pub ignore_inconsistent: bool,
    pub in_nvfailed_state: bool,
    pub is_cluster_volume: bool,
    pub is_constituent: bool,
    pub is_flexgroup: bool,
    pub is_inconsistent: bool,
    pub is_invalid: bool,
    pub is_node_root: bool,
    pub is_nvfail_enabled: bool,
    pub is_quiesced_in_memory: bool,
    pub is_quiesced_on_disk: bool,
    pub is_unrecoverable: bool,
    pub state: String,
}

impl FromXml for NetappVolumes {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        let doc = Document::parse(data.as_bytes())?;
        let root = doc
            .root
            .ok_or_else(|| StorageError::new(format!("root xml not found for {}", data)))?;
        let results = root
            .find_child(|tag| tag.name == "results")
            .ok_or_else(|| StorageError::new(format!("results tag not found in {:?}", root)))?;
        check_failure(&results)?;

        let vols = results
            .find_child(|tag| tag.name == "attributes-list")
            .ok_or_else(|| StorageError::new(format!("results tag not found in {:?}", root)))?;
        let mut volumes: Vec<NetappVolume> = Vec::new();
        for vol_child in &vols.children {
            let autosize_attributes = vol_child
                .find_child(|tag| tag.name == "volume-autosize-attributes")
                .ok_or_else(|| {
                    StorageError::new(format!(
                        "volume-autosize-attributes tag not found in {:?}",
                        vol_child
                    ))
                })?;
            let id_attributes = vol_child
                .find_child(|tag| tag.name == "volume-id-attributes")
                .ok_or_else(|| {
                    StorageError::new(format!(
                        "volume-id-attributes tag not found in {:?}",
                        vol_child
                    ))
                })?;
            let inode_attributes = vol_child
                .find_child(|tag| tag.name == "volume-inode-attributes")
                .ok_or_else(|| {
                    StorageError::new(format!(
                        "volume-inode-attributes tag not found in {:?}",
                        vol_child
                    ))
                })?;
            let sis_attributes = vol_child
                .find_child(|tag| tag.name == "volume-sis-attributes")
                .ok_or_else(|| {
                    StorageError::new(format!(
                        "volume-sis-attributes tag not found in {:?}",
                        vol_child
                    ))
                })?;
            let space_attributes = vol_child
                .find_child(|tag| tag.name == "volume-space-attributes")
                .ok_or_else(|| {
                    StorageError::new(format!(
                        "volume-space-attributes tag not found in {:?}",
                        vol_child
                    ))
                })?;
            let state_attributes = vol_child
                .find_child(|tag| tag.name == "volume-state-attributes")
                .ok_or_else(|| {
                    StorageError::new(format!(
                        "volume-state-attributes tag not found in {:?}",
                        vol_child
                    ))
                })?;
            let aggr_list: Option<Vec<String>> =
                // Find all the <aggr-list> xml elements
                match id_attributes.find_child(|tag| tag.name == "aggr-list") {
                    Some(aggr) => {
                        let child_aggrs = aggr
                            // look through the children
                            .children
                            .iter()
                            // grab their text
                            .map(|elem| elem.text.clone())
                            // filter out any that are not Some
                            .filter(|elem| elem.is_some())
                            // unwrap
                            .map(|elem| elem.unwrap())
                            .collect();
                        Some(child_aggrs)
                    }
                    None => None,
                };

            // Find all the <node> xml elements
            let node_list: Option<Vec<String>> =
                match id_attributes.find_child(|tag| tag.name == "nodes") {
                    Some(nodes) => {
                        let child_nodes = nodes
                            // look through the children
                            .children
                            .iter()
                            // grab their text
                            .map(|elem| elem.text.clone())
                            // filter out any that are not Some
                            .filter(|elem| elem.is_some())
                            // unwrap
                            .map(|elem| elem.unwrap())
                            .collect();
                        Some(child_nodes)
                    }
                    None => None,
                };
            volumes.push(NetappVolume {
                encrypted: get_key::<bool>(&vol_child, "encrypt")?,
                grow_threshold_percent: get_key::<u8>(
                    &autosize_attributes,
                    "grow-threshold-percent",
                )?,
                autosize_is_enabled: get_key::<bool>(&autosize_attributes, "is-enabled")?,
                autosize_maximum_size: get_key::<u64>(&autosize_attributes, "maximum-size")?,
                autosize_minimum_size: get_key::<u64>(&autosize_attributes, "minimum-size")?,
                autosize_mode: get_str_key(&autosize_attributes, "mode")
                    .unwrap_or_else(|| "".to_string()),
                autosize_shrink_threshold_percent: get_key::<u8>(
                    &autosize_attributes,
                    "shrink-threshold-percent",
                )?,
                volume_id_aggr_list: aggr_list.unwrap_or_else(|| vec![]),
                containing_aggregate_name: get_str_key(&id_attributes, "containing-aggregate-name")
                    .unwrap_or_else(|| "".to_string()),
                containing_aggregate_uuid: get_key::<Uuid>(
                    &id_attributes,
                    "containing-aggregate-uuid",
                )?,
                creation_time: get_key::<u64>(&id_attributes, "creation-time")?,
                fsid: get_key::<u64>(&id_attributes, "fsid")?,
                instance_uuid: get_key::<Uuid>(&id_attributes, "instance-uuid")?,
                name: get_str_key(&id_attributes, "name").unwrap_or_else(|| "".to_string()),
                name_ordinal: get_str_key(&id_attributes, "name-ordinal")
                    .unwrap_or_else(|| "".to_string()),
                node: get_str_key(&id_attributes, "node").unwrap_or_else(|| "".to_string()),
                node_list: node_list.unwrap_or_else(|| vec![]),
                owning_vserver_name: get_str_key(&id_attributes, "owning-vserver-name")
                    .unwrap_or_else(|| "".to_string()),
                owning_vserver_uuid: get_key::<Uuid>(&id_attributes, "owning-vserver-uuid")?,
                provenance_uuid: get_key::<Uuid>(&id_attributes, "provenance-uuid")?,
                volume_style: get_str_key(&id_attributes, "style")
                    .unwrap_or_else(|| "".to_string()), // example: flex
                volume_type: get_key(&id_attributes, "type")?, // example: rw
                volume_uuid: get_key::<Uuid>(&id_attributes, "uuid")?,

                inode_block_type: get_str_key(&inode_attributes, "block-type")
                    .unwrap_or_else(|| "".to_string()),
                inode_files_private_used: get_key::<u64>(&inode_attributes, "files-private-used")?,
                inode_files_total: get_key::<u64>(&inode_attributes, "files-total")?,
                inode_files_used: get_key::<u64>(&inode_attributes, "files-used")?,
                inodefile_private_capacity: get_key::<u64>(
                    &inode_attributes,
                    "inodefile-private-capacity",
                )?,
                inodefile_public_capacity: get_key::<u64>(
                    &inode_attributes,
                    "inodefile-public-capacity",
                )?,
                inofile_version: get_key::<u64>(&inode_attributes, "inofile-version")?,

                compression_space_saved: get_key::<u64>(
                    &sis_attributes,
                    "compression-space-saved",
                )?,
                deduplication_space_saved: get_key::<u64>(
                    &sis_attributes,
                    "deduplication-space-saved",
                )?,
                deduplication_space_shared: get_key::<u64>(
                    &sis_attributes,
                    "deduplication-space-shared",
                )?,
                is_sis_logging_enabled: get_key::<bool>(&sis_attributes, "is-sis-logging-enabled")?,
                is_sis_state_enabled: get_key::<bool>(&sis_attributes, "is-sis-state-enabled")?,
                is_sis_volume: get_key::<bool>(&sis_attributes, "is-sis-volume")?,
                percentage_compression_space_saved: get_key::<u8>(
                    &sis_attributes,
                    "percentage-compression-space-saved",
                )?,
                percentage_deduplication_space_saved: get_key::<u8>(
                    &sis_attributes,
                    "percentage-deduplication-space-saved",
                )?,
                percentage_total_space_saved: get_key::<u8>(
                    &sis_attributes,
                    "percentage-total-space-saved",
                )?,
                total_space_saved: get_key::<u64>(&sis_attributes, "total-space-saved")?,

                filesystem_size: get_key::<u64>(&space_attributes, "filesystem-size")?,
                is_filesys_size_fixed: get_key::<bool>(&space_attributes, "is-filesys-size-fixed")?,
                is_space_guarantee_enabled: get_key::<bool>(
                    &space_attributes,
                    "is-space-guarantee-enabled",
                )?,
                is_space_slo_enabled: get_key::<bool>(&space_attributes, "is-space-slo-enabled")?,
                overwrite_reserve: get_key::<u64>(&space_attributes, "overwrite-reserve")?,
                overwrite_reserve_required: get_key::<u64>(
                    &space_attributes,
                    "overwrite-reserve-required",
                )?,
                overwrite_reserve_used: get_key::<u8>(&space_attributes, "overwrite-reserve-used")?,
                overwrite_reserve_used_actual: get_key::<u8>(
                    &space_attributes,
                    "overwrite-reserve-used-actual",
                )?,
                percentage_fractional_reserve: get_key::<u8>(
                    &space_attributes,
                    "percentage-fractional-reserve",
                )?,
                percentage_size_used: get_key::<u8>(&space_attributes, "percentage-size-used")?,
                percentage_snapshot_reserve: get_key::<u8>(
                    &space_attributes,
                    "percentage-snapshot-reserve",
                )?,
                percentage_snapshot_reserve_used: get_key::<u8>(
                    &space_attributes,
                    "percentage-snapshot-reserve-used",
                )?,
                physical_used: get_key::<u64>(&space_attributes, "physical-used")?,
                physical_used_percent: get_key::<u8>(&space_attributes, "physical-used-percent")?,
                size: get_key::<u64>(&space_attributes, "size")?,
                size_available: get_key::<u64>(&space_attributes, "size-available")?,
                size_available_for_snapshots: get_key::<u64>(
                    &space_attributes,
                    "size-available-for-snapshots",
                )?,
                size_total: get_key::<u64>(&space_attributes, "size-total")?,
                size_used: get_key::<u64>(&space_attributes, "size-used")?,
                size_used_by_snapshots: get_key::<u64>(
                    &space_attributes,
                    "size-used-by-snapshots",
                )?,
                snapshot_reserve_size: get_key::<u64>(&space_attributes, "snapshot-reserve-size")?,
                space_full_threshold_percent: get_key::<u8>(
                    &space_attributes,
                    "space-full-threshold-percent",
                )?,
                space_guarantee: get_str_key(&space_attributes, "space-guarantee")
                    .unwrap_or_else(|| "".to_string()),
                space_mgmt_option_try_first: get_str_key(
                    &space_attributes,
                    "space-mgmt-option-try-first",
                )
                .unwrap_or_else(|| "".to_string()),
                space_nearly_full_threshold_percent: get_key::<u8>(
                    &space_attributes,
                    "space-nearly-full-threshold-percent",
                )?,
                space_slo: get_str_key(&space_attributes, "space-slo")
                    .unwrap_or_else(|| "".to_string()),

                become_node_root_after_reboot: get_key::<bool>(
                    &state_attributes,
                    "become-node-root-after-reboot",
                )?,
                force_nvfail_on_dr: get_key::<bool>(&state_attributes, "force-nvfail-on-dr")?,
                ignore_inconsistent: get_key::<bool>(&state_attributes, "ignore-inconsistent")?,
                in_nvfailed_state: get_key::<bool>(&state_attributes, "in-nvfailed-state")?,
                is_cluster_volume: get_key::<bool>(&state_attributes, "is-cluster-volume")?,
                is_constituent: get_key::<bool>(&state_attributes, "is-constituent")?,
                is_flexgroup: get_key::<bool>(&state_attributes, "is-flexgroup")?,
                is_inconsistent: get_key::<bool>(&state_attributes, "is-inconsistent")?,
                is_invalid: get_key::<bool>(&state_attributes, "is-invalid")?,
                is_node_root: get_key::<bool>(&state_attributes, "is-node-root")?,
                is_nvfail_enabled: get_key::<bool>(&state_attributes, "is-nvfail-enabled")?,
                is_quiesced_in_memory: get_key::<bool>(&state_attributes, "is-quiesced-in-memory")?,
                is_quiesced_on_disk: get_key::<bool>(&state_attributes, "is-quiesced-on-disk")?,
                is_unrecoverable: get_key::<bool>(&state_attributes, "is-unrecoverable")?,
                state: get_str_key(&state_attributes, "state").unwrap_or_else(|| "".to_string()),
            });
        }

        Ok(NetappVolumes { vols: volumes })
    }
}

#[derive(Debug, IntoPoint)]
pub struct OnTapVersion {
    pub build_timestamp: u64,
    pub is_clustered: bool,
    pub version: String,
    pub generation: u64,
    pub major: u64,
    pub minor: u64,
}

impl FromXml for OnTapVersion {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        debug!("parsing ontap data: {}", data);

        let doc = Document::parse(data.as_bytes())?;
        let root = doc
            .root
            .ok_or_else(|| StorageError::new(format!("root xml not found for {}", data)))?;
        let results = root
            .find_child(|tag| tag.name == "results")
            .ok_or_else(|| StorageError::new(format!("results tag not found in {:?}", root)))?;
        check_failure(&results)?;

        let is_clustered = get_str_key(&root, "is-clustered");
        let build_timestamp = get_str_key(&root, "build-timestamp");
        let version = get_str_key(&root, "version");
        let generation = get_str_key(&root, "generation");
        let major = get_str_key(&root, "major-version");
        let minor = get_str_key(&root, "minor-version");

        Ok(OnTapVersion {
            build_timestamp: u64::from_str(&build_timestamp.unwrap_or_else(|| "0".to_string()))?,
            is_clustered: bool::from_str(&is_clustered.unwrap_or_else(|| "false".to_string()))?,
            version: version.unwrap_or_else(|| "".to_string()),
            generation: u64::from_str(&generation.unwrap_or_else(|| "0".to_string()))?,
            major: u64::from_str(&major.unwrap_or_else(|| "0".to_string()))?,
            minor: u64::from_str(&minor.unwrap_or_else(|| "0".to_string()))?,
        })
    }
}

fn api_request<T>(client: &Client, config: &NetappConfig, req: Vec<u8>) -> MetricsResult<T>
where
    T: FromXml,
{
    debug!("Sending: {}", String::from_utf8_lossy(&req));
    let mut s = client
        .post(&format!("http://{}/{}", config.endpoint, FILER_URL))
        .basic_auth(config.user.clone(), Some(config.password.clone()))
        .body(req)
        .header(CONTENT_TYPE, HeaderValue::from_str("application/xml")?)
        .send()?
        .error_for_status()?;

    let data = s.text()?;
    debug!("api_request response: {}", data);
    let res = T::from_xml(&data)?;

    Ok(res)
}

#[test]
fn test_netapp_request() {
    let mut output: Vec<u8> = Vec::new();
    {
        let mut writer = EventWriter::new(&mut output);
        create_version_request(&mut writer).unwrap();
    }
    println!("request {}", String::from_utf8(output.clone()).unwrap());
}

#[test]
fn test_version_parser() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/netapp/version.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = OnTapVersion::from_xml(&data).unwrap();
    println!("result: {:#?}", res);
}

fn start_request<W: Write>(w: &mut EventWriter<W>) -> MetricsResult<()> {
    // Get information about the kernel to tell netapp about.  I don't know
    // why it needs this
    let sys_info = uname::uname()?;

    w.write(XmlEvent::start_element(
        "!DOCTYPE netapp SYSTEM 'file:/etc/netapp_filer.dtd'",
    ))?;

    let platform = format!("{} {}", sys_info.sysname, sys_info.machine);

    let e = XmlEvent::start_element("netapp")
        .default_ns("http://www.netapp.com/filer/admin")
        .attr("version", "1.0")
        .attr("nmsdk_version", "9.4")
        .attr("nmsdk_platform", &platform)
        .attr("nmsdk_language", "rust")
        .attr("nmsdk_app", "ZEDI");

    w.write(e)?;
    Ok(())
}

#[test]
fn test_netapp_create_volume() {
    let mut output: Vec<u8> = Vec::new();
    {
        let mut writer = EventWriter::new(&mut output);
        create_volume_request(&mut writer).unwrap();
    }
    println!("request {}", String::from_utf8(output.clone()).unwrap());
}

fn create_peformance_request<W: Write>(w: &mut EventWriter<W>) -> MetricsResult<()> {
    start_request(w)?;
    start_element(w, "ha-interconnect-performance-statistics-get-iter", None)?;
    end_element(w, "ha-interconnect-performance-statistics-get-iter")?;
    end_element(w, "netapp")?;

    Ok(())
}

fn create_volume_request<W: Write>(w: &mut EventWriter<W>) -> MetricsResult<()> {
    start_request(w)?;
    start_element(w, "volume-get-iter", None)?;
    start_element(w, "max-records", Some("1000"))?;

    end_element(w, "max-records")?;
    start_element(w, "tag", None)?;
    end_element(w, "tag")?;
    end_element(w, "volume-get-iter")?;
    end_element(w, "netapp")?;

    Ok(())
}

fn create_version_request<W: Write>(w: &mut EventWriter<W>) -> MetricsResult<()> {
    start_request(w)?;
    start_element(w, "system-get-ontapi-version", None)?;
    end_element(w, "system-get-ontapi-version")?;
    start_element(w, "system-get-version", None)?;
    end_element(w, "system-get-version")?;
    end_element(w, "netapp")?;
    Ok(())
}

fn start_element<W: Write>(
    w: &mut EventWriter<W>,
    element_name: &str,
    data: Option<&str>,
) -> MetricsResult<()> {
    match data {
        Some(chars) => {
            w.write(XmlEvent::start_element(element_name))?;
            w.write(XmlEvent::characters(chars))?;
        }
        None => {
            let e = XmlEvent::start_element(element_name);
            w.write(e)?;
        }
    };

    Ok(())
}

fn end_element<W: Write>(w: &mut EventWriter<W>, name: &str) -> MetricsResult<()> {
    let e = XmlEvent::end_element().name(name);
    w.write(e)?;
    Ok(())
}

impl Netapp {
    pub fn get_volume_performance(&self, t: DateTime<Utc>) -> MetricsResult<Vec<TsPoint>> {
        let mut output: Vec<u8> = Vec::new();
        {
            let mut writer = EventWriter::new(&mut output);
            create_peformance_request(&mut writer)?;
        }
        let res: HaPerformanceStats = api_request(&self.client, &self.config, output)?;
        debug!("netapp ha performance: {:#?}", res);

        // Squash all the Vec<Vec<TsPoints>> into Vec<TsPoint>
        let mut points: Vec<TsPoint> = res
            .perf
            .iter()
            .flat_map(|vol| vol.into_point(Some("netapp_volume_stat"), true))
            .collect();
        // Set all the timestamps to be identical
        for p in &mut points {
            p.timestamp = Some(t);
        }

        Ok(points)
    }

    pub fn get_volume_usage(&self, t: DateTime<Utc>) -> MetricsResult<Vec<TsPoint>> {
        let mut output: Vec<u8> = Vec::new();
        {
            let mut writer = EventWriter::new(&mut output);
            create_volume_request(&mut writer)?;
        }
        let res: NetappVolumes = api_request(&self.client, &self.config, output)?;
        debug!("netapp volume usage: {:#?}", res);

        // Squash all the Vec<Vec<TsPoints>> into Vec<TsPoint>
        let mut points: Vec<TsPoint> = res
            .vols
            .iter()
            .flat_map(|vol| vol.into_point(Some("netapp_volume"), true))
            .collect();
        // Set all the timestamps to be identical
        for p in &mut points {
            p.timestamp = Some(t);
        }

        Ok(points)
    }

    pub fn system_version_request(&self) -> MetricsResult<OnTapVersion> {
        let mut output: Vec<u8> = Vec::new();
        {
            let mut writer = EventWriter::new(&mut output);
            create_version_request(&mut writer)?;
        }
        let res: OnTapVersion = api_request(&self.client, &self.config, output)?;
        Ok(res)
    }
}
