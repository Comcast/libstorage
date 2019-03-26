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
use std::fmt::Debug;

use crate::error::MetricsResult;
use crate::ir::{TsPoint, TsValue};
use crate::IntoPoint;

use chrono::offset::Utc;
use chrono::DateTime;
use log::debug;
use serde::de::DeserializeOwned;
use uuid::Uuid;

#[derive(Deserialize, Debug)]
pub struct SolidfireConfig {
    /// The solidfire endpoint to use
    pub endpoint: String,
    pub user: String,
    pub password: String,
    /// Optional certificate file to use against the server
    /// der encoded
    pub certificate: Option<String>,
    /// The region this cluster is located in
    pub region: String,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct AddressBlock {
    pub available: String,
    pub size: u64,
    pub start: String,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct Cluster {
    pub cipi: String,
    pub cluster: String,
    pub encryption_capable: bool,
    pub ensemble: Vec<String>,
    pub mipi: String,
    pub name: String,
    #[serde(rename = "nodeID")]
    pub node_id: String,
    #[serde(rename = "pendingNodeID")]
    pub pending_node_id: u64,
    pub role: u64,
    pub sipi: String,
    pub state: ClusterState,
    pub version: String,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCapacity {
    active_block_space: u64,
    active_sessions: u64,
    #[serde(rename = "averageIOPS")]
    average_iops: u64,
    #[serde(rename = "clusterRecentIOSize")]
    cluster_recent_io_size: u64,
    #[serde(rename = "currentIOPS")]
    current_iops: u64,
    #[serde(rename = "maxIOPS")]
    max_iops: u64,
    max_over_provisionable_space: u64,
    max_provisioned_space: u64,
    max_used_metadata_space: u64,
    max_used_space: u64,
    non_zero_blocks: u64,
    peak_active_sessions: u64,
    #[serde(rename = "peakIOPS")]
    peak_iops: u64,
    provisioned_space: u64,
    timestamp: String,
    total_ops: u64,
    unique_blocks: u64,
    unique_blocks_used_space: u64,
    used_metadata_space: u64,
    used_metadata_space_in_snapshots: u64,
    used_space: u64,
    zero_blocks: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCapacityResult {
    cluster_capacity: ClusterCapacity,
}

impl IntoPoint for ClusterCapacityResult {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        self.cluster_capacity.into_point(name, is_time_series)
    }
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct ClusterFullThreshold {
    pub block_fullness: FullnessStatus,
    pub fullness: String,
    pub max_metadata_over_provision_factor: u16,
    pub metadata_fullness: FullnessStatus,
    pub slice_reserve_used_threshold_pct: u16,
    pub stage2_aware_threshold: u16,
    pub stage2_block_threshold_bytes: u64,
    pub stage3_block_threshold_bytes: u64,
    pub stage3_block_threshold_percent: u16,
    pub stage3_low_threshold: i64,
    pub stage4_block_threshold_bytes: u64,
    pub stage4_critical_threshold: i64,
    pub stage5_block_threshold_bytes: u64,
    pub sum_total_cluster_bytes: u64,
    pub sum_total_metadata_cluster_bytes: u64,
    pub sum_used_cluster_bytes: u64,
    pub sum_used_metadata_cluster_bytes: u64,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct ClusterInfo {
    pub attributes: HashMap<String, String>,
    pub encryption_at_rest_state: EncryptionState,
    pub ensemble: Vec<String>,
    pub mvip: String,
    pub mvip_interface: String,
    #[serde(rename = "mvipNodeID")]
    pub mvip_node_id: u16,
    pub mvip_vlan_tag: String,
    pub name: String,
    pub rep_count: u16,
    pub svip: String,
    pub svip_interface: String,
    #[serde(rename = "svipNodeID")]
    pub svip_node_id: u16,
    pub svip_vlan_tag: String,
    #[serde(rename = "uniqueID")]
    pub unique_id: String,
    pub uuid: Uuid,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterInfoResult {
    pub cluster_info: ClusterInfo,
}

impl IntoPoint for ClusterInfoResult {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        self.cluster_info.into_point(name, is_time_series)
    }
}

#[derive(Debug, Deserialize)]
pub enum ClusterState {
    Available,
    Pending,
    Active,
    PendingActive,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct ClusterStat {
    #[serde(rename = "actualIOPS")]
    pub actual_iops: u64,
    #[serde(rename = "averageIOPSize")]
    pub average_iop_size: u64,
    pub client_queue_depth: u64,
    pub cluster_utilization: f64,
    pub latency_u_sec: u64,
    #[serde(rename = "normalizedIOPS")]
    pub normalized_iops: u64,
    // Total cumulative bytes read from the cluster since creation
    pub read_bytes: u64,
    pub read_bytes_last_sample: u64,
    pub read_latency_u_sec: u64,
    pub read_latency_u_sec_total: u64,
    pub read_ops: u64,
    pub read_ops_last_sample: u64,
    pub sample_period_msec: u64,
    pub services_count: u64,
    pub services_total: u64,
    pub timestamp: String,
    pub unaligned_reads: u64,
    pub unaligned_writes: u64,
    pub write_bytes: u64,
    pub write_bytes_last_sample: u64,
    pub write_latency_u_sec: u64,
    pub write_latency_u_sec_total: u64,
    pub write_ops: u64,
    pub write_ops_last_sample: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterStatsResult {
    pub cluster_stats: ClusterStat,
}

impl IntoPoint for ClusterStatsResult {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        self.cluster_stats.into_point(name, is_time_series)
    }
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct Drive {
    pub attributes: HashMap<String, String>,
    pub capacity: u64,
    pub classic_slot: String,
    #[serde(rename = "driveID")]
    pub drive_id: u64,
    #[serde(rename = "nodeID")]
    pub node_id: u64,
    pub serial: String,
    pub status: DriveStatus,
    #[serde(rename = "drive")]
    pub drive_type: DriveType,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DriveStatus {
    Available,
    Active,
    Erasing,
    Failed,
    Removing,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DriveType {
    Block,
    Unknown,
    Volume,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EncryptionState {
    Enabling,
    Enabled,
    Disabling,
    Disabled,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FullnessStatus {
    Stage1Happy,
    Stage2Aware,
    Stage3Low,
    Stage4Critical,
    Stage5CompletelyConsumed,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct MetadataHosts {
    dead_secondaries: Vec<u64>,
    live_secondaries: Vec<u64>,
    primary: u64,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    #[serde(rename = "associatedFServiceID")]
    pub associated_f_service_id: u64,
    #[serde(rename = "associatedMasterServiceID")]
    pub associated_master_service_id: u64,
    pub attributes: HashMap<String, String>,
    pub cip: String,
    pub cipi: String,
    pub fibre_channel_target_port_group: Option<u64>,
    pub mip: String,
    pub mipi: String,
    pub name: String,
    #[serde(rename = "nodeID")]
    pub node_id: u64,
    pub node_slot: String,
    pub platform_info: PlatformInfo,
    #[serde(rename = "protocolEndpointIDs")]
    pub protocol_endpoints_ids: Option<Vec<Uuid>>,
    pub sip: String,
    pub sipi: String,
    pub software_version: String,
    pub uuid: Uuid,
    pub virtual_networks: Vec<VirtualNetwork>,
}

#[derive(Debug, Deserialize)]
pub struct Nodes {
    pub nodes: Vec<Node>,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct NodeStats {
    pub count: u64,
    pub cpu: u16,
    pub cpu_total: u64,
    // Bytes in on the cluster interface
    pub c_bytes_in: u64,
    // Bytes out on the cluster interface
    pub c_bytes_out: u64,
    // Bytes in on the storage interface
    pub s_bytes_in: u64,
    // Bytes out on the storage interface
    pub s_bytes_out: u64,
    // Bytes in on the management interface
    pub m_bytes_in: u64,
    // Bytes out on the management interface
    pub m_bytes_out: u64,
    pub network_utilization_cluster: u16,
    pub network_utilization_storage: u16,
    pub read_latency_u_sec_total: u64,
    pub read_ops: u64,
    pub timestamp: String,
    pub used_memory: u64,
    pub write_latency_u_sec_total: u64,
    pub write_ops: u64,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct PlatformInfo {
    pub chassis_type: String,
    pub cpu_model: String,
    #[serde(rename = "nodeMemoryGB")]
    pub node_memory_gb: u64,
    pub node_type: String,
    pub platform_config_version: String,
}

#[derive(Debug, Deserialize)]
pub enum ReplicationMode {
    Async,
    Sync,
    SnapshotsOnly,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct RemoteReplication {
    pub mode: ReplicationMode,
    pub pause_limit: u64,
    #[serde(rename = "remoteServiceID")]
    pub remote_service_id: u64,
    pub resume_details: Option<String>,
    pub snapshot_replication: SnapshotReplication,
    pub state: String,
    pub state_details: Option<String>,
}

#[derive(Debug, Deserialize)]
pub enum RemoveStatus {
    Present,
    NotPresent,
    Syncing,
    Deleted,
}

#[derive(Debug, Deserialize, IntoPoint)]
pub struct Snapshot {
    pub attributes: HashMap<String, String>,
    pub checksum: String,
    pub create_time: String,
    pub enable_remote_replication: String,
    pub expiration_reason: String,
    pub expiration_time: String,
    #[serde(rename = "groupID")]
    pub group_id: u64,
    #[serde(rename = "groupSnapshotUUID")]
    pub group_snapshot_uuid: Uuid,
    pub name: String,
    pub remote_status: RemoveStatus,
    #[serde(rename = "snapshotID")]
    pub snapshot_id: String,
    #[serde(rename = "snapshotUUID")]
    pub snapshot_uuid: Uuid,
    pub status: SnapshotStatus,
    pub total_size: u64,
    #[serde(rename = "virtualVolumeID")]
    pub virtual_volume_id: Uuid,
    #[serde(rename = "volumeID")]
    pub volume_id: u64,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotReplication {
    pub state: String,
    pub state_details: String,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct StorageContainer {
    #[serde(rename = "accountID")]
    pub account_id: u64,
    pub initiator_secret: String,
    pub name: String,
    pub protocol_endpoint_type: String,
    pub status: StorageContainerStatus,
    #[serde(rename = "storageContainerID")]
    pub storage_container_id: Uuid,
    pub target_secret: String,
    pub virtual_volumes: Vec<Uuid>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SnapshotStatus {
    Unknown,
    Preparing,
    RemoteSyncing,
    Done,
    Active,
    Cloning,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StorageContainerStatus {
    Active,
    Locked,
}

#[derive(Debug, Deserialize, IntoPoint)]
pub struct VolumesAttributesImageInfo {
    pub image_created_at: String,
    pub image_id: String,
    pub image_name: String,
    pub image_updated_at: String,
}

/*
#[derive(Debug, Deserialize)]
pub struct VolumesAttribute {
    pub cloned_count: Option<i64>,
    pub image_info: VolumesAttributesImageInfo,
}
*/

#[derive(Debug, Deserialize, IntoPoint)]
pub struct VolumesQosCurve {
    #[serde(rename = "1048576")]
    pub one_mb: i64,
    #[serde(rename = "131072")]
    pub onehundred_twentyeight_kb: i64,
    #[serde(rename = "16384")]
    pub sixteen_kb: i64,
    #[serde(rename = "262144")]
    pub twohundred_fiftysix_kb: i64,
    #[serde(rename = "32768")]
    pub thirtytwo_kb: i64,
    #[serde(rename = "4096")]
    pub four_kb: i64,
    #[serde(rename = "524288")]
    pub fivehundred_kb: i64,
    #[serde(rename = "65536")]
    pub sixtyfive_kb: i64,
    #[serde(rename = "8192")]
    pub eight_kb: i64,
}

#[derive(Debug, Deserialize, IntoPoint)]
pub struct VolumesQos {
    #[serde(rename = "burstIOPS")]
    pub burst_iops: i64,
    #[serde(rename = "burstTime")]
    pub burst_time: i64,
    pub curve: VolumesQosCurve,
    #[serde(rename = "maxIOPS")]
    pub max_iops: i64,
    #[serde(rename = "minIOPS")]
    pub min_iops: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum VolumeAccess {
    ReadOnly,
    ReadWrite,
    Locked,
    ReplicationTarget,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum VolumeStatus {
    Cloning,
    Waiting,
    Ready,
}

#[derive(Debug, Deserialize, IntoPoint)]
pub struct VolumePair {
    #[serde(rename = "clusterPairID")]
    pub cluster_pair_id: i64,
    pub remote_replication: RemoteReplication,
    #[serde(rename = "remoteSliceID")]
    pub remote_slice_id: u64,
    #[serde(rename = "remoteVolumeID")]
    pub remote_volume_id: u64,
    pub remote_volume_name: String,
    #[serde(rename = "volumePairUUID")]
    pub volume_pair_uuid: String,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct VirtualNetwork {
    pub address_blocks: Vec<AddressBlock>,
    pub attributes: HashMap<String, String>,
    pub name: String,
    pub netmask: String,
    pub svip: String,
    pub gateway: String,
    #[serde(rename = "virtualNetworkID")]
    pub virtual_network_id: u64,
    pub virtual_network_tag: u64,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct VirtualVolume {
    pub bindings: Vec<Uuid>,
    pub children: Vec<Uuid>,
    pub descendants: Vec<Uuid>,
    pub metadata: HashMap<String, String>,
    #[serde(rename = "parentVirtualVolumeID")]
    pub parent_virtual_volume_id: Uuid,
    #[serde(rename = "snapshotID")]
    pub snapshot_id: u64,
    pub snapshot_info: Snapshot,
    pub status: VolumeStatus,
    pub storage_container: StorageContainer,
    #[serde(rename = "virtualVolumeID")]
    pub virtual_volume_id: Uuid,
    pub virtual_volume_type: String,
    #[serde(rename = "volumeID")]
    pub volume_id: u64,
    pub volume_info: Option<Volume>,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    pub access: VolumeAccess,
    #[serde(rename = "accountID")]
    pub account_id: u64,
    pub attributes: HashMap<String, serde_json::Value>,
    pub block_size: u64,
    pub create_time: String,
    pub delete_time: String,
    pub enable512e: bool,
    pub enable_snap_mirror_replication: bool,
    pub iqn: String,
    pub last_access_time: Option<String>,
    #[serde(rename = "lastAccessTimeIO")]
    pub last_access_time_io: Option<String>,
    pub name: String,
    pub purge_time: String,
    pub qos: VolumesQos,
    #[serde(rename = "qosPolicyID")]
    pub qos_policy_id: Option<i64>,
    #[serde(rename = "scsiEUIDeviceID")]
    pub scsi_eui_device_id: String,
    #[serde(rename = "scsiNAADeviceID")]
    pub scsi_naa_device_id: String,
    pub slice_count: i64,
    pub status: String,
    pub total_size: i64,
    #[serde(rename = "virtualVolumeID")]
    pub virtual_volume_id: Option<Uuid>,
    pub volume_access_groups: Vec<u64>,
    #[serde(rename = "volumeConsistencyGroupUUID")]
    pub volume_consistency_group_uuid: Uuid,
    #[serde(rename = "volumeID")]
    pub volume_id: u64,
    pub volume_pairs: Vec<VolumePair>,
    #[serde(rename = "volumeUUID")]
    pub volume_uuid: Uuid,
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
pub struct VolumeStats {
    #[serde(rename = "accountID")]
    pub account_id: u64,
    #[serde(rename = "actualIOPS")]
    pub actual_iops: u64,
    pub async_delay: Option<String>,
    #[serde(rename = "averageIOPSize")]
    pub average_iop_size: u64,
    #[serde(rename = "burstIOPSCredit")]
    pub burst_iops_credit: u64,
    pub client_queue_depth: u64,
    pub cluster_utilization: Option<f64>,
    pub desired_metadata_hosts: Option<serde_json::Value>,
    pub latency_u_sec: u64,
    pub metadata_hosts: MetadataHosts,
    pub non_zero_blocks: u64,
    pub read_bytes: u64,
    pub read_bytes_last_sample: u64,
    pub read_latency_u_sec: u64,
    pub read_latency_u_sec_total: u64,
    pub read_ops: u64,
    pub read_ops_last_sample: u64,
    pub sample_period_m_sec: u64,
    pub throttle: f64,
    pub timestamp: String,
    pub unaligned_reads: u64,
    pub unaligned_writes: u64,
    pub volume_access_groups: Vec<u64>,
    #[serde(rename = "volumeID")]
    pub volume_id: u64,
    pub volume_size: u64,
    pub volume_utilization: f64,
    pub write_bytes: u64,
    pub write_bytes_last_sample: u64,
    pub write_latency_u_sec: u64,
    pub write_latency_u_sec_total: u64,
    pub write_ops: u64,
    pub write_ops_last_sample: u64,
    pub zero_blocks: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeStatsResult {
    pub volume_stats: VolumeStats,
}

impl IntoPoint for VolumeStatsResult {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        self.volume_stats.into_point(name, is_time_series)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveHardwareResult {
    nodes: Vec<HardwareNode>,
}

impl IntoPoint for DriveHardwareResult {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        self.nodes
            .iter()
            .flat_map(|n| n.into_point(name, is_time_series))
            .collect::<Vec<TsPoint>>()
    }
}

#[test]
fn test_get_drive_hardware() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/solidfire/list_drive_hardware.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let r: JsonResult<HardwareNodes> = serde_json::from_str(&buff).unwrap();
    println!(
        "JsonResult: {:?}",
        r.result.into_point(Some("solidfire_drive_hardware"), true)
    );
}

#[derive(Debug, Deserialize)]
struct HardwareNodes {
    nodes: Vec<HardwareNode>,
}

impl IntoPoint for HardwareNodes {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        self.nodes
            .iter()
            .flat_map(|n| n.into_point(name, is_time_series))
            .collect::<Vec<TsPoint>>()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HardwareNode {
    #[serde(rename = "nodeID")]
    node_id: u64,
    result: DriveHardware,
}

impl IntoPoint for HardwareNode {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        self.result.into_point(name, is_time_series)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveHardware {
    drive_hardware: Vec<HardwareDrive>,
}

impl IntoPoint for DriveHardware {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        self.drive_hardware
            .iter()
            .flat_map(|n| n.into_point(name, is_time_series))
            .collect::<Vec<TsPoint>>()
    }
}

#[derive(Debug, Deserialize, IntoPoint)]
#[serde(rename_all = "camelCase")]
struct HardwareDrive {
    canonical_name: String,
    connected: bool,
    dev: u64,
    dev_path: String,
    drive_type: String,
    life_remaining_percent: u64,
    lifetime_read_bytes: u64,
    lifetime_write_bytes: u64,
    name: String,
    path: String,
    path_link: String,
    power_on_hours: u64,
    product: String,
    reallocated_sectors: u64,
    reserve_capacity_percent: u64,
    scsi_compat_id: String,
    scsi_state: String,
    security_at_maximum: bool,
    security_enabled: bool,
    security_frozen: bool,
    security_locked: bool,
    security_supported: bool,
    serial: String,
    size: u64,
    slot: u64,
    uncorrectable_errors: u64,
    uuid: Uuid,
    vendor: String,
    version: String,
}

#[derive(Debug, Deserialize)]
pub struct Volumes {
    pub volumes: Vec<Volume>,
}

#[derive(Debug, Deserialize)]
pub struct JsonResult<T> {
    pub id: Option<String>,
    pub result: T,
}

#[test]
fn test_get_cluster_capacity() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/solidfire/get_cluster_capacity.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let r: JsonResult<ClusterCapacityResult> = serde_json::from_str(&buff).unwrap();
    println!("JsonResult: {:?}", r);
}

#[test]
fn test_get_cluster_fullness() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/solidfire/get_cluster_full_threshold.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let r: JsonResult<ClusterFullThreshold> = serde_json::from_str(&buff).unwrap();
    println!("JsonResult: {:?}", r);
}

#[test]
fn test_get_cluster_info() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/solidfire/get_cluster_info.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let r: JsonResult<ClusterInfoResult> = serde_json::from_str(&buff).unwrap();
    println!("JsonResult: {:?}", r);
}

#[test]
fn test_get_cluster_stats() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/solidfire/get_cluster_stats.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let r: JsonResult<ClusterStatsResult> = serde_json::from_str(&buff).unwrap();
    println!("JsonResult: {:?}", r);
}

#[test]
fn test_get_volume_stats() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/solidfire/get_volume_stats.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let r: JsonResult<VolumeStatsResult> = serde_json::from_str(&buff).unwrap();
    println!("JsonResult: {:?}", r);
}

#[test]
fn test_list_sf_volumes() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/solidfire/list_volumes.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let r: JsonResult<Volumes> = serde_json::from_str(&buff).unwrap();
    println!("JsonResult: {:?}", r);
}

#[test]
fn test_list_sf_nodes() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/solidfire/list_active_nodes.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let r: JsonResult<Nodes> = serde_json::from_str(&buff).unwrap();
    println!("JsonResult: {:?}", r);
}

// Call out to solidfire and return the result as a json deserialized struct
pub fn get<T>(
    client: &reqwest::Client,
    config: &SolidfireConfig,
    method: &str,
    params: Option<HashMap<String, String>>,
    force: bool,
) -> MetricsResult<T>
where
    T: DeserializeOwned + Debug,
{
    let mut url = format!("https://{}/json-rpc/8.4?method={}", config.endpoint, method);
    if force {
        url.push_str("&force=true");
    }
    if let Some(p) = params {
        url.push_str(
            &p.into_iter()
                .map(|(k, v)| format!("&{}={}", k, v))
                .collect::<Vec<String>>()
                .join(""),
        );
    }
    let j: T = crate::get(&client, &url, &config.user, Some(&config.password))?;

    Ok(j)
}

pub fn get_drive_hardware_info(
    client: &reqwest::Client,
    config: &SolidfireConfig,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    debug!("get_hardware_info");
    let info = get::<JsonResult<HardwareNodes>>(&client, &config, "ListDriveHardware", None, true)?;
    Ok(info
        .result
        .into_point(Some("solidfire_drive_hardware"), true)
        .into_iter()
        .map(|mut p| {
            p.timestamp = Some(t);
            p
        })
        .collect::<Vec<TsPoint>>())
}

pub fn get_cluster_capacity(
    client: &reqwest::Client,
    config: &SolidfireConfig,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    debug!("get_cluster_capacity");
    let info = get::<JsonResult<ClusterCapacityResult>>(
        &client,
        &config,
        "GetClusterCapacity",
        None,
        false,
    )?;
    Ok(info
        .result
        .into_point(Some("solidfire_cluster_capacity"), true)
        .into_iter()
        .map(|mut p| {
            p.timestamp = Some(t);
            p
        })
        .collect::<Vec<TsPoint>>())
}

pub fn get_cluster_fullness(
    client: &reqwest::Client,
    config: &SolidfireConfig,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    debug!("get_cluster_fullness");
    let info = get::<JsonResult<ClusterFullThreshold>>(
        &client,
        &config,
        "GetClusterFullThreshold",
        None,
        false,
    )?;
    Ok(info
        .result
        .into_point(Some("solidfire_cluster_full_threshold"), true)
        .into_iter()
        .map(|mut p| {
            p.timestamp = Some(t);
            p
        })
        .collect::<Vec<TsPoint>>())
}

pub fn get_cluster_info(
    client: &reqwest::Client,
    config: &SolidfireConfig,
) -> MetricsResult<ClusterInfoResult> {
    debug!("get_cluster_info");
    let info =
        get::<JsonResult<ClusterInfoResult>>(&client, &config, "GetClusterInfo", None, false)?;
    Ok(info.result)
}

pub fn get_cluster_stats(
    client: &reqwest::Client,
    config: &SolidfireConfig,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    debug!("get_cluster_stats");
    let info =
        get::<JsonResult<ClusterStatsResult>>(&client, &config, "GetClusterStats", None, false)?;
    Ok(info
        .result
        .into_point(Some("solidfire_cluster_stats"), true)
        .into_iter()
        .map(|mut p| {
            p.timestamp = Some(t);
            p
        })
        .collect::<Vec<TsPoint>>())
}

pub fn get_volume_stats(
    client: &reqwest::Client,
    config: &SolidfireConfig,
    volume_id: u64,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let mut params = HashMap::new();
    params.insert("volumeID".to_string(), volume_id.to_string());

    debug!("get_volume_stats");
    let info = get::<JsonResult<VolumeStatsResult>>(
        &client,
        &config,
        "GetVolumeStats",
        Some(params),
        false,
    )?;
    Ok(info
        .result
        .into_point(Some("solidfire_volume_stats"), true)
        .into_iter()
        .map(|mut p| {
            p.timestamp = Some(t);
            p
        })
        .collect::<Vec<TsPoint>>())
}

//pub fn get_node_stats() -> MetricsResult<Vec<TsPoint>> {
//
//}

pub fn list_volumes(client: &reqwest::Client, config: &SolidfireConfig) -> MetricsResult<Volumes> {
    debug!("list_volumes");
    let info = get::<JsonResult<Volumes>>(&client, &config, "ListVolumes", None, false)?;
    Ok(info.result)
}

pub fn list_volume_ids(
    client: &reqwest::Client,
    config: &SolidfireConfig,
) -> MetricsResult<Vec<u64>> {
    debug!("list_volume_ids");
    let info = get::<JsonResult<Volumes>>(&client, &config, "ListVolumes", None, false)?;
    Ok(info
        .result
        .volumes
        .into_iter()
        .map(|v| v.volume_id)
        .collect::<Vec<u64>>())
}
