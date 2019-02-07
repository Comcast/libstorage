//! Scaleio Definitions:
//! SDS: the SDS is defined as the ScaleIO Data Server. The SDS is
//! the software component that contributes local storage space to an
//! aggregated pool of storage within the ScaleIO virtual SAN.
//! SDC: ScaleIO Data Client. A  lightweight device driver that exposes ScaleIO
//! shared block volumes to applications.
//! MDM: ScaleIO Meta Data Manager.  Manages, configures and monitors the ScaleIO system

use crate::deserialize_string_or_int;
use crate::error::{MetricsResult, StorageError};
use crate::ir::{TsPoint, TsValue};
use crate::IntoPoint;

use std::collections::HashMap;
use std::fmt::Debug;
use std::net::IpAddr;
use std::str;

use chrono::offset::Utc;
use chrono::DateTime;
use nom::IResult;
use reqwest::header::CONTENT_TYPE;
use serde::de::DeserializeOwned;

#[derive(Deserialize, Debug)]
pub struct ScaleioConfig {
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
    /// bandwidth limits for new volumes in this cluster
    pub bandwidth_limit: Option<u64>,
    /// iops limit for new volumes in this cluster
    pub iops_limit: Option<u64>,
}

#[test]
fn test_get_system_config() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/scaleio/system_config.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: SystemConfig = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug)]
pub struct SystemConfig {
    pub system_id: String,
    pub mdm_port: String,
    pub snmp_resend_frequency: String,
    pub snmp_sampling_frequency: String,
    pub features_enable_snmp: String,
    pub cipher_suites: Option<String>,
    #[serde(rename = "featuresEnableIM")]
    pub features_enable_im: String,
    pub snmp_port: String,
    pub bypass_certificate_check: String,
    pub mdm_username: String,
    pub mdm_addresses: Vec<String>,
    pub snmp_traps_receiver_ips: Option<Vec<String>>,
    pub allow_non_secure_communication: String,
}

// BWC=Bandwidth Calculation
#[serde(rename_all = "camelCase")]
#[derive(Clone, Deserialize, Debug)]
pub struct BWC {
    pub total_weight_in_kb: u64,
    pub num_occured: u64,
    pub num_seconds: u64,
}

impl BWC {
    // Calculate the average kb/s from the fields
    fn average(&self) -> u64 {
        let avg = self
            .total_weight_in_kb
            .checked_div(self.num_occured)
            .unwrap_or(0);
        avg.checked_div(self.num_seconds).unwrap_or(0)
    }
}

#[serde(rename_all = "camelCase")]
#[derive(Clone, Deserialize, Debug)]
pub struct CertificateInfo {
    subject: String,
    issuer: String,
    valid_from: String,
    valid_to: String,
    thumbprint: String,
    valid_from_asn1_format: String,
    valid_to_asn1_format: String,
}

#[serde(rename_all = "camelCase")]
#[derive(Clone, Deserialize, Debug, IntoPoint)]
pub struct OscillatingCounterWindow {
    pub threshold: Option<i64>,
    pub window_size_in_sec: Option<i64>,
    pub last_oscillation_count: Option<i64>,
    pub last_oscillationi_time: Option<u64>,
    pub max_failures_count: Option<i64>,
    pub max_failures_time: Option<u64>,
    pub fixed_read_error_count: Option<u64>,
    pub avg_read_size_in_bytes: Option<u64>,
    pub avg_write_size_in_bytes: Option<u64>,
    pub avg_read_latency_in_microsec: Option<u64>,
    pub avg_write_latency_in_microsec: Option<u64>,
    pub capacity_in_use_in_kb: Option<u64>,
    pub thick_capacity_in_use_in_kb: Option<u64>,
    pub thin_capacity_in_use_in_kb: Option<u64>,
    pub snap_capacity_in_use_in_kb: Option<u64>,
    pub snap_capacity_in_use_occupied_in_kb: Option<u64>,
    pub unreachable_unused_capacity_in_kb: Option<u64>,
    pub protected_vac_in_kb: Option<u64>,
    pub degraded_healthy_vac_in_kb: Option<u64>,
    pub degraded_failed_vac_in_kb: Option<u64>,
    pub failed_vac_in_kb: Option<u64>,
    pub in_use_vac_in_kb: Option<u64>,
    pub active_moving_in_fwd_rebuild_jobs: Option<u64>,
    pub pending_moving_in_fwd_rebuild_jobs: Option<u64>,
    pub active_moving_out_fwd_rebuild_jobs: Option<u64>,
    pub pending_moving_out_fwd_rebuild_jobs: Option<u64>,
    pub active_moving_in_bck_rebuild_jobs: Option<u64>,
    pub pending_moving_in_bck_rebuild_jobs: Option<u64>,
    pub active_moving_out_bck_rebuild_jobs: Option<u64>,
    pub pending_moving_out_bck_rebuild_jobs: Option<u64>,
    pub active_moving_in_rebalance_jobs: Option<u64>,
    pub pending_moving_in_rebalance_jobs: Option<u64>,
    pub active_moving_rebalance_jobs: Option<u64>,
    pub pending_moving_rebalance_jobs: Option<u64>,
    pub primary_vac_in_kb: Option<u64>,
    pub secondary_vac_in_kb: Option<u64>,
    pub primary_read_bwc: Option<BWC>,
    pub primary_read_from_dev_bwc: Option<BWC>,
    pub primary_write_bwc: Option<BWC>,
    pub secondary_read_bwc: Option<BWC>,
    pub secondary_read_from_dev_bwc: Option<BWC>,
    pub secondary_write_bwc: Option<BWC>,
    pub total_read_bwc: Option<BWC>,
    pub total_write_bwc: Option<BWC>,
    pub fwd_rebuild_read_bwc: Option<BWC>,
    pub fwd_rebuild_write_bwc: Option<BWC>,
    pub bck_rebuild_read_bwc: Option<BWC>,
    pub bck_rebuild_write_bwc: Option<BWC>,
    pub rebalance_read_bwc: Option<BWC>,
    pub rebalance_write_bwc: Option<BWC>,
    pub background_scan_compare_count: Option<u64>,
    pub background_scanned_in_mb: Option<u64>,
    pub thin_capacity_allocated_in_km: Option<u64>,
    pub rm_pending_allocated_in_kb: Option<u64>,
    pub semi_protected_vac_in_kb: Option<u64>,
    pub in_maintenance_vac_in_kb: Option<u64>,
    pub active_moving_in_norm_rebuild_jobs: Option<u64>,
    pub active_moving_out_norm_rebuild_jobs: Option<u64>,
    pub pending_moving_in_norm_rebuild_jobs: Option<u64>,
    pub pending_moving_out_normrebuild_jobs: Option<u64>,
    pub primary_read_from_rmcache_bwc: Option<BWC>,
    pub secondary_read_from_rmcache_bwc: Option<BWC>,
    pub norm_rebuild_read_bwc: Option<BWC>,
    pub norm_rebuild_write_bwc: Option<BWC>,
    pub rfcache_reads_received: Option<u64>,
    pub rfcache_writes_received: Option<u64>,
    pub rfcache_avg_read_time: Option<u64>,
    pub rfcache_avg_write_time: Option<u64>,
    pub rfcache_source_device_reads: Option<u64>,
    pub rfcache_source_device_writes: Option<u64>,
    pub rfache_read_hit: Option<u64>,
    pub rfcache_read_miss: Option<u64>,
    pub rfcache_write_miss: Option<u64>,
    pub rfcache_ios_skipped: Option<u64>,
    pub rfcache_reads_skipped: Option<u64>,
    pub rfcache_reads_skipped_aligned_size_too_large: Option<u64>,
    pub rfcache_reads_skipped_max_io_size: Option<u64>,
    pub rfcache_reads_skipped_heavy_load: Option<u64>,
    pub rfcache_reads_skipped_stuck_io: Option<u64>,
    pub rfcache_reads_skipped_low_resources: Option<u64>,
    pub rfcache_reads_skipped_internal_error: Option<u64>,
    pub rfcache_reads_skipped_lock_ios: Option<u64>,
    pub rfcache_writes_skipped_max_io_size: Option<u64>,
    pub rfcache_writes_skipped_heavy_load: Option<u64>,
    pub rfcache_writes_skipped_stuck_io: Option<u64>,
    pub rfcache_writes_skipped_low_resources: Option<u64>,
    pub rfcache_writes_skipped_internal_error: Option<u64>,
    pub rfcache_writes_skipped_cache_miss: Option<u64>,
    pub rfcache_skipped_unlined_write: Option<u64>,
    pub rfcache_io_errors: Option<u64>,
    pub rfcache_reads_from_cache: Option<u64>,
    pub rfcache_ios_outstanding: Option<u64>,
    pub rfcache_reads_pending: Option<u64>,
    pub rfcache_write_pending: Option<u64>,
}

#[serde(rename_all = "camelCase")]
#[derive(Clone, Deserialize, Debug)]
pub struct FailureCounter {
    pub short_window: Window,
    pub medium_window: Option<Window>,
    pub long_window: Option<Window>,
}

#[serde(rename_all = "camelCase")]
#[derive(Clone, Deserialize, Debug)]
pub struct Successfulio {
    pub short_window: Option<OscillatingCounterWindow>,
    pub medium_window: Option<OscillatingCounterWindow>,
    pub long_window: Option<OscillatingCounterWindow>,
}

#[derive(Clone, Deserialize, Debug)]
pub struct Link {
    pub rel: String,
    pub href: String,
}

#[derive(Deserialize, Debug)]
pub enum AuthenticationError {
    None,
    General,
    ErrorLoadingOpenssl,
    ErrorLoadingCertificate,
    VerificationError,
    ErrorLoadingAuthenticationInMdm,
    Open,
    #[serde(rename = "sslVerionTooLong")]
    SslVerionTooLong,
}

impl ToString for AuthenticationError {
    fn to_string(&self) -> String {
        match *self {
            AuthenticationError::None => "None".into(),
            AuthenticationError::General => "General".into(),
            AuthenticationError::ErrorLoadingOpenssl => "ErrorLoadingOpenssl".into(),
            AuthenticationError::ErrorLoadingCertificate => "ErrorLoadingCertificiate".into(),
            AuthenticationError::VerificationError => "VerificationError".into(),
            AuthenticationError::ErrorLoadingAuthenticationInMdm => {
                "ErrorLoadingAuthenticationInMdm".into()
            }
            AuthenticationError::Open => "Open".into(),
            AuthenticationError::SslVerionTooLong => "SslVersionTooLong".into(),
        }
    }
}

#[derive(Clone, Deserialize, Debug)]
pub enum DeviceState {
    InitialTest,
    InitialTestDone,
    Normal,
    NormalTesting,
    RemovePending,
}

impl ToString for DeviceState {
    fn to_string(&self) -> String {
        match *self {
            DeviceState::InitialTest => "InitialTest".into(),
            DeviceState::InitialTestDone => "InitialTestDone".into(),
            DeviceState::Normal => "Normal".into(),
            DeviceState::NormalTesting => "NormalTesting".into(),
            DeviceState::RemovePending => "RemovePending".into(),
        }
    }
}

#[test]
fn test_scaleio_drive_stats() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/scaleio/device_statistics.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: DeviceStatistics = serde_json::from_str(&buff).unwrap();
    let points = i.into_point(Some("scaleio_device"));
    println!("result: {:#?}", i);
    println!("points: {:?}", points);
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug)]
pub struct DeviceStatistics {
    avg_write_size_in_bytes: u64,
    active_moving_in_fwd_rebuild_jobs: u64,
    active_moving_in_rebalance_jobs: u64,
    active_moving_out_bck_rebuild_jobs: u64,
    active_moving_out_fwd_rebuild_jobs: u64,
    active_moving_rebalance_jobs: u64,
    active_moving_in_norm_rebuild_jobs: u64,
    active_moving_in_bck_rebuild_jobs: u64,
    active_moving_out_norm_rebuild_jobs: u64,
    avg_read_latency_in_microsec: u64,
    avg_write_latency_in_microsec: u64,
    avg_read_size_in_bytes: u64,
    #[serde(rename = "BackgroundScanCompareCount")]
    background_scan_compare_count: u64,
    #[serde(rename = "BackgroundScannedInMB")]
    background_scanned_in_mb: u64,
    bck_rebuild_write_bwc: BWC,
    bck_rebuild_read_bwc: BWC,
    capacity_in_use_in_kb: u64,
    degraded_healthy_vac_in_kb: u64,
    degraded_failed_vac_in_kb: u64,
    failed_vac_in_kb: u64,
    fixed_read_error_count: u64,
    fwd_rebuild_read_bwc: BWC,
    fwd_rebuild_write_bwc: BWC,
    in_maintenance_vac_in_kb: u64,
    in_use_vac_in_kb: u64,
    norm_rebuild_read_bwc: BWC,
    norm_rebuild_write_bwc: BWC,
    pending_moving_in_bck_rebuild_jobs: u64,
    pending_moving_out_bck_rebuild_jobs: u64,
    pending_moving_in_norm_rebuild_jobs: u64,
    pending_moving_rebalance_jobs: u64,
    pending_moving_out_normrebuild_jobs: u64,
    pending_moving_in_rebalance_jobs: u64,
    pending_moving_in_fwd_rebuild_jobs: u64,
    pending_moving_out_fwd_rebuild_jobs: u64,
    primary_read_from_rmcache_bwc: BWC,
    primary_read_from_dev_bwc: BWC,
    primary_read_bwc: BWC,
    primary_vac_in_kb: u64,
    protected_vac_in_kb: u64,
    primary_write_bwc: BWC,
    rebalance_read_bwc: BWC,
    rebalance_write_bwc: BWC,
    rfcache_avg_read_time: u64,
    rfcache_io_errors: u64,
    rfcache_reads_skipped_internal_error: u64,
    rfcache_source_device_writes: u64,
    rfcache_reads_skipped_low_resources: u64,
    rfcache_reads_skipped_max_io_size: u64,
    rfcache_reads_skipped_aligned_size_too_large: u64,
    rfcache_writes_skipped_internal_error: u64,
    rfcache_writes_skipped_stuck_io: u64,
    rfcache_writes_skipped_cache_miss: u64,
    rfcache_writes_skipped_heavy_load: u64,
    rfcache_write_miss: u64,
    rfcache_writes_skipped_low_resources: u64,
    rfcache_reads_from_cache: u64,
    rfcache_ios_outstanding: u64,
    rfcache_skipped_unlined_write: u64,
    rfcache_writes_received: u64,
    rfcache_write_pending: u64,
    rfcache_writes_skipped_max_io_size: u64,
    rfcache_reads_skipped_stuck_io: u64,
    rfcache_reads_skipped: u64,
    rfcache_reads_received: u64,
    rfcache_ios_skipped: u64,
    rfcache_read_miss: u64,
    rfcache_reads_skipped_lock_ios: u64,
    rfache_read_hit: u64,
    rfcache_avg_write_time: u64,
    rfcache_source_device_reads: u64,
    rfcache_reads_skipped_heavy_load: u64,
    rfcache_reads_pending: u64,
    rm_pending_allocated_in_kb: u64,
    secondary_read_from_dev_bwc: BWC,
    secondary_vac_in_kb: u64,
    secondary_read_bwc: BWC,
    secondary_read_from_rmcache_bwc: BWC,
    secondary_write_bwc: BWC,
    snap_capacity_in_use_occupied_in_kb: u64,
    snap_capacity_in_use_in_kb: u64,
    thick_capacity_in_use_in_kb: u64,
    thin_capacity_in_use_in_kb: u64,
    thin_capacity_allocated_in_km: u64,
    total_read_bwc: BWC,
    total_write_bwc: BWC,
    unused_capacity_in_kb: u64,
    unreachable_unused_capacity_in_kb: u64,
    semi_protected_vac_in_kb: u64,
}

impl IntoPoint for DeviceStatistics {
    fn into_point(&self, name: Option<&str>) -> Vec<TsPoint> {
        let mut p = TsPoint::new(name.unwrap_or("scaleio_drive_stat"));
        p.add_field(
            "avg_write_size_in_bytes",
            TsValue::Long(self.avg_write_size_in_bytes),
        );
        p.add_field(
            "avg_read_latency_in_microsec",
            TsValue::Long(self.avg_read_latency_in_microsec),
        );
        p.add_field(
            "avg_write_latency_in_microsec",
            TsValue::Long(self.avg_write_latency_in_microsec),
        );
        p.add_field(
            "avg_read_size_in_bytes",
            TsValue::Long(self.avg_read_size_in_bytes),
        );
        p.add_field(
            "capacity_in_use_in_kb",
            TsValue::Long(self.capacity_in_use_in_kb),
        );
        p.add_field(
            "degraded_healthy_vac_in_kb",
            TsValue::Long(self.degraded_healthy_vac_in_kb),
        );
        p.add_field(
            "degraded_failed_vac_in_kb",
            TsValue::Long(self.degraded_failed_vac_in_kb),
        );
        p.add_field(
            "primary_read_bwc",
            TsValue::Long(self.primary_read_bwc.average()),
        );
        p.add_field("primary_vac_in_kb", TsValue::Long(self.primary_vac_in_kb));
        p.add_field(
            "protected_vac_in_kb",
            TsValue::Long(self.protected_vac_in_kb),
        );
        p.add_field(
            "primary_write_bwc",
            TsValue::Long(self.primary_write_bwc.average()),
        );
        p.add_field(
            "thick_capacity_in_use_in_kb",
            TsValue::Long(self.thick_capacity_in_use_in_kb),
        );
        p.add_field(
            "thin_capacity_in_use_in_kb",
            TsValue::Long(self.thin_capacity_in_use_in_kb),
        );
        p.add_field(
            "thin_capacity_allocated_in_km",
            TsValue::Long(self.thin_capacity_allocated_in_km),
        );
        p.add_field(
            "total_read_bwc",
            TsValue::Long(self.total_read_bwc.average()),
        );
        p.add_field(
            "total_write_bwc",
            TsValue::Long(self.total_write_bwc.average()),
        );
        p.add_field(
            "unused_capacity_in_kb",
            TsValue::Long(self.unused_capacity_in_kb),
        );
        p.add_field(
            "unreachable_unused_capacity_in_kb",
            TsValue::Long(self.unreachable_unused_capacity_in_kb),
        );
        p.add_field(
            "rebalance_read_bwc",
            TsValue::Long(self.rebalance_read_bwc.average()),
        );
        p.add_field(
            "rebalance_write_bwc",
            TsValue::Long(self.rebalance_write_bwc.average()),
        );

        vec![p]
    }
}

#[derive(Deserialize, Debug)]
pub enum DrlMode {
    Volatile,
    NonVolatile,
}

impl ToString for DrlMode {
    fn to_string(&self) -> String {
        match *self {
            DrlMode::Volatile => "Volatile".into(),
            DrlMode::NonVolatile => "NonVolatile".into(),
        }
    }
}

#[derive(Deserialize, Debug)]
pub enum IpRole {
    #[serde(rename = "sdsOnly")]
    SdsOnly,
    #[serde(rename = "sdcOnly")]
    SdcOnly,
    #[serde(rename = "all")]
    All,
}

#[derive(Deserialize, Debug)]
pub enum MembershipState {
    JoinPending,
    Joined,
    Decoupled,
}

impl ToString for MembershipState {
    fn to_string(&self) -> String {
        match *self {
            MembershipState::JoinPending => "JoinPending".into(),
            MembershipState::Joined => "Joined".into(),
            MembershipState::Decoupled => "Decoupled".into(),
        }
    }
}

#[derive(Deserialize, Debug)]
pub enum MaintenanceState {
    NoMaintenance,
    SetMaintenanceInProgress,
    InMaintenance,
    ExitMaintenanceInProgress,
}

impl ToString for MaintenanceState {
    fn to_string(&self) -> String {
        match *self {
            MaintenanceState::NoMaintenance => "NoMaintenance".into(),
            MaintenanceState::SetMaintenanceInProgress => "SetMaintenanceInProgress".into(),
            MaintenanceState::InMaintenance => "Inmaintenance".into(),
            MaintenanceState::ExitMaintenanceInProgress => "ExitMaintenanceInProgress".into(),
        }
    }
}

#[derive(Deserialize, Debug)]
pub enum MdmConnectionState {
    Connected,
    Disconnected,
}

impl ToString for MdmConnectionState {
    fn to_string(&self) -> String {
        match *self {
            MdmConnectionState::Connected => "Connected".into(),
            MdmConnectionState::Disconnected => "Disconnected".into(),
        }
    }
}

#[derive(Deserialize, Debug)]
pub enum MemoryAllocationState {
    RmcacheMemoryAllocationStateInvalid,
    AllocationPending,
    AllocationSuccessful,
    AllocationFailed,
    RmcacheDisabled,
}

impl ToString for MemoryAllocationState {
    fn to_string(&self) -> String {
        match *self {
            MemoryAllocationState::RmcacheMemoryAllocationStateInvalid => {
                "RmcacheMemoryAllocationStateInvalid".into()
            }
            MemoryAllocationState::AllocationPending => "AllocationPending".into(),
            MemoryAllocationState::AllocationSuccessful => "AllocationSuccessful".into(),
            MemoryAllocationState::AllocationFailed => "AllocationFailed".into(),
            MemoryAllocationState::RmcacheDisabled => "RmcacheDisabled".into(),
        }
    }
}

#[derive(Deserialize, Debug)]
pub enum PerfProfile {
    Custom,
    Default,
    HighPerformance,
}

impl ToString for PerfProfile {
    fn to_string(&self) -> String {
        match *self {
            PerfProfile::Custom => "Custom".into(),
            PerfProfile::Default => "Default".into(),
            PerfProfile::HighPerformance => "HighPerformance".into(),
        }
    }
}

#[test]
fn test_instances() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/scaleio/instances.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: Vec<Instance> = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[test]
fn test_selected_stats() {
    use std::fs::File;
    use std::io::Read;

    // Test drive stats response
    let mut f = File::open("tests/scaleio/querySelectedStatistics.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: SelectedStatisticsResponse = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);

    // Test cluster stats response
    let mut f = File::open("tests/scaleio/clusterSelectedStatisticsResponse.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: ClusterSelectedStatisticsResponse = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[derive(Deserialize, Debug)]
pub struct SelectedStatisticsResponse {
    #[serde(rename = "Device")]
    pub device: HashMap<String, HashMap<String, u64>>,
}

#[derive(Deserialize, Debug)]
pub struct ClusterSelectedStatisticsResponse {
    #[serde(rename = "StoragePool")]
    pub storage_pool: HashMap<String, StoragePoolInfo>,
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug, IntoPoint)]
pub struct StoragePoolInfo {
    pub num_of_devices: u64,
    pub num_of_volumes: u64,
    pub primary_read_bwc: BWC,
    pub primary_write_bwc: BWC,
    pub secondary_write_bwc: BWC,
    pub secondary_read_bwc: BWC,
    pub capacity_limit_in_kb: u64,
    pub thick_capacity_in_use_in_kb: u64,
    pub thin_capacity_in_use_in_kb: u64,
    pub thin_capacity_allocated_in_km: u64,
    pub total_write_bwc: BWC,
    pub total_read_bwc: BWC,
}

#[serde(rename_all = "camelCase")]
#[derive(Clone, Deserialize, Debug, IntoPoint)]
pub struct Instance {
    pub device_current_path_name: String,
    pub device_original_path_name: String,
    pub rfcache_error_device_does_not_exist: bool,
    pub sds_id: String,
    pub device_state: DeviceState,
    pub capacity_limit_in_kb: Option<u64>,
    pub max_capacity_in_kb: u64,
    pub storage_pool_id: String,
    pub long_successful_ios: Option<Successfulio>,
    pub error_state: String,
    pub name: Option<String>,
    pub id: String,
    pub links: Vec<Link>,
    pub update_configuration: Option<bool>,
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct MdmCluster {
    pub master: TieBreaker,
    pub slaves: Vec<TieBreaker>,
    pub cluster_mode: String,
    pub tie_breakers: Vec<TieBreaker>,
    pub good_nodes_num: u16,
    pub good_replicas_num: u16,
    pub cluster_state: String,
    pub name: String,
    pub id: String,
}

#[serde(rename_all = "camelCase")]
#[derive(Serialize, Debug)]
pub struct SelectedStatisticsRequest {
    pub selected_statistics_list: Vec<StatsRequest>,
}

#[derive(Serialize, Debug)]
pub enum StatsRequestType {
    System,
    ProtectionDomain,
    Sds,
    StoragePool,
    Device,
    Volume,
    VTree,
    Sdc,
    FaultSet,
    RfcacheDevice,
}

#[derive(Debug, Deserialize)]
pub enum RebuildIoPriority {
    #[serde(rename = "unlimited")]
    Unlimited,
    #[serde(rename = "limitNumOfConcurrentIos")]
    LimitNumOfConcurrentIos,
    #[serde(rename = "favorAppIos")]
    FavorAppIos,
    #[serde(rename = "dynamicBwThrottling")]
    DynamicBwThrottling,
}

#[derive(Debug, Deserialize)]
pub enum BackgroundScannerMode {
    Disabled,
    DeviceOnly,
    DataComparison,
}

#[derive(Debug, Deserialize)]
pub enum CacheWriteHandlingMode {
    Passthrough,
    Cached,
}

#[serde(rename_all = "camelCase")]
#[derive(Serialize, Debug)]
pub struct StatsRequest {
    #[serde(rename = "type")]
    pub req_type: StatsRequestType,
    // This can be left blank for all ids
    pub all_ids: Vec<String>,
    // "fixedReadErrorCount", "avgReadSizeInBytes", "avgWriteSizeInBytes",
    //
    pub properties: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct IpObject {
    pub ip: IpAddr,
    pub role: IpRole,
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize, IntoPoint)]
pub struct ScsiInitiatorMappingInfo {
    pub scsi_initiator_id: String,
    pub scsi_initiator_name: String,
    pub scsi_initiator_iqn: String,
    pub lun: String,
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct SdcMappingInfo {
    pub sdc_id: String,
    pub sdc_ip: String,
    pub limit_iops: u64,
    pub limit_bw_in_mbps: u64,
}

impl IntoPoint for SdcMappingInfo {
    fn into_point(&self, name: Option<&str>) -> Vec<TsPoint> {
        let mut p = TsPoint::new(name.unwrap_or("scaleio_volume_sdc"));
        p.add_tag("sdc_id", TsValue::String(self.sdc_id.clone()));
        p.add_tag("sdc_ip", TsValue::String(self.sdc_ip.clone()));
        p.add_field("limit_iops", TsValue::Long(self.limit_iops));
        p.add_field("limit_bw_in_mbps", TsValue::Long(self.limit_bw_in_mbps));

        vec![p]
    }
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct SdsVolume {
    pub id: String,
    pub name: Option<String>,
    pub size_in_kb: u64,
    pub is_obfuscated: bool,
    pub creation_time: u64,
    pub volume_type: String,
    pub consistency_group_id: Option<String>,
    pub mapping_to_all_sdcs_enabled: bool,
    pub mapped_sdc_info: Option<Vec<SdcMappingInfo>>,
    pub mapped_scsi_initiator_info_list: Option<Vec<ScsiInitiatorMappingInfo>>,
    pub ancestor_volume_id: Option<String>,
    pub vtree_id: String,
    pub storage_pool_id: String,
    pub use_rmcache: Option<bool>,
}

impl IntoPoint for SdsVolume {
    fn into_point(&self, name: Option<&str>) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        let mut p = TsPoint::new(name.unwrap_or("scaleio_volume"));
        p.add_tag("id", TsValue::String(self.id.clone()));
        if let Some(ref name) = self.name {
            p.add_tag("name", TsValue::String(name.clone()));
        }
        p.add_field("size_in_kb", TsValue::Long(self.size_in_kb));
        p.add_field("is_obfuscated", TsValue::Boolean(self.is_obfuscated));
        p.add_field("creation_time", TsValue::Long(self.creation_time));
        p.add_tag("volume_type", TsValue::String(self.volume_type.clone()));
        if let Some(ref group_id) = self.consistency_group_id {
            p.add_tag("consistency_group_id", TsValue::String(group_id.clone()));
        }
        p.add_field(
            "mapping_to_all_sdcs_enabled",
            TsValue::Boolean(self.mapping_to_all_sdcs_enabled),
        );

        // This is a 1:Many relationship so we're going to denormalize that here
        // and store the sdc_info is a separate table with the volume id so we can
        // find it later
        if let Some(ref mapped_sdc_info) = self.mapped_sdc_info {
            for sdc_map in mapped_sdc_info {
                sdc_map
                    .into_point(Some("scaleio_volume_sdc"))
                    .into_iter()
                    .for_each(|mut point| {
                        // Add the volume id so we can look this up later
                        point.add_tag("volume", TsValue::String(self.id.clone()));
                        points.push(point);
                    });
            }
        }

        if let Some(ref mapped_scsi_list) = self.mapped_scsi_initiator_info_list {
            for scsi_map in mapped_scsi_list {
                scsi_map
                    .into_point(Some("scaleio_volume_scsi"))
                    .into_iter()
                    .for_each(|mut point| {
                        point.add_tag("volume", TsValue::String(self.id.clone()));
                        points.push(point);
                    });
            }
        }

        if let Some(ref ancestor) = self.ancestor_volume_id {
            p.add_field("ancestor_volume_id", TsValue::String(ancestor.clone()));
        }
        p.add_tag("vtree_id", TsValue::String(self.vtree_id.clone()));
        p.add_tag(
            "storage_pool_id",
            TsValue::String(self.storage_pool_id.clone()),
        );
        if let Some(use_rmcache) = self.use_rmcache {
            p.add_field("use_rmcache", TsValue::Boolean(use_rmcache));
        }
        points.push(p);

        points
    }
}

#[test]
fn test_sds_object() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/scaleio/sdsObject.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: SdsObject = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[test]
fn test_sds_statistics() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/scaleio/sds_statistics.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: SdsStatistics = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize, IntoPoint)]
pub struct SdsStatistics {
    active_moving_in_bck_rebuild_jobs: u64,
    active_moving_in_fwd_rebuild_jobs: u64,
    active_moving_in_norm_rebuild_jobs: u64,
    active_moving_in_rebalance_jobs: u64,
    active_moving_out_bck_rebuild_jobs: u64,
    active_moving_out_fwd_rebuild_jobs: u64,
    active_moving_out_norm_rebuild_jobs: u64,
    active_moving_rebalance_jobs: u64,
    #[serde(rename = "BackgroundScanCompareCount")]
    background_scan_compare_count: u64,
    #[serde(rename = "BackgroundScannedInMB")]
    background_scanned_in_mb: u64,
    bck_rebuild_read_bwc: BWC,
    bck_rebuild_write_bwc: BWC,
    capacity_in_use_in_kb: u64,
    capacity_limit_in_kb: u64,
    degraded_failed_vac_in_kb: u64,
    degraded_healthy_vac_in_kb: u64,
    failed_vac_in_kb: u64,
    fixed_read_error_count: u64,
    fwd_rebuild_read_bwc: BWC,
    fwd_rebuild_write_bwc: BWC,
    in_maintenance_vac_in_kb: u64,
    in_use_vac_in_kb: u64,
    maintenance_mode_state: Option<u64>,
    max_capacity_in_kb: u64,
    norm_rebuild_read_bwc: BWC,
    norm_rebuild_write_bwc: BWC,
    num_of_devices: u64,
    num_of_rfcache_devices: u64,
    pending_moving_in_bck_rebuild_jobs: u64,
    pending_moving_in_fwd_rebuild_jobs: u64,
    pending_moving_in_norm_rebuild_jobs: u64,
    pending_moving_in_rebalance_jobs: u64,
    pending_moving_out_bck_rebuild_jobs: u64,
    pending_moving_out_fwd_rebuild_jobs: u64,
    pending_moving_out_normrebuild_jobs: u64,
    pending_moving_rebalance_jobs: u64,
    primary_read_bwc: BWC,
    primary_read_from_dev_bwc: BWC,
    primary_read_from_rmcache_bwc: BWC,
    primary_vac_in_kb: u64,
    primary_write_bwc: BWC,
    protected_vac_in_kb: u64,
    rebalance_per_receive_job_net_throttling_in_kbps: u64,
    rebalance_read_bwc: BWC,
    rebalance_wait_send_q_length: u64,
    rebalance_write_bwc: BWC,
    rebuild_per_receive_job_net_throttling_in_kbps: u64,
    rebuild_wait_send_q_length: u64,
    rfache_read_hit: u64,
    rfcache_avg_read_time: u64,
    rfcache_avg_write_time: u64,
    rfcache_fd_avg_read_time: u64,
    rfcache_fd_avg_write_time: u64,
    rfcache_fd_cache_overloaded: u64,
    rfcache_fd_inlight_reads: u64,
    rfcache_fd_inlight_writes: u64,
    rfcache_fd_io_errors: u64,
    rfcache_fd_monitor_error_stuck_io: u64,
    rfcache_fd_reads_received: u64,
    rfcache_fd_read_time_greater1_min: u64,
    rfcache_fd_read_time_greater1_sec: u64,
    rfcache_fd_read_time_greater500_millis: u64,
    rfcache_fd_read_time_greater5_sec: u64,
    rfcache_fd_writes_received: u64,
    rfcache_fd_write_time_greater1_min: u64,
    rfcache_fd_write_time_greater1_sec: u64,
    rfcache_fd_write_time_greater500_millis: u64,
    rfcache_fd_write_time_greater5_sec: u64,
    rfcache_io_errors: u64,
    rfcache_ios_outstanding: u64,
    rfcache_ios_skipped: u64,
    rfcache_poo_ios_outstanding: u64,
    rfcache_pool_continuos_mem: u64,
    rfcache_pool_evictions: u64,
    rfcache_pool_in_low_memory_condition: u64,
    rfcache_pool_io_time_greater1_min: u64,
    rfcache_pool_lock_time_greater1_sec: u64,
    rfcache_pool_low_resources_initiated_passthrough_mode: u64,
    rfcache_pool_max_io_size: u64,
    rfcache_pool_num_cache_devs: u64,
    rfcache_pool_num_of_driver_theads: u64,
    rfcache_pool_num_src_devs: u64,
    rfcache_pool_opmode: u64,
    rfcache_pool_pages_inuse: u64,
    rfcache_pool_page_size: u64,
    rfcache_pool_read_hit: u64,
    rfcache_pool_read_miss: u64,
    rfcache_pool_read_pending_g10_millis: u64,
    rfcache_pool_read_pending_g1_millis: u64,
    rfcache_pool_read_pending_g1_sec: u64,
    rfcache_pool_read_pending_g500_micro: u64,
    rfcache_pool_reads_pending: u64,
    rfcache_pool_size: u64,
    rfcache_pool_source_id_mismatch: u64,
    rfcache_pool_suspended_ios: u64,
    rfcache_pool_suspended_ios_max: u64,
    rfcache_pool_suspended_pequests_redundant_searchs: u64,
    rfcache_pool_write_hit: u64,
    rfcache_pool_write_miss: u64,
    rfcache_pool_write_pending: u64,
    rfcache_pool_write_pending_g10_millis: u64,
    rfcache_pool_write_pending_g1_millis: u64,
    rfcache_pool_write_pending_g1_sec: u64,
    rfcache_pool_write_pending_g500_micro: u64,
    rfcache_read_miss: u64,
    rfcache_reads_from_cache: u64,
    rfcache_reads_pending: u64,
    rfcache_reads_received: u64,
    rfcache_reads_skipped: u64,
    rfcache_reads_skipped_aligned_size_too_large: u64,
    rfcache_reads_skipped_heavy_load: u64,
    rfcache_reads_skipped_internal_error: u64,
    rfcache_reads_skipped_lock_ios: u64,
    rfcache_reads_skipped_low_resources: u64,
    rfcache_reads_skipped_max_io_size: u64,
    rfcache_reads_skipped_stuck_io: u64,
    rfcache_skipped_unlined_write: u64,
    rfcache_source_device_reads: u64,
    rfcache_source_device_writes: u64,
    rfcache_write_miss: u64,
    rfcache_write_pending: u64,
    rfcache_writes_received: u64,
    rfcache_writes_skipped_cache_miss: u64,
    rfcache_writes_skipped_heavy_load: u64,
    rfcache_writes_skipped_internal_error: u64,
    rfcache_writes_skipped_low_resources: u64,
    rfcache_writes_skipped_max_io_size: u64,
    rfcache_writes_skipped_stuck_io: u64,
    rmcache128kb_entry_count: u64,
    rmcache16kb_entry_count: u64,
    rmcache32kb_entry_count: u64,
    rmcache4kb_entry_count: u64,
    rmcache64kb_entry_count: u64,
    rmcache8kb_entry_count: u64,
    rmcache_big_block_eviction_count: u64,
    rmcache_big_block_eviction_size_count_in_kb: u64,
    rmcache_curr_num_of128kb_entries: u64,
    rmcache_curr_num_of16kb_entries: u64,
    rmcache_curr_num_of32kb_entries: u64,
    rmcache_curr_num_of4kb_entries: u64,
    rmcache_curr_num_of64kb_entries: u64,
    rmcache_curr_num_of8kb_entries: u64,
    rmcache_entry_eviction_count: u64,
    rmcache_entry_eviction_size_count_in_kb: u64,
    rmcache_no_eviction_count: u64,
    rmcache_size_in_use_in_kb: u64,
    rmcache_skip_count_cache_all_busy: u64,
    rmcache_skip_count_large_io: u64,
    rmcache_skip_count_unaligned4kb_io: u64,
    rm_pending_allocated_in_kb: u64,
    secondary_read_bwc: BWC,
    secondary_read_from_dev_bwc: BWC,
    secondary_read_from_rmcache_bwc: BWC,
    secondary_vac_in_kb: u64,
    secondary_write_bwc: BWC,
    semi_protected_vac_in_kb: u64,
    snap_capacity_in_use_in_kb: u64,
    snap_capacity_in_use_occupied_in_kb: u64,
    thick_capacity_in_use_in_kb: u64,
    thin_capacity_allocated_in_km: u64,
    thin_capacity_in_use_in_kb: u64,
    total_read_bwc: BWC,
    total_write_bwc: BWC,
    unreachable_unused_capacity_in_kb: u64,
    unused_capacity_in_kb: u64,
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct SdsObject {
    pub ip_list: Vec<IpObject>,
    pub on_vm_ware: bool,
    pub protection_domain_id: String,
    pub num_of_io_buffers: Option<u64>,
    pub fault_set_id: String,
    pub software_version_info: String,
    pub sds_state: DeviceState,
    pub membership_state: MembershipState,
    pub mdm_connection_state: MdmConnectionState,
    pub drl_mode: DrlMode,
    pub rmcache_enabled: bool,
    pub rmcache_size_in_kb: u64,
    pub rmcache_frozen: bool,
    pub rmcache_memory_allocation_state: MemoryAllocationState,
    pub rfcache_enabled: bool,
    pub maintenance_state: MaintenanceState,
    pub sds_decoupled: Option<OscillatingCounterWindow>,
    pub sds_configuration_failure: Option<OscillatingCounterWindow>,
    pub sds_receive_buffer_allocation_failures: Option<OscillatingCounterWindow>,
    pub rfcache_error_device_does_not_exist: bool,
    pub rfcache_error_low_resources: bool,
    pub rfcache_error_api_version_mismatch: bool,
    pub rfcache_error_inconsistent_cache_configuration: bool,
    pub rfcache_error_inconsistent_source_configuration: bool,
    pub rfcache_error_invalid_driver_path: bool,
    pub certificate_info: Option<CertificateInfo>,
    pub authentication_error: Option<AuthenticationError>,
    pub perf_profile: PerfProfile,
    pub name: String,
    pub port: u16,
    pub id: String,
    pub links: Vec<HashMap<String, String>>,
}

impl IntoPoint for SdsObject {
    fn into_point(&self, name: Option<&str>) -> Vec<TsPoint> {
        let mut p = TsPoint::new(name.unwrap_or("scaleio_sds"));
        p.add_field(
            "ip_list",
            TsValue::String(
                self.ip_list
                    .iter()
                    .map(|i| format!("{}", i.ip))
                    .collect::<Vec<String>>()
                    .join(","),
            ),
        );
        p.add_field("on_vm_ware", TsValue::Boolean(self.on_vm_ware));
        p.add_tag(
            "protection_domain_id",
            TsValue::String(self.protection_domain_id.clone()),
        );
        if let Some(buffers) = self.num_of_io_buffers {
            p.add_field("num_of_io_buffers", TsValue::Long(buffers));
        }
        p.add_tag("fault_set_id", TsValue::String(self.fault_set_id.clone()));
        p.add_tag(
            "software_version_info",
            TsValue::String(self.software_version_info.clone()),
        );
        p.add_field("sds_state", TsValue::String(self.sds_state.to_string()));
        p.add_field(
            "membership_state",
            TsValue::String(self.membership_state.to_string()),
        );
        p.add_field(
            "mdm_connection_state",
            TsValue::String(self.mdm_connection_state.to_string()),
        );
        p.add_field("drl_mode", TsValue::String(self.drl_mode.to_string()));
        p.add_field("rmcache_enabled", TsValue::Boolean(self.rmcache_enabled));
        p.add_field("rmcache_size_in_kb", TsValue::Long(self.rmcache_size_in_kb));
        p.add_field("rmcache_frozen", TsValue::Boolean(self.rmcache_frozen));
        p.add_field(
            "rmcache_memory_allocation_state",
            TsValue::String(self.rmcache_memory_allocation_state.to_string()),
        );
        p.add_field("rfcache_enabled", TsValue::Boolean(self.rfcache_enabled));
        p.add_field(
            "maintenance_state",
            TsValue::String(self.maintenance_state.to_string()),
        );
        //if let Some(counter) = self.sds_decoupled {
        //p.add_field("sds_decoupled", self.sds_decoupled: Option<OscillatingCounterWindow>);
        //}
        //if let Some(counter) = self.sds_configuration_failure {
        //p.add_field("sds_configuration_failure", self.sds_configuration_failure: Option<OscillatingCounterWindow>);
        //}
        //if let Some(counter) = self.sds_receive_buffer_allocation_failures {
        //counter.add_fields(&mut p);
        //p.add_field("sds_receive_buffer_allocation_failures", self.sds_receive_buffer_allocation_failures,
        //}
        p.add_field(
            "rfcache_error_device_does_not_exist",
            TsValue::Boolean(self.rfcache_error_device_does_not_exist),
        );
        p.add_field(
            "rfcache_error_low_resources",
            TsValue::Boolean(self.rfcache_error_low_resources),
        );
        p.add_field(
            "rfcache_error_api_version_mismatch",
            TsValue::Boolean(self.rfcache_error_api_version_mismatch),
        );
        p.add_field(
            "rfcache_error_inconsistent_cache_configuration",
            TsValue::Boolean(self.rfcache_error_inconsistent_cache_configuration),
        );
        p.add_field(
            "rfcache_error_inconsistent_source_configuration",
            TsValue::Boolean(self.rfcache_error_inconsistent_source_configuration),
        );
        p.add_field(
            "rfcache_error_invalid_driver_path",
            TsValue::Boolean(self.rfcache_error_invalid_driver_path),
        );

        if let Some(ref info) = self.certificate_info {
            p.add_field("certificate_subject", TsValue::String(info.subject.clone()));
            p.add_field("certificate_issuer", TsValue::String(info.issuer.clone()));
            p.add_field(
                "certificate_validfrom",
                TsValue::String(info.valid_from.clone()),
            );
            p.add_field(
                "certificate_validto",
                TsValue::String(info.valid_to.clone()),
            );
            p.add_field(
                "certificate_thumbprint",
                TsValue::String(info.thumbprint.clone()),
            );
            p.add_field(
                "certificate_validfrom_asn",
                TsValue::String(info.valid_from_asn1_format.clone()),
            );
            p.add_field(
                "certificate_validto_asn",
                TsValue::String(info.valid_to_asn1_format.clone()),
            );
        }

        if let Some(ref err) = self.authentication_error {
            p.add_field("authentication_error", TsValue::String(err.to_string()));
        }

        p.add_field(
            "perf_profile",
            TsValue::String(self.perf_profile.to_string()),
        );
        p.add_tag("name", TsValue::String(self.name.clone()));
        p.add_field("port", TsValue::Short(self.port));
        p.add_tag("id", TsValue::String(self.id.clone()));

        vec![p]
    }
}

#[test]
fn test_pool_response() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/scaleio/poolInstance.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();
    println!("buff: {}", buff);

    let i: PoolInstanceResponse = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct PoolInstanceResponse {
    pub rebuild_io_priority_policy: RebuildIoPriority,
    pub rebalance_io_priority_policy: RebuildIoPriority,
    pub rebuild_io_priority_num_of_concurrent_ios_per_device: Option<u64>,
    pub rebalance_io_priority_num_of_concurrent_ios_per_device: Option<u64>,
    pub rebuild_io_priority_bw_limit_per_device_in_kbps: Option<u64>,
    pub rebalance_io_priority_bw_limit_per_device_in_kbps: Option<u64>,
    pub rebuild_io_priority_app_iops_per_device_threshold: Option<u64>,
    pub rebalance_io_priority_app_iops_per_device_threshold: Option<u64>,
    pub rebuild_io_priority_app_bw_per_device_threshold_in_kbps: Option<u64>,
    pub rebalance_io_priority_app_bw_per_device_threshold_in_kbps: Option<u64>,
    pub rebuild_io_priority_quiet_period_in_msec: Option<u64>,
    pub rebalance_io_priority_quiet_period_in_msec: Option<u64>,
    pub zero_padding_enabled: bool,
    pub use_rmcache: bool,
    pub background_scanner_mode: BackgroundScannerMode,
    #[serde(rename = "backgroundScannerBWLimitKBps")]
    pub background_scanner_bw_limit_kbps: u64,
    pub protection_domain_id: String,
    pub spare_percentage: u8,
    pub rmcache_write_handling_mode: CacheWriteHandlingMode,
    pub checksum_enabled: bool,
    pub use_rfcache: bool,
    pub rebuild_enabled: bool,
    pub rebalance_enabled: bool,
    pub num_of_parallel_rebuild_rebalance_jobs_per_device: u16,
    pub capacity_alert_high_threshold: u8,
    pub capacity_alert_critical_threshold: u8,
    pub name: String,
    pub id: String,
    pub links: Vec<HashMap<String, String>>,
}

#[test]
fn test_sdc_objects() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/scaleio/sdc_info.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: Vec<Sdc> = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[serde(rename_all = "camelCase")]
#[derive(Clone, Deserialize, Debug, IntoPoint)]
pub struct Sdc {
    pub sdc_approved: bool,
    pub mdm_connection_state: String,
    pub memory_allocation_failure: Option<OscillatingCounterWindow>,
    pub socket_allocation_failure: Option<OscillatingCounterWindow>,
    pub sdc_guid: String,
    pub sdc_ip: String,
    pub perf_profile: String,
    pub version_info: Option<String>,
    pub system_id: String,
    pub name: Option<String>,
    pub id: String,
    pub links: Vec<HashMap<String, String>>,
}

#[test]
fn test_system_response() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/scaleio/systems.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();
    println!("buff: {}", buff);

    let i: Vec<System> = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize, IntoPoint)]
pub struct System {
    pub system_version_name: String,
    pub capacity_alert_high_threshold_percent: u16,
    pub capacity_alert_critical_threshold_percent: u16,
    pub remote_read_only_limit_state: bool,
    pub upgrade_state: String,
    pub mdm_management_port: u16,
    pub sdc_mdm_network_disconnections_counter_parameters: FailureCounter,
    pub sdc_sds_network_disconnections_counter_parameters: FailureCounter,
    pub sdc_memory_allocation_failures_counter_parameters: FailureCounter,
    pub sdc_socket_allocation_failures_counter_parameters: FailureCounter,
    pub sdc_long_operations_counter_parameters: FailureCounter,
    pub cli_password_allowed: bool,
    pub management_client_secure_communication_enabled: bool,
    pub tls_version: String,
    pub show_guid: bool,
    pub authentication_method: String,
    pub mdm_to_sds_policy: String,
    pub mdm_cluster: MdmCluster,
    pub perf_profile: PerfProfile,
    pub install_id: String,
    pub days_installed: u64,
    #[serde(deserialize_with = "deserialize_string_or_int")]
    pub max_capacity_in_gb: i64,
    pub capacity_time_left_in_days: String,
    pub enterprise_features_enabled: bool,
    pub is_initial_license: bool,
    pub default_is_volume_obfuscated: bool,
    pub restricted_sdc_mode_enabled: bool,
    pub swid: String,
    pub name: String,
    pub id: String,
    pub links: Vec<HashMap<String, String>>,
}

#[serde(rename_all = "camelCase")]
#[derive(Clone, Deserialize, Debug)]
pub struct TieBreaker {
    pub openssl_version: String,
    #[serde(rename = "managementIPs")]
    pub management_ips: Vec<String>,
    pub ips: Vec<String>,
    pub version_info: String,
    pub role: String,
    pub status: Option<String>,
    pub name: String,
    pub id: String,
    pub port: u16,
}

#[serde(rename_all = "camelCase")]
#[derive(Clone, Deserialize, Debug)]
pub struct Window {
    threshold: u64,
    window_size_in_sec: u64,
}

fn get<T>(client: &reqwest::Client, config: &ScaleioConfig, api: &str) -> MetricsResult<T>
where
    T: DeserializeOwned + Debug,
{
    let res: Result<T, reqwest::Error> = client
        .get(&format!("https://{}/api/{}", config.endpoint, api))
        .basic_auth(config.user.clone(), Some(config.password.clone()))
        .send()?
        .error_for_status()?
        .json();
    debug!("deserialized: {:?}", res);
    Ok(res?)
}

// Connect to the metadata server and request a new api token
pub fn get_api_token(client: &reqwest::Client, config: &ScaleioConfig) -> MetricsResult<String> {
    let mut token = client
        .get(&format!("https://{}/api/login", config.endpoint))
        .basic_auth(config.user.clone(), Some(config.password.clone()))
        .send()?
        .error_for_status()?;
    let t = token.text()?;
    trace!("api token: {}", t);

    match api_token(t.as_bytes()) {
        IResult::Done(_, o) => Ok(o.into()),
        IResult::Incomplete(_) => Err(StorageError::new(format!(
            "Unable to parse api token {} from server",
            t
        ))),
        IResult::Error(e) => Err(StorageError::new(e.to_string())),
    }
}

// Get the basic cluster configuration
pub fn get_configuration(
    client: &reqwest::Client,
    config: &ScaleioConfig,
) -> MetricsResult<SystemConfig> {
    // Ask scaleio for the system configuration information
    let sys_config = get::<SystemConfig>(&client, &config, "Configuration")?;
    Ok(sys_config)
}

// Dump all drive information.  Call get_sds_object afterwards to turn the sdsId into
// more useful information
pub fn get_drive_instances(
    client: &reqwest::Client,
    config: &ScaleioConfig,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let instances =
        get::<Vec<Instance>>(&client, &config, "types/Device/instances").and_then(|instance| {
            let points: Vec<TsPoint> = instance
                .iter()
                .flat_map(|instance| instance.into_point(Some("scaleio_drive")))
                .map(|mut point| {
                    point.timestamp = Some(t);
                    point
                })
                .collect();
            Ok(points)
        })?;
    Ok(instances)
}

pub fn get_drive_ids(
    client: &reqwest::Client,
    config: &ScaleioConfig,
) -> MetricsResult<Vec<String>> {
    let instance_ids =
        get::<Vec<Instance>>(&client, &config, "types/Device/instances").and_then(|instances| {
            let ids = instances
                .iter()
                .map(|instance| instance.id.clone())
                .collect::<Vec<String>>();
            Ok(ids)
        })?;
    Ok(instance_ids)
}

pub fn get_sds_ids(client: &reqwest::Client, config: &ScaleioConfig) -> MetricsResult<Vec<String>> {
    let sds_ids =
        get::<Vec<SdsObject>>(&client, &config, "types/Sds/instances").and_then(|sds_objects| {
            let ids = sds_objects
                .iter()
                .map(|sds| sds.id.clone())
                .collect::<Vec<String>>();
            Ok(ids)
        })?;

    Ok(sds_ids)
}

pub fn get_sds_statistics(
    client: &reqwest::Client,
    config: &ScaleioConfig,
    t: DateTime<Utc>,
    sds_id: &str,
) -> MetricsResult<Vec<TsPoint>> {
    let instance_statistics = get::<SdsStatistics>(
        &client,
        &config,
        &format!("instances/Sds::{}/relationships/Statistics", sds_id),
    )
    .and_then(|instance| {
        let points: Vec<TsPoint> = instance
            .into_point(Some("scaleio_sds_stat"))
            .iter_mut()
            .map(|point| {
                point.timestamp = Some(t);
                point.add_tag("sds_id", TsValue::String(sds_id.to_string()));
                point.clone()
            })
            .collect();
        Ok(points)
    })?;

    Ok(instance_statistics)
}

pub fn get_drive_statistics(
    client: &reqwest::Client,
    config: &ScaleioConfig,
    t: DateTime<Utc>,
    device_id: &str,
) -> MetricsResult<Vec<TsPoint>> {
    let instance_statistics = get::<DeviceStatistics>(
        &client,
        &config,
        &format!("instances/Device::{}/relationships/Statistics", device_id),
    )
    .and_then(|instance| {
        let points: Vec<TsPoint> = instance
            .into_point(Some("scaleio_drive_stat"))
            .iter_mut()
            .map(|point| {
                point.timestamp = Some(t);
                point.add_tag("device_id", TsValue::String(device_id.to_string()));
                point.clone()
            })
            .collect();
        Ok(points)
    })?;

    Ok(instance_statistics)
}

// Get all the drive stats.  This hashmap is referenced by sdsId.
pub fn get_drive_stats(
    client: &reqwest::Client,
    config: &ScaleioConfig,
) -> MetricsResult<SelectedStatisticsResponse> {
    let stats_req = SelectedStatisticsRequest {
        selected_statistics_list: vec![StatsRequest {
            req_type: StatsRequestType::Device,
            all_ids: vec![],
            properties: vec![
                // TODO: Change this into an enum
                "fixedReadErrorCount".into(),
                "avgReadSizeInBytes".into(),
                "avgWriteSizeInBytes".into(),
                "avgReadLatencyInMicrosec".into(),
                "avgWriteLatencyInMicrosec".into(),
            ],
        }],
    };

    // Contact scaleio metadata server and parse the results
    // back into json.  If the call isn't an http success result
    // then return an error
    let mut resp = client
        .post(&format!(
            "https://{}/api/instances/querySelectedStatistics",
            config.endpoint
        ))
        .header(CONTENT_TYPE, "application/json")
        .basic_auth(config.user.clone(), Some(config.password.clone()))
        .json(&stats_req)
        .send()?
        .error_for_status()?;
    let json_resp: SelectedStatisticsResponse = resp.json()?;
    Ok(json_resp)
}

/// Gets all instances
pub fn get_instances(client: &reqwest::Client, config: &ScaleioConfig) -> MetricsResult<()> {
    let instances = client
        .get(&format!("https://{}/api/instances", config.endpoint,))
        .basic_auth(config.user.clone(), Some(config.password.clone()))
        .send()?
        .error_for_status()?
        .text()?;
    println!("instances: {}", instances);

    Ok(())
}

#[test]
fn test_api_token_parser() {
    let raw_token = "\"YXV0b21hdGlvbjoxNTE1MTk4NjYzNDg0OjJiOWFhODhiYzliY2Y5O\
                     WU3OTc1OGVjMmM0MzgyZGE0\"";
    let expected = "YXV0b21hdGlvbjoxNTE1MTk4NjYzNDg0OjJiOWFhODhi\
                    YzliY2Y5OWU3OTc1OGVjMmM0MzgyZGE0";
    let res = api_token(raw_token.as_bytes());
    println!("parsed api_token: {:?}", res);
    assert_eq!(
        api_token(raw_token.as_bytes()),
        IResult::Done(&b""[..], expected)
    );
}

// We parse any value surrounded by quotes, ignoring all whitespaces around those
named!(
    api_token<&str>,
    ws!(delimited!(
        tag!("\""),
        map_res!(take_until!("\""), str::from_utf8),
        tag!("\"")
    ))
);

pub fn get_pool_info(
    client: &reqwest::Client,
    config: &ScaleioConfig,
    pool_id: &str,
) -> MetricsResult<PoolInstanceResponse> {
    let pool_info = get::<PoolInstanceResponse>(
        &client,
        &config,
        &format!("instances/StoragePool::{}", pool_id),
    )?;
    Ok(pool_info)
}

pub fn get_pool_stats(
    client: &reqwest::Client,
    config: &ScaleioConfig,
) -> MetricsResult<ClusterSelectedStatisticsResponse> {
    let stats_req = SelectedStatisticsRequest {
        selected_statistics_list: vec![StatsRequest {
            req_type: StatsRequestType::StoragePool,
            all_ids: vec![],
            properties: vec![
                "numOfDevices".into(),
                "numOfVolumes".into(),
                "capacityLimitInKb".into(),
                "thickCapacityInUseInKb".into(),
                "thinCapacityInUseInKb".into(),
                "primaryReadBwc".into(),
                "primaryWriteBwc".into(),
                "secondaryReadBwc".into(),
                "secondaryWriteBwc".into(),
                "totalReadBwc".into(),
                "totalWriteBwc".into(),
                "thinCapacityAllocatedInKm".into(),
            ],
        }],
    };

    // Contact scaleio metadata server and parse the results
    // back into json.  If the call isn't an http success result
    // then return an error
    let mut resp = client
        .post(&format!(
            "https://{}/api/instances/querySelectedStatistics",
            config.endpoint
        ))
        .header(CONTENT_TYPE, "application/json")
        .basic_auth(config.user.clone(), Some(config.password.clone()))
        .json(&stats_req)
        .send()?
        .error_for_status()?;
    let json_resp: ClusterSelectedStatisticsResponse = resp.json()?;
    Ok(json_resp)
}

pub fn get_sdc_objects(
    client: &reqwest::Client,
    config: &ScaleioConfig,
    system_id: &str,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let sdc_info = get::<Vec<Sdc>>(
        &client,
        &config,
        &format!("instances/System::{}/relationships/Sdc", system_id),
    )
    .and_then(|sdc_objects| {
        let points: Vec<TsPoint> = sdc_objects
            .iter()
            .flat_map(|sdc| sdc.into_point(Some("scaleio_sdc")))
            .map(|mut point| {
                point.timestamp = Some(t);
                point
            })
            .collect();
        Ok(points)
    })?;
    Ok(sdc_info)
}

// Use this to gather more information about the sds device like
// ip address, state, storage server attached to, etc
pub fn get_sds_object(
    client: &reqwest::Client,
    config: &ScaleioConfig,
    sds_id: &str,
) -> MetricsResult<SdsObject> {
    let sds_object = get::<SdsObject>(&client, &config, &format!("instances/Sds::{}", sds_id))?;
    Ok(sds_object)
}

pub fn get_sds_objects(
    client: &reqwest::Client,
    config: &ScaleioConfig,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let sds_info =
        get::<Vec<SdsObject>>(&client, &config, "types/Sds/instances").and_then(|sds_objects| {
            let points: Vec<TsPoint> = sds_objects
                .iter()
                .flat_map(|sds| sds.into_point(Some("scaleio_sds")))
                .map(|mut point| {
                    point.timestamp = Some(t);
                    point
                })
                .collect();
            Ok(points)
        })?;
    Ok(sds_info)
}

pub fn get_system(
    client: &reqwest::Client,
    config: &ScaleioConfig,
    system_id: &str,
) -> MetricsResult<System> {
    let system = get::<System>(
        &client,
        &config,
        &format!("instances/System::{}", system_id),
    )?;
    Ok(system)
}

pub fn get_systems(client: &reqwest::Client, config: &ScaleioConfig) -> MetricsResult<Vec<System>> {
    let systems = get::<Vec<System>>(&client, &config, "types/System/instances")?;
    Ok(systems)
}

pub fn get_version(client: &reqwest::Client, config: &ScaleioConfig) -> MetricsResult<String> {
    let version = client
        .get(&format!("https://{}/api/version", config.endpoint))
        .basic_auth(config.user.clone(), Some(config.password.clone()))
        .send()?
        .error_for_status()?
        .text()?;
    Ok(version)
}

pub fn get_volumes(
    client: &reqwest::Client,
    config: &ScaleioConfig,
    t: DateTime<Utc>,
) -> MetricsResult<Vec<TsPoint>> {
    let sds_vols =
        get::<Vec<SdsVolume>>(&client, &config, "types/Volume/instances").and_then(|sds_vols| {
            let points: Vec<TsPoint> = sds_vols
                .iter()
                .flat_map(|vol| vol.into_point(Some("scaleio_volume")))
                .map(|mut point| {
                    point.timestamp = Some(t);
                    point
                })
                .collect();
            Ok(points)
        })?;
    Ok(sds_vols)
}

#[derive(Serialize, Debug)]
pub enum VolumeRequestType {
    ThinProvisioned,
    ThickProvisioned,
}

#[serde(rename_all = "camelCase")]
#[derive(Serialize, Debug)]
pub struct VolumeRequest {
    pub volume_size_in_kb: String,
    pub storage_pool_id: String,
    pub name: String,
    pub volume_type: VolumeRequestType,
    pub use_rmcache: bool,
}

impl VolumeRequest {
    fn new(volume_size_int: u64, storage_pool_id: String, name: String) -> VolumeRequest {
        VolumeRequest {
            volume_size_in_kb: volume_size_int.to_string(),
            storage_pool_id,
            name,
            volume_type: VolumeRequestType::ThinProvisioned,
            use_rmcache: true,
        }
    }
}

/// Finds the ideal pools where volumes need to be created.
/// Returns a vector of pool_ids, will never return an empty list
/// Available space and percent provisioned are considered
/// when identifying the pools.
fn identify_ideal_pools(
    mut storage_pools: Vec<PoolInstanceResponse>,
    num_of_pools: usize,
    spare_cutoff: u8,
) -> MetricsResult<Vec<String>> {
    if !storage_pools.is_empty() {
        let mut ids: Vec<String> = Vec::new();

        // Retain only those pools which pass the cutoff
        storage_pools.retain(|ref each| each.spare_percentage > spare_cutoff);
        if storage_pools.is_empty() {
            // None have enough spare space
            return Err(StorageError::new(format!(
                "All storage pools are above
                                    cutoff of {}%",
                spare_cutoff
            )));
        }
        // Now reverse sort storage_pools based on the 'spare_percentage' field
        storage_pools.sort_unstable_by(|a, b| b.spare_percentage.cmp(&a.spare_percentage));

        let available_pools: usize = if storage_pools.len() < num_of_pools {
            storage_pools.len()
        } else {
            num_of_pools
        };

        for i in 0..available_pools {
            if let Some(element) = storage_pools.get(i) {
                let pool_id = &element.id;
                ids.push(pool_id.to_string());
            }
        }
        if !ids.is_empty() {
            Ok(ids)
        } else {
            // storage_pools is not empty, but did not find any elements?
            Err(StorageError::new(
                "Failed to identify ideal pool to create volume".to_string(),
            ))
        }
    } else {
        Err(StorageError::new("No storage pools found".to_string()))
    }
}

/// Creates a volume on the given endpoint using the credentials specified
/// in the config file. Automatically selects a storage pool
/// vol_name_prefix refers to the tracking ID/ticket ID of the request
pub fn create_volume(
    client: &reqwest::Client,
    config: &ScaleioConfig,
    vol_name_prefix: &str,
    requested_size_in_kb: u64,
    num_of_luns: usize,
    mut spare_cutoff: u8,
) -> MetricsResult<Vec<String>> {
    // Set minimum cut off
    if spare_cutoff <= 10 {
        spare_cutoff = 10
    }

    // First, get a list of available pools
    let storage_pools =
        get::<Vec<PoolInstanceResponse>>(&client, &config, "types/StoragePool/instances")?;

    // don't need storage_pools later on, OK to move
    let pool_ids: Vec<String> = identify_ideal_pools(storage_pools, num_of_luns, spare_cutoff)?;

    // Could be more defensive and check if pool_ids is empty.
    // identify_ideal_pools() should ideally return an error in that case.
    // So, skip that check.
    if pool_ids.len() < num_of_luns {
        debug!(
            "Cannot create volumes in {} pools, creating in {} instead",
            num_of_luns,
            pool_ids.len()
        );
    }
    // Create each volume with sizes balanced over pools
    let each_vol_size_in_kb = requested_size_in_kb / (pool_ids.len() as u64);

    let mut volume_ids: Vec<String> = Vec::new();

    for (vol_num, pool_id) in pool_ids.iter().enumerate() {
        debug!(
            "Creating volume of size {} in pool with ID {}",
            each_vol_size_in_kb,
            pool_id.to_string()
        );
        let vol_creation_req = VolumeRequest::new(
            each_vol_size_in_kb,
            pool_id.to_string(),
            format!("{}_{}", vol_name_prefix, vol_num),
        );
        // post a request to endpoint to create a volume. If call isn't
        // an http success result, return an error. Return is newly created volume ID
        let mut vol_creation_resp = client
            .post(&format!(
                "https://{}/api/types/Volume/instances",
                config.endpoint
            ))
            .header(CONTENT_TYPE, "application/json")
            .basic_auth(config.user.clone(), Some(config.password.clone()))
            .json(&vol_creation_req)
            .send()?
            .error_for_status()?;
        let json_resp: String = vol_creation_resp.json()?;
        volume_ids.push(json_resp);
    }

    // Did we succeed in creating as many as intended?
    if volume_ids.len() != pool_ids.len() {
        debug!(
            "Created only {} volumes. {} intended",
            volume_ids.len(),
            pool_ids.len()
        );
        debug!("Request is not met in full");
        // TODO: Rollback/delete these volumes without mapping?
    }
    Ok(volume_ids)
}

/// Returns the sdcId corresponding to the given name
fn get_sdc_id_from_name(
    client: &reqwest::Client,
    config: &ScaleioConfig,
    sdc_name: &str,
) -> MetricsResult<String> {
    // get a list of all sdc's, filter entry that matches sdc_name
    // and return corresponding sdc_id

    debug!("Retrieving SDC ID for {}", sdc_name);
    let sdc_info =
        get::<Vec<Sdc>>(&client, &config, "api/types/Sdc/instances").and_then(|sdc_objects| {
            let ids: Vec<String> = sdc_objects
                .iter()
                .filter(|sdc| match sdc.name {
                    Some(ref name) => name == sdc_name,
                    None => false,
                })
                .map(|sdc| sdc.id.clone())
                .collect::<Vec<String>>();
            Ok(ids)
        })?;

    if !sdc_info.is_empty() {
        if let Some(id) = sdc_info.get(0) {
            Ok(id.to_string())
        } else {
            Err(StorageError::new(format!(
                "SDC ID not found for {}",
                sdc_name
            )))
        }
    } else {
        Err(StorageError::new(format!(
            "SDC ID not found for {}",
            sdc_name
        )))
    }
}

/// Maps all the volumes in the list to the given sdc
/// Also sets iops and bandwidth limits
pub fn map_volumes(
    client: &reqwest::Client,
    config: &ScaleioConfig,
    volume_ids: &[String],
    sdc_name: &str,
) -> MetricsResult<bool> {
    // Get sdc_id from sdc_name
    let sdc_id = get_sdc_id_from_name(client, config, sdc_name)?;
    debug!("SDC ID for {} is {}", sdc_name, sdc_id);

    for vol_id in volume_ids {
        debug!("Mapping {} to {}", vol_id, sdc_id);

        let mut sdc_map = HashMap::new();
        sdc_map.insert("sdcId", sdc_id.clone());

        // TODO: allow multiple mappings?

        // Returns only http status of success or failure
        let mut _resp = client
            .post(&format!(
                "https://{}/api/instances/Volume::{}/action/addMappedSdc",
                config.endpoint, vol_id
            ))
            .header(CONTENT_TYPE, "application/json")
            .basic_auth(config.user.clone(), Some(config.password.clone()))
            .json(&sdc_map)
            .send()?
            .error_for_status()?;

        let mut sdc_limits = HashMap::new();
        sdc_limits.insert("sdcId", sdc_id.clone());

        if let Some(limit) = config.bandwidth_limit {
            sdc_limits.insert("bandwidthLimitInKbps", limit.to_string());
        } else {
            sdc_limits.insert("bandwidthLimitInKbps", "0".to_string());
        }
        if let Some(limit) = config.iops_limit {
            sdc_limits.insert("iopsLimit", limit.to_string());
        } else {
            sdc_limits.insert("iopsLimit", "0".to_string());
        }

        debug!("Adding bandwidth limits to volume with ID {}", vol_id);
        let mut _resp = client
            .post(&format!(
                "https://{}/api/instances/Volume::{}/action/setMappedSdcLimits",
                config.endpoint, vol_id
            ))
            .header(CONTENT_TYPE, "application/json")
            .basic_auth(config.user.clone(), Some(config.password.clone()))
            .json(&sdc_limits)
            .send()?
            .error_for_status()?;
    }
    Ok(true)
}
/* Uncomment this when this test should run
#[test]
fn test_create_and_map_volume() {
    use self::serde_json;
    use self::simplelog::{Config, TermLogger};
    use std::fs::File;
    use std::io::{Error as ioError, ErrorKind};

    // read config file
    TermLogger::new(log::LevelFilter::Debug, Config::default()).unwrap();
    let config_file = "/newDevice/tests/scaleio_wc.json".to_string();
    let config_file_fd = File::open(&config_file).unwrap();

    let config: serde_json::Value = serde_json::from_reader(config_file_fd)
        .map_err(|e| ioError::new(ErrorKind::InvalidData, e.to_string()))
        .unwrap();

    debug!("Read file");
    let mut scaleio_config = ScaleioConfig {
        user: config["username"]
            .as_str()
            .expect("User name is missing")
            .to_string(),
        password: config["password"]
            .as_str()
            .expect("password is missing")
            .to_string(),
        endpoint: config["endpoint"]
            .as_str()
            .expect("endpoint is missing")
            .to_string(),
        region: config["region"]
            .as_str()
            .expect("region is missing")
            .to_string(),
        bandwidth_limit: {
            if let Some(b) = config["bandwidth"].as_u64() {
                Some(b)
            } else {
                // unspecified, set unlimited
                Some(0)
            }
        },
        iops_limit: {
            if let Some(c) = config["iops"].as_u64() {
                Some(c)
            } else {
                // unspecified, set unlimited
                Some(0)
            }
        },
        certificate: None,
    };

    debug!("Config is {:#?}", scaleio_config);
    let vol_size: u64 = config["total_size"].as_u64().unwrap();
    let luns = config["no_of_luns"].as_u64().unwrap();
    let sdc_hostname = config["sdc_hostname"]
        .as_str()
        .expect("SDC hostname is missing");
    let spare_cutoff: u8 = config["spare_cutoff"].as_u8().unwrap();

    let web_client = reqwest::Client::builder().build().unwrap();

    let token = get_api_token(&web_client, &scaleio_config).unwrap();
    // valid 10 mins
    scaleio_config.password = token;

    let vols = create_volume(
        &web_client,
        &scaleio_config,
        "LIBTEST",
        vol_size,
        luns as usize,
        spare_cutoff,
    ).unwrap();
    println!("{:#?}", vols);

    map_volumes(&web_client, &scaleio_config, vols, &sdc_hostname).unwrap();
} */
