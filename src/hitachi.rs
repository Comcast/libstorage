#![allow(non_snake_case)]
use crate::error::{MetricsResult, StorageError};

use std::collections::HashMap;
use std::fmt;
use std::str;
use std::str::FromStr;

use crate::ir::{TsPoint, TsValue};
use chrono::offset::Utc;
use chrono::DateTime;
use csv::Reader;
use reqwest::header::ACCEPT;
use serde_json::{Number, Value};

#[serde(rename_all = "UPPERCASE")]
#[derive(Deserialize, Debug)]
pub enum BlockingMode {
    /// Full or blockade
    Fb,
    /// No blocking
    Nb,
    /// Pool full
    Pf,
    /// Pool vol Blockade
    Pb,
}

impl fmt::Display for BlockingMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BlockingMode::Fb => write!(f, "FB"),
            BlockingMode::Nb => write!(f, "NB"),
            BlockingMode::Pb => write!(f, "PB"),
            BlockingMode::Pf => write!(f, "PF"),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Collection {
    pub items: Vec<HashMap<String, serde_json::Value>>,
}

#[serde(rename_all = "snake_case")]
#[derive(Debug, Serialize)]
pub enum DataReductionMode {
    Compression,
    CompressionDeduduplication,
    Disabled,
}

impl fmt::Display for DataReductionMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataReductionMode::Compression => write!(f, "compression"),
            DataReductionMode::CompressionDeduduplication => write!(f, "compression_deduplication"),
            DataReductionMode::Disabled => write!(f, "disabled"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DriveType {
    Sas,
    Sata,
    #[serde(rename = "SSD(FMC)")]
    SsdFmc,
    #[serde(rename = "SSD(FMD)")]
    SsdFmd,
    #[serde(rename = "SSD(MLC)")]
    SsdMlc,
    #[serde(rename = "SSD(SLC)")]
    SsdSlc,
    Ssd,
}

impl fmt::Display for DriveType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DriveType::Sas => write!(f, "SAS"),
            DriveType::Sata => write!(f, "SATA"),
            DriveType::SsdFmc => write!(f, "SSD(FMC)"),
            DriveType::SsdFmd => write!(f, "SSD(FMD)"),
            DriveType::SsdMlc => write!(f, "SSD(MLC)"),
            DriveType::SsdSlc => write!(f, "SSD(SLC)"),
            DriveType::Ssd => write!(f, "SSD"),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DetailInfoType {
    Fmc,
    #[serde(rename = "SSD(FMC)")]
    SsdFmc,
}

impl fmt::Display for DetailInfoType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DetailInfoType::Fmc => write!(f, "FMC"),
            DetailInfoType::SsdFmc => write!(f, "SSD(FMC)"),
        }
    }
}
/*
#[derive(Debug, Deserialize)]
pub struct RaidType {
    pub data: (u8, Option<u8>),
    pub parity: Option<u8>,
    /*
    Specify one of the following values:
    2D+2D
    3D+1P
    4D+1P
    6D+1P
    7D+1P
    6D+2P
    12D+2P
    14D+2P
    */
}

impl serde::Serialize for RaidType {
fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
where
S: serde::ser::Serializer,
{
//serializer.serialize_str(format!("{}D+{}P"))
}
}
*/

#[derive(Deserialize, Debug)]
pub struct ParityGroup {
    parityGroupId: String,
    numOfLdevs: i32,
    usedCapacityRate: i32,
    availableVolumeCapacity: u64,
    raidLevel: String,
    raidType: String,
    clprId: i32,
    driveType: String,
    driveTypeName: DriveType,
    driveSpeed: i32,
    totalCapacity: u64,
    physicalCapacity: u64,
    isAcceleratedCompressionEnabled: bool,
}

#[derive(Deserialize, Debug)]
pub struct ParityGroupResp {
    pub data: Vec<ParityGroup>,
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct DataIncludingSystemDatum {
    pub is_reduction_capacity_available: bool,
    pub is_reduction_rate_available: bool,
    pub reduction_capacity: u64,
    pub reduction_rate: i64,
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct DataExcludingSystemDatum {
    pub compressed_capacity: u64,
    pub deduped_capacity: u64,
    pub pre_used_capacity: u64,
    pub pre_compressed_capacity: u64,
    pub pre_dedupred_capacity: u64,
    pub reclaimed_capacity: u64,
    pub system_data_capacity: u64,
    pub used_virtual_volume_capacity: u64,
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug)]
pub struct DataPort {
    pub port_id: String,
    pub host_group_number: i64,
    pub host_group_name: String,
    pub lun: i64,
}

#[serde(rename_all = "camelCase")]
#[derive(Debug, Deserialize)]
pub struct Datum {
    pub available_physical_volume_capacity: u64,
    pub available_volume_capacity: u64,
    pub blocking_mode: BlockingMode,
    pub capacities_excluding_system_data: DataExcludingSystemDatum,
    pub compression_rate: i64,
    pub data_reduction_accelerate_comp_capacity: u64,
    pub data_reduction_accelerate_comp_including_system_data: DataIncludingSystemDatum,
    pub data_reduction_accelerate_comp_rate: i64,
    pub data_reduction_before_capacity: u64,
    pub data_reduction_capacity: u64,
    pub data_reduction_including_system_data: DataIncludingSystemDatum,
    pub data_reduction_rate: i64,
    pub depletion_threshold: i64,
    pub duplication_ldev_ids: Vec<i64>,
    pub duplication_number: i64,
    pub duplication_rate: i64,
    pub first_ldev_id: i64,
    pub is_mainframe: bool,
    pub is_shrinking: bool,
    pub located_volume_count: i64,
    pub num_of_ldevs: i64,
    pub pool_id: i64,
    pub pool_name: String,
    pub pool_status: PoolStatus,
    pub pool_type: PoolType,
    pub reserved_volume_count: i64,
    pub snapshot_count: i64,
    /// Total size of snapshot data mapped to pool (MB)
    pub snapshot_used_capacity: u64,
    pub suspend_snapshot: bool,
    /// Total capacity of the DP Volumes mapped to the pool (MB)
    pub total_located_capacity: u64,
    pub total_physical_capacity: u64,
    pub total_pool_capacity: u64,
    pub total_reserved_capacity: u64,
    /// Usage rate of logical capacity in %
    pub used_capacity_rate: i64,
    /// Usage rate of physical capacity in %
    pub used_physical_capacity_rate: i64,
    pub virtual_volume_capacity_rate: i64,
    pub warning_threshold: i64,
}

#[derive(Debug)]
pub enum LdevOption {
    /// Gets information about implemented LDEVs.
    Defined,
    /// Gets information about DP volumes.
    DpVolume,
    /// Gets information about external volumes.
    ExternalVolume,
    ///Gets information about LDEVs for which LU paths are defined.
    LuMapped,
    /// Gets information about LDEVs for which LU paths are undefined.
    LuUnmapped,
    /// Gets information about LDEVs that are not implemented.
    Undefined,
}

impl fmt::Display for LdevOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LdevOption::Defined => write!(f, "defined"),
            LdevOption::DpVolume => write!(f, "dpVolume"),
            LdevOption::ExternalVolume => write!(f, "externalVolume"),
            LdevOption::LuMapped => write!(f, "luMapped"),
            LdevOption::LuUnmapped => write!(f, "luUnmapped"),
            LdevOption::Undefined => write!(f, "undefined"),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Pools {
    pub data: Vec<Datum>,
}
#[serde(rename_all = "UPPERCASE")]
#[derive(Debug, Deserialize)]
pub enum PoolStatus {
    /// Pool is normal
    PolN,
    /// The pool is in overflow status exceeding the threshold
    PolF,
    /// The pool is in overflow status exceeding the threshold and is suspended
    PolS,
    /// The pool is suspended in failure status.  If pool is POLE then
    /// info cannot be obtained
    PolE,
}

impl fmt::Display for PoolStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PoolStatus::PolN => write!(f, "POLN"),
            PoolStatus::PolF => write!(f, "POLF"),
            PoolStatus::PolS => write!(f, "POLS"),
            PoolStatus::PolE => write!(f, "POLE"),
        }
    }
}

#[serde(rename_all = "UPPERCASE")]
#[derive(Debug, Deserialize)]
pub enum PoolType {
    /// Data direct mapped HDP pool
    Dm,
    /// Dynamic Provisioning
    Dp,
    Hdp,
    Hdt,
    /// Hitachi thin image
    Hti,
    /// Active flash pool
    Rt,
}

impl fmt::Display for PoolType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PoolType::Dm => write!(f, "DM"),
            PoolType::Dp => write!(f, "DP"),
            PoolType::Hdp => write!(f, "HDP"),
            PoolType::Hdt => write!(f, "HDT"),
            PoolType::Hti => write!(f, "HTI"),
            PoolType::Rt => write!(f, "RT"),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Version {
    pub product_name: String,
    pub product_version: String,
    pub api_version: String,
    pub description: String,
}

#[serde(rename_all = "UPPERCASE")]
#[derive(Deserialize, Debug)]
pub enum LdevAttribute {
    /// Volume Migration volume
    Alun,
    /// Cache LUN (DCR)
    Clun,
    /// Command device
    Cmd,
    /// CVS volume
    Cvs,
    /// External volume
    Elun,
    /// Encrypted disk
    Encd,
    /// global-active device volume
    Gad,
    /// HDP volume or Dynamic Provisioning for Mainframe volume
    Hdp,
    /// HDT volume
    Hdt,
    /// Volume used as the system LU of NAS Platform
    Hnass,
    /// Volume used as a user LU of NAS Platform
    Hnasu,
    /// Pair volume (P-VOL or S-VOL) for
    /// remote copy (TrueCopy,TrueCopy for
    /// Mainframe, Universal Replicator, Universal
    /// Replicator for Mainframe)
    Horc,
    /// Thin Image volume (P-VOL or S-VOL)
    Hti,
    /// Journal volume
    Jnl,
    /// ShadowImage volume (P-VOL or S-VOL)
    Mrcf,
    /// OpenLDEV Guard volume
    Olg,
    /// Pool volume
    Pool,
    /// Quorum disk
    Qrd,
    /// Remote command device
    Rcmd,
    /// System disk
    Sysd,
    /// Volume for which the T10 PI attribute is enabled
    T10pi,
    /// HDP volume used for FCSE
    Tse,
    /// Virtual volume
    Vvol,
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug)]
pub struct VolumeDatum {
    pub attributes: Vec<LdevAttribute>,
    pub block_capacity: i64,
    pub byte_format_capacity: String,
    pub clpr_id: i64,
    pub data_reduction_mode: String,
    pub data_reduction_status: String,
    pub emulation_type: String,
    pub is_full_allocation_enabled: bool,
    pub is_alua_enabled: bool,
    pub label: String,
    pub ldev_id: i64,
    pub mp_blade_id: i64,
    pub num_of_ports: i64,
    pub num_of_used_block: i64,
    pub operation_type: Option<VolumeOperationType>,
    pub pool_id: i64,
    pub ports: Vec<DataPort>,
    pub resource_group_id: i64,
    pub ssid: String,
    pub status: VolumeStatus,
}

#[serde(rename_all = "UPPERCASE")]
#[derive(Deserialize, Debug)]
pub enum VolumeOperationType {
    /// Collection access is in progress.
    Caccs,
    /// Collection copying is in progress.
    Ccopy,
    /// Formatting is in progress.
    Fmt,
    /// Quick formatting is in progress.
    Qfmt,
    /// Pools are being rebalanced.
    Rbl,
    /// Pools are being reallocated.
    Rlc,
    /// Shredding is in progress.
    Shred,
    ///Deletion from the pool is in progress.
    Shrpl,
    /// Pages are being released.
    Zpd,
}

#[serde(rename_all = "UPPERCASE")]
#[derive(Deserialize, Debug)]
pub enum VolumeStatus {
    /// The LDEV is blocked.
    Blk,
    /// The LDEV status is being changed.
    Bsy,
    /// The LDEV is in normal status.
    Nml,
    /// The LDEV status is unknown (not supported).
    #[serde(rename = "Unknown")]
    Unknown,
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug)]
pub struct Volumes {
    pub data: Vec<VolumeDatum>,
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

#[test]
fn test_pools() {
    let f = include_str!("../tests/hitachi/pool.json");
    let v: Pools = serde_json::from_str(f).unwrap();
    println!("Result: {:?}", v);
}

#[test]
fn test_volumes() {
    let f = include_str!("../tests/hitachi/volume.json");
    let v: Volumes = serde_json::from_str(f).unwrap();
    println!("Result: {:?}", v);
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

/// Capacity is specified in GB
pub fn create_pool(
    client: &reqwest::Client,
    config: &HitachiConfig,
    storage_device_id: &str,
    capacity: u64,
    ldev_id: Option<i32>,
    parity_group_id: Option<&str>,
    pool_id: Option<i32>,
    data_reduction_mode: Option<DataReductionMode>,
) -> MetricsResult<()> {
    let api_call = format!("v1/objects/storages/{}/ldevs", storage_device_id);
    let mut params: HashMap<String, Value> = HashMap::new();
    params.insert(
        "byteFormatCapacity".to_string(),
        Value::String(format!("{}G", capacity)),
    );
    if let Some(ldev_id) = ldev_id {
        params.insert("ldevId".to_string(), Value::Number(<Number>::from(ldev_id)));
    }
    if let Some(parity_group_id) = parity_group_id {
        params.insert(
            "parityGroupId".to_string(),
            Value::String(parity_group_id.to_string()),
        );
    }
    if let Some(pool_id) = pool_id {
        params.insert("poolId".to_string(), Value::Number(<Number>::from(pool_id)));
    }
    if let Some(data_mode) = data_reduction_mode {
        params.insert(
            "dataReductionMode".to_string(),
            Value::String(format!("{}", data_mode)),
        );
    }
    post_command_server_response(&client, &config, &api_call, &serde_json::to_value(params)?)?;
    Ok(())
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

fn get_command_server_response<T>(
    client: &reqwest::Client,
    config: &HitachiConfig,
    api_call: &str,
    params: Option<&HashMap<String, String>>,
) -> MetricsResult<T>
where
    T: serde::de::DeserializeOwned,
{
    let content: T = if let Some(params) = params {
        client
            .get(&format!("http://{}/{}", config.endpoint, api_call))
            .query(params)
            .basic_auth(&config.user, Some(&config.password))
            .send()?
            .error_for_status()?
            .json()?
    } else {
        client
            .get(&format!("http://{}/{}", config.endpoint, api_call))
            .basic_auth(&config.user, Some(&config.password))
            .send()?
            .error_for_status()?
            .json()?
    };
    Ok(content)
}

fn post_command_server_response<B>(
    client: &reqwest::Client,
    config: &HitachiConfig,
    api_call: &str,
    body: &B,
) -> MetricsResult<()>
where
    B: serde::ser::Serialize,
{
    client
        .post(&format!("http://{}/{}", config.endpoint, api_call))
        .basic_auth(&config.user, Some(&config.password))
        .json(body)
        .send()?
        .error_for_status()?;
    Ok(())
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
        let mut p = TsPoint::new(point_name);
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

pub fn get_parity_groups(
    client: &reqwest::Client,
    config: &HitachiConfig,
    storage_device_id: &str,
    clpr: Option<i32>,
    drive_type: Option<DriveType>,
    detail_info_type: Option<DetailInfoType>,
) -> MetricsResult<Vec<ParityGroup>> {
    let api_call = format!("v1/objects/storages/{}/parity-groups", storage_device_id);
    let mut params: HashMap<String, String> = HashMap::new();
    if let Some(clpr) = clpr {
        params.insert("clprId".into(), clpr.to_string());
    }
    if let Some(detail_info) = detail_info_type {
        params.insert("detailInfoType".into(), detail_info.to_string());
    }
    if let Some(drive_type) = drive_type {
        params.insert("driveTypeName".into(), drive_type.to_string());
    }
    let result: Vec<ParityGroup> =
        get_command_server_response(&client, &config, &api_call, Some(&params))?;
    Ok(result)
}

pub fn get_pools(
    client: &reqwest::Client,
    config: &HitachiConfig,
    storage_device_id: &str,
    pool_type: Option<PoolType>,
) -> MetricsResult<Pools> {
    let api_call = format!("v1/objects/storages/{}/pools", storage_device_id);
    let mut params: HashMap<String, String> = HashMap::new();
    if let Some(pool_type) = pool_type {
        params.insert("poolType".into(), pool_type.to_string());
    }
    let result: Pools = get_command_server_response(&client, &config, &api_call, Some(&params))?;
    Ok(result)
}

pub fn get_volumes(
    client: &reqwest::Client,
    config: &HitachiConfig,
    storage_device_id: &str,
    count: Option<u16>,
    // Default to all types
    ldev_option: Option<LdevOption>,
    pool_id: Option<i32>,
) -> MetricsResult<Volumes> {
    let api_call = format!("v1/objects/storages/{}/ldevs", storage_device_id);
    let mut params: HashMap<String, String> = HashMap::new();
    if let Some(count) = count {
        params.insert("count".into(), count.to_string());
    }
    if let Some(ldev_option) = ldev_option {
        params.insert("ldevOption".into(), ldev_option.to_string());
    }
    if let Some(pool_id) = pool_id {
        params.insert("poolId".into(), pool_id.to_string());
    }
    let result: Volumes = get_command_server_response(&client, &config, &api_call, Some(&params))?;
    Ok(result)
}
