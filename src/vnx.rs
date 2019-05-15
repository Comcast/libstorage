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
use std::io::Write;
use std::str::FromStr;

use crate::error::*;
use crate::IntoPoint;

use crate::ir::{TsPoint, TsValue};
use cookie::{Cookie, CookieJar};
use log::{debug, error, warn};
use quick_xml::events::attributes::Attributes;
use quick_xml::events::Event;
use quick_xml::Reader;
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE, COOKIE, SET_COOKIE,
};
use xml::writer::{EventWriter, XmlEvent};

pub trait FromXml {
    fn from_xml(data: &str) -> MetricsResult<Self>
    where
        Self: Sized;
}

/// Helper trait to make converting messy xml into
/// counter structs
pub trait FromXmlAttributes {
    fn from_xml_attributes(data: Attributes<'_>) -> MetricsResult<Self>
    where
        Self: Sized;
}

pub enum MoverStatsRequest {
    Cifs,
    Network,
    Nfs,
    ResourceUsage,
}

impl ToString for MoverStatsRequest {
    fn to_string(&self) -> String {
        match *self {
            MoverStatsRequest::Cifs => "CIFS-All".into(),
            MoverStatsRequest::Network => "Network-All".into(),
            MoverStatsRequest::Nfs => "NFS-All".into(),
            MoverStatsRequest::ResourceUsage => "ResourceUsage".into(),
        }
    }
}

#[derive(Clone, Deserialize, Debug)]
pub struct VnxConfig {
    /// The scaleio endpoint to use
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

pub struct Vnx {
    client: reqwest::Client,
    config: VnxConfig,
    cookie_jar: CookieJar,
}

impl Vnx {
    pub fn new(client: &reqwest::Client, config: VnxConfig) -> MetricsResult<Self> {
        let mut cookie_jar = CookieJar::new();
        login_request(&client, &config, &mut cookie_jar)?;
        Ok(Vnx {
            client: client.clone(),
            config,
            cookie_jar,
        })
    }
}

impl Drop for Vnx {
    fn drop(&mut self) {
        if let Err(e) = self.logout_request() {
            error!("Vnx logout request failed: {}", e);
        }
    }
}

fn parse_data_services_policies(s: &str) -> MetricsResult<HashMap<String, String>> {
    let mut h = HashMap::new();
    let parts = s.split(',').collect::<Vec<&str>>();

    for part in parts {
        if part.is_empty() {
            // Skip empty sections
            continue;
        }
        let key_value = part.split_terminator('=').collect::<Vec<&str>>();
        if key_value.len() != 2 {
            // Key=Value isn't formatted here properly
            return Err(StorageError::new(format!(
                "Invalid key=value in dataServicesPolicy string: {}",
                part
            )));
        }
        h.insert(key_value[0].to_string(), key_value[1].to_string());
    }

    Ok(h)
}

#[derive(Clone, Debug)]
pub enum VolumeType {
    Disk(DiskVolume),
    Meta(MetaVolume),
    Pool(PoolVolume),
    Slice(SliceVolume),
    Stripe(StripeVolume),
    Unknown,
}

#[derive(Clone, Debug)]
pub enum DiskType {
    Clstd,
    Mixed,
    Unknown,
}

impl ToString for DiskType {
    fn to_string(&self) -> String {
        match *self {
            DiskType::Clstd => "clstd".into(),
            DiskType::Mixed => "mixed".into(),
            DiskType::Unknown => "unknown".into(),
        }
    }
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct IpCounter {
    pub sent: u64,
    pub received: u64,
    pub notForw: u64,
    pub deliv: u64,
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct TcpCounter {
    pub sent: u64,
    pub received: u64,
    pub connReq: u64,
    pub connLing: u64,
    pub retransm: u64,
    pub resets: u64,
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct UdpCounter {
    pub deliv: u64,
    pub sent: u64,
    pub badPorts: u64,
    pub incomplHdrs: u64,
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct DeviceCounter {
    device: String,
    _in: u64,
    out: u64,
}

#[test]
fn test_cifs_servers_parser() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/vnx/cifs_server_query.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = CifsServers::from_xml(&data).unwrap();
    println!("result: {:#?}", res);
    let points = res.into_point(Some("vnx_cifs_servers"), false);
    println!("points: {:#?}", points);
}

#[derive(Clone, Debug)]
pub struct CifsServers {
    pub cifs_servers: Vec<CifsServer>,
}

impl IntoPoint for CifsServers {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let all_cifs: Vec<TsPoint> = self
            .cifs_servers
            .iter()
            .flat_map(|f| f.into_point(name, is_time_series))
            .collect();

        all_cifs
    }
}

impl FromXml for CifsServers {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        let mut reader = Reader::from_str(data);
        reader.trim_text(true);
        let mut buf = Vec::new();

        let mut cifs_servers: Vec<CifsServer> = Vec::new();

        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    if b"CifsServer" == e.name() {
                        cifs_servers.push(CifsServer::from_xml_attributes(e.attributes())?);
                    }
                }
                Ok(Event::Empty(_e)) => {}
                Ok(Event::End(_e)) => {}
                Err(e) => {
                    return Err(StorageError::new(format!(
                        "invalid xml data  from server at position: {}: {:?}",
                        reader.buffer_position(),
                        e
                    )));
                }
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }
        Ok(CifsServers { cifs_servers })
    }
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct CifsServer {
    pub mover: String,
    pub name: String,
    //pub r#type: String,
    pub localUsers: bool,
    pub moverIdIsVdm: bool,
    pub interfaces: String,
}

#[derive(Clone, Debug)]
pub struct FileSystemCapacities {
    pub capacity: Vec<FileSystemCapacity>,
}

#[derive(Clone, Debug, IntoPoint)]
pub struct FileSystemCapacity {
    filesystem_id: u64,
    /// The maximum number of files and/or directories possible on this file
    /// system.
    files_total: u64,
    /// The current number of files and/or directories on this file system.
    files_used: u64,
    /// Returns the name (alias) of the file system.
    /// name is unique among all file systems
    name: String,
    /// The total data capacity of the file system.
    space_total: u64,
    /// The amount of space currently used by the user data.
    space_used: u64,
    /// List of IDs of storages from which this file system was allocated.
    storages: Vec<u64>,
    /// The list of IDs of storage pools from which this file system was
    /// allocated. If the file system was not allocated from storage pools,
    /// this list is empty.
    storage_pools: Vec<u64>,
    /// The ID of the volume object this file system is based on
    volume: u64,
    /// The size of the underlying volume. This datum characterizes the
    /// resources used by this file system in the context of the entire
    /// Celerra system
    volume_size: u64,
}

impl IntoPoint for FileSystemCapacities {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let capacity_points: Vec<TsPoint> = self
            .capacity
            .iter()
            .flat_map(|f| f.into_point(name, is_time_series))
            .collect();

        capacity_points
    }
}

impl FromXml for FileSystemCapacities {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        let mut reader = Reader::from_str(data);
        reader.trim_text(true);
        let mut buf = Vec::new();

        let mut capacity: Vec<FileSystemCapacity> = Vec::new();
        let mut filesystem_id: u64 = 0;

        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    if b"FileSystem" == e.name() {
                        let mut name = String::new();
                        let mut volume = 0;
                        let mut storages: Vec<u64> = Vec::new();
                        let mut storage_pools: Vec<u64> = Vec::new();

                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"name" => {
                                    name = val.to_string();
                                }
                                b"volume" => {
                                    volume = u64::from_str(&val)?;
                                }
                                b"storagePools" => {
                                    // TODO: Verify if this is comma or space separated
                                    // The API docs don't specify
                                    storage_pools = val
                                        .split_whitespace()
                                        .collect::<Vec<&str>>()
                                        .iter()
                                        .map(|v| u64::from_str(&v))
                                        .filter(|num| num.is_ok())
                                        .map(|num| num.unwrap())
                                        .collect::<Vec<u64>>();
                                }
                                b"storages" => {
                                    // TODO: Verify if this is comma or space separated
                                    // The API docs don't specify
                                    storages = val
                                        .split_whitespace()
                                        .collect::<Vec<&str>>()
                                        .iter()
                                        .map(|v| u64::from_str(&v))
                                        .filter(|num| num.is_ok())
                                        .map(|num| num.unwrap())
                                        .collect::<Vec<u64>>();
                                }
                                b"fileSystem" => {
                                    filesystem_id = u64::from_str(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for FileSystem",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                        capacity.push(FileSystemCapacity {
                            filesystem_id,
                            name,
                            files_total: 0,
                            files_used: 0,
                            space_total: 0,
                            space_used: 0,
                            storages,
                            storage_pools,
                            volume,
                            volume_size: 0,
                        });
                    } else if b"FileSystemCapacityInfo" == e.name() {
                        let mut volume_size = 0;
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"fileSystem" => {
                                    filesystem_id = u64::from_str(&val)?;
                                }
                                b"volumeSize" => {
                                    volume_size = u64::from_str(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for FileSystemCapacityInfo",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                        // Try to locate an existing struct to update
                        match capacity
                            .iter()
                            .position(|c| c.filesystem_id == filesystem_id)
                        {
                            Some(pos) => {
                                // Update
                                capacity[pos].volume_size = volume_size;
                            }
                            None => {
                                // Nothing to do here?
                                warn!("Found FileSystemCapacityInfo element without FileSystem");
                            }
                        }
                    } else {
                        debug!("Unknown empty tag: {}", String::from_utf8_lossy(e.name()));
                    }
                }
                Ok(Event::Empty(ref e)) => {
                    if b"ResourceUsage" == e.name() {
                        let mut space_total = 0;
                        let mut space_used = 0;
                        let mut files_total = 0;
                        let mut files_used = 0;
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"filesUsed" => {
                                    files_used = u64::from_str(&val)?;
                                }
                                b"filesTotal" => {
                                    files_total = u64::from_str(&val)?;
                                }
                                b"spaceUsed" => {
                                    space_used = u64::from_str(&val)?;
                                }
                                b"spaceTotal" => {
                                    space_total = u64::from_str(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for FileSystemCapacityInfo",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                        match capacity
                            .iter()
                            .position(|c| c.filesystem_id == filesystem_id)
                        {
                            Some(pos) => {
                                // Update the capacity information
                                capacity[pos].files_total = files_total;
                                capacity[pos].files_used = files_used;
                                capacity[pos].space_total = space_total;
                                capacity[pos].space_used = space_used;
                            }
                            None => {
                                // Nothing to do here?
                                warn!("Found ResourceUsage element without FileSystemCapacityInfo");
                            }
                        }
                    }
                }
                Ok(Event::End(_e)) => {}
                Err(e) => {
                    return Err(StorageError::new(format!(
                        "invalid xml data from server at position: {}: {:?}",
                        reader.buffer_position(),
                        e
                    )));
                }
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }

        Ok(FileSystemCapacities { capacity })
    }
}

#[test]
fn test_filesystem_capacity_parser() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/vnx/filesystem_capacity_query.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = FileSystemCapacities::from_xml(&data).unwrap();
    let points = res.into_point(Some("vnx_filesystem_capacity"), true);
    println!("result: {:#?}", points);
}

#[test]
fn test_mount_parser() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/vnx/mounts_query.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = Mounts::from_xml(&data).unwrap();
    let points = res.into_point(Some("vnx_mounts"), false);
    println!("result: {:#?}", points);
}

#[derive(Clone, Debug)]
pub struct Mounts {
    pub mounts: Vec<Mount>,
}

impl IntoPoint for Mounts {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        for m in &self.mounts {
            points.extend(m.into_point(Some(name.unwrap_or("vnx_mounts")), is_time_series));
        }

        points
    }
}

#[derive(Clone, Debug, Default, IntoPoint)]
pub struct Mount {
    pub disabled: bool,
    pub file_system: u64,
    pub path: String,
    pub mover: u64,
    pub mover_is_vdm: bool,
}

impl FromXml for Mounts {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        let mut reader = Reader::from_str(data);
        reader.trim_text(true);
        let mut buf = Vec::new();

        let mut mounts: Vec<Mount> = Vec::new();
        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    if b"Mount" == e.name() {
                        let mut disabled = false;
                        let mut file_system: u64 = 0;
                        let mut path = String::new();
                        let mut mover: u64 = 0;
                        let mut mover_is_vdm = false;

                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"disabled" => {
                                    disabled = bool::from_str(&val)?;
                                }
                                b"fileSystem" => {
                                    file_system = u64::from_str(&val)?;
                                }
                                b"mover" => {
                                    mover = u64::from_str(&val)?;
                                }
                                b"moverIdIsVdm" => {
                                    mover_is_vdm = bool::from_str(&val)?;
                                }
                                b"path" => {
                                    path = val.to_string();
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for MoverNetStats",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                        mounts.push(Mount {
                            disabled,
                            file_system,
                            path,
                            mover,
                            mover_is_vdm,
                        });
                    }
                }
                Ok(Event::End(_e)) => {}
                Err(e) => {
                    return Err(StorageError::new(format!(
                        "invalid xml data  from server at position: {}: {:?}",
                        reader.buffer_position(),
                        e
                    )));
                }
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }

        Ok(Mounts { mounts })
    }
}

#[test]
fn test_network_all_parser() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/vnx/network_stats_query.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = NetworkAllSample::from_xml(&data).unwrap();
    let points = res.into_point(None, true);
    println!("result: {:#?}", points);
}

/// All CIFS related counters
#[derive(Clone, Debug)]
pub struct NetworkAllSample {
    pub mover: String,
    pub time: u64,
    pub stamp: u64,
    pub ip: IpCounter,
    pub tcp: TcpCounter,
    pub udp: UdpCounter,
    pub devices: Vec<DeviceCounter>,
}

impl IntoPoint for NetworkAllSample {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut p = TsPoint::new(name.unwrap_or("networking_usage"), true);
        p.add_tag("mover", TsValue::String(self.mover.clone()));
        // Turn these counters into point arrays, get the first one and merge
        // the fields with this point
        p.fields
            .extend(self.ip.into_point(None, is_time_series)[0].fields.clone());
        p.fields
            .extend(self.tcp.into_point(None, is_time_series)[0].fields.clone());
        p.fields
            .extend(self.udp.into_point(None, is_time_series)[0].fields.clone());
        for device in &self.devices {
            p.add_tag("device", TsValue::String(device.device.clone()));
            p.add_field(
                format!("{}_in", device.device.clone()),
                TsValue::Long(device._in),
            );
            p.add_field(format!("{}_out", device.device), TsValue::Long(device.out));
        }

        vec![p]
    }
}

impl FromXml for NetworkAllSample {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        let mut reader = Reader::from_str(data);
        reader.trim_text(true);
        let mut buf = Vec::new();

        let mut mover = String::new();
        let mut time = 0;
        let mut stamp = 0;
        let mut ip = IpCounter::default();
        let mut tcp = TcpCounter::default();
        let mut udp = UdpCounter::default();
        let mut devices = Vec::new();

        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    if b"MoverNetStats" == e.name() {
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"mover" => {
                                    mover = val.to_string();
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for MoverNetStats",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                    } else if b"Sample" == e.name() {
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"time" => {
                                    time = u64::from_str(&val)?;
                                }
                                b"stamp" => {
                                    stamp = u64::from_str(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for Sample",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                    }
                }
                Ok(Event::Empty(e)) => {
                    if b"Ip" == e.name() {
                        ip = IpCounter::from_xml_attributes(e.attributes())?;
                    } else if b"Tcp" == e.name() {
                        tcp = TcpCounter::from_xml_attributes(e.attributes())?;
                    } else if b"Udp" == e.name() {
                        udp = UdpCounter::from_xml_attributes(e.attributes())?;
                    } else if b"DeviceTraffic" == e.name() {
                        let d = DeviceCounter::from_xml_attributes(e.attributes())?;
                        devices.push(d);
                    } else {
                        debug!("Unknown empty tag: {}", String::from_utf8_lossy(e.name()));
                    }
                }
                Ok(Event::End(_e)) => {}
                Err(e) => {
                    return Err(StorageError::new(format!(
                        "invalid xml data  from server at position: {}: {:?}",
                        reader.buffer_position(),
                        e
                    )));
                }
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }
        Ok(NetworkAllSample {
            mover,
            time,
            stamp,
            ip,
            tcp,
            udp,
            devices,
        })
    }
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct Trans2Counter {
    pub trans2Open: u64,
    pub trans2FindFirst: u64,
    pub trans2FindNext: u64,
    pub trans2QFsInfo: u64,
    pub trans2QPathInfo: u64,
    pub trans2SetPathInfo: u64,
    pub trans2QFileInfo: u64,
    pub trans2SetFileInfo: u64,
    pub trans2Mkdir: u64,
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct NtCounter {
    pub ntCreate: u64,
    pub ntSetSd: u64,
    pub ntNotifyChange: u64,
    pub ntRename: u64,
    pub ntQuerySd: u64,
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct StateCounter {
    pub openConnections: u64,
    pub openFiles: u64,
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct TotalsCounter {
    pub all: u64,
    pub smb: u64,
    pub trans2: u64,
    pub nt: u64,
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct SmbCounter {
    pub mkdir: u64,
    pub rmdir: u64,
    pub open: u64,
    pub create: u64,
    pub close: u64,
    pub flush: u64,
    pub unlink: u64,
    pub rename: u64,
    pub getAttr: u64,
    pub setAttr: u64,
    pub read: u64,
    pub write: u64,
    pub lock: u64,
    pub unlock: u64,
    pub createTmp: u64,
    pub mkNew: u64,
    pub chkPath: u64,
    pub exit: u64,
    pub lseek: u64,
    pub lockRead: u64,
    pub writeUnlock: u64,
    pub readBlockRaw: u64,
    pub writeBlockRaw: u64,
    pub setAttrExp: u64,
    pub getAttrExp: u64,
    pub lockingX: u64,
    pub trans: u64,
    pub transSec: u64,
    pub copy: u64,
    pub _move: u64,
    pub echo: u64,
    pub writeClose: u64,
    pub openX: u64,
    pub readX: u64,
    pub writeX: u64,
    pub closeTreeDisco: u64,
    pub trans2Prim: u64,
    pub trans2Secd: u64,
    pub findClose2: u64,
    pub findNotifyClose: u64,
    pub treeConnect: u64,
    pub treeDisco: u64,
    pub negProt: u64,
    pub sessSetupX: u64,
    pub userLogOffX: u64,
    pub treeConnectX: u64,
    pub diskAttr: u64,
    pub search: u64,
    pub findFirst: u64,
    pub findUnique: u64,
    pub findClose: u64,
    pub transNt: u64,
    pub transNtSecd: u64,
    pub createNtX: u64,
    pub cancelNt: u64,
    pub sendMessage: u64,
    pub beginMessage: u64,
    pub endMessage: u64,
    pub messageText: u64,
}

#[test]
fn test_cifs_all_parser() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/vnx/cifs_stats_query.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = CifsAllSample::from_xml(&data).unwrap();
    println!("result: {:#?}", res);
}

/// All CIFS related counters
#[derive(Clone, Debug)]
pub struct CifsAllSample {
    pub mover: String,
    pub time: u64,
    pub stamp: u64,
    pub smb_calls: SmbCounter,
    pub smb_time: SmbCounter,
    pub trans2_calls: Trans2Counter,
    pub trans2_time: Trans2Counter,
    pub nt_calls: NtCounter,
    pub nt_time: NtCounter,
    pub state: StateCounter,
    pub totals: TotalsCounter,
}

impl IntoPoint for CifsAllSample {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut p = TsPoint::new(name.unwrap_or("cifs_usage"), true);
        p.add_tag("mover", TsValue::String(self.mover.clone()));
        // Turn these counters into point arrays, get the first one and merge
        // the fields with this point
        p.fields.extend(
            self.smb_calls.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields.extend(
            self.smb_time.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields.extend(
            self.trans2_calls.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields.extend(
            self.trans2_time.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields.extend(
            self.nt_calls.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields.extend(
            self.nt_time.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields.extend(
            self.state.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields.extend(
            self.totals.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );

        vec![p]
    }
}

impl FromXml for CifsAllSample {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        let mut reader = Reader::from_str(data);
        reader.trim_text(true);
        let mut buf = Vec::new();

        let mut mover = String::new();
        let mut time = 0;
        let mut stamp = 0;
        let mut smb_calls = SmbCounter::default();
        let mut smb_time = SmbCounter::default();
        let mut trans2_calls = Trans2Counter::default();
        let mut trans2_time = Trans2Counter::default();
        let mut nt_calls = NtCounter::default();
        let mut nt_time = NtCounter::default();
        let mut state = StateCounter::default();
        let mut totals = TotalsCounter::default();

        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    if b"MoverCifsStats" == e.name() {
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"mover" => {
                                    mover = val.to_string();
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for MoverCifsStats",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                    } else if b"Sample" == e.name() {
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"time" => {
                                    time = u64::from_str(&val)?;
                                }
                                b"stamp" => {
                                    stamp = u64::from_str(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for Sample",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                    }
                }
                Ok(Event::Empty(e)) => {
                    if b"SMBCalls" == e.name() {
                        smb_calls = SmbCounter::from_xml_attributes(e.attributes())?;
                    } else if b"SMBTime" == e.name() {
                        smb_time = SmbCounter::from_xml_attributes(e.attributes())?;
                    } else if b"Trans2Calls" == e.name() {
                        trans2_calls = Trans2Counter::from_xml_attributes(e.attributes())?;
                    } else if b"Trans2Time" == e.name() {
                        trans2_time = Trans2Counter::from_xml_attributes(e.attributes())?;
                    } else if b"NTCalls" == e.name() {
                        nt_calls = NtCounter::from_xml_attributes(e.attributes())?;
                    } else if b"NTTime" == e.name() {
                        nt_time = NtCounter::from_xml_attributes(e.attributes())?;
                    } else if b"State" == e.name() {
                        state = StateCounter::from_xml_attributes(e.attributes())?;
                    } else if b"Totals" == e.name() {
                        totals = TotalsCounter::from_xml_attributes(e.attributes())?;
                    } else if b"MoverCifsStats" == e.name() {
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"mover" => {
                                    mover = val.to_string();
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for MoverNfsStats",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                    } else {
                        //warn!("Unknown empty tag: {}", String::from_utf8_lossy(e.name()));
                    }
                }
                Ok(Event::End(_e)) => {}
                Err(e) => {
                    return Err(StorageError::new(format!(
                        "invalid xml data  from server at position: {}: {:?}",
                        reader.buffer_position(),
                        e
                    )));
                }
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }
        Ok(CifsAllSample {
            mover,
            time,
            stamp,
            smb_calls,
            smb_time,
            trans2_calls,
            trans2_time,
            nt_calls,
            nt_time,
            state,
            totals,
        })
    }
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct CacheCounter {
    pub hits: u64,
    pub misses: u64,
    pub adds: u64,
    pub nonExistent: u64,
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct RpcCounter {
    pub calls: u64,
    pub badData: u64,
    pub dupl: u64,
    pub resends: u64,
    pub badAuth: u64,
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct NfsV2Counter {
    pub null: u64,
    pub getattr: u64,
    pub setattr: u64,
    pub root: u64,
    pub lookup: u64,
    pub readlink: u64,
    pub read: u64,
    pub wrcache: u64,
    pub write: u64,
    pub create: u64,
    pub remove: u64,
    pub link: u64,
    pub symlink: u64,
    pub mkdir: u64,
    pub rmdir: u64,
    pub readdir: u64,
    pub fsstat: u64,
}

#[derive(Clone, Debug, Default, FromXmlAttributes, IntoPoint)]
pub struct NfsV3Counter {
    pub v3null: u64,
    pub v3getattr: u64,
    pub v3setattr: u64,
    pub v3lookup: u64,
    pub v3access: u64,
    pub v3readlink: u64,
    pub v3read: u64,
    pub v3write: u64,
    pub v3create: u64,
    pub v3mkdir: u64,
    pub v3symlink: u64,
    pub v3mknod: u64,
    pub v3remove: u64,
    pub v3rmdir: u64,
    pub v3rename: u64,
    pub v3link: u64,
    pub v3readdir: u64,
    pub v3readdirplus: u64,
    pub v3fsstat: u64,
    pub v3fsinfo: u64,
    pub v3pathconf: u64,
    pub v3commit: u64,
}

#[test]
fn test_nfs_all_parser() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/vnx/nfs_mover_request.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = NfsAllSample::from_xml(&data).unwrap();
    println!("result: {:#?}", res);
}

/// All NFS related counters
#[derive(Clone, Debug)]
pub struct NfsAllSample {
    pub mover: String,
    pub time: u64,
    pub stamp: u64,
    pub proc_v2_calls: NfsV2Counter,
    pub proc_v2_time: NfsV2Counter,
    pub proc_v2_failures: NfsV2Counter,
    pub proc_v3_failures: NfsV3Counter,
    pub proc_v3_calls: NfsV3Counter,
    pub proc_v3_time: NfsV3Counter,
    pub cache: CacheCounter,
    pub rpc: RpcCounter,
}

impl IntoPoint for NfsAllSample {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut p = TsPoint::new(name.unwrap_or("nfs_usage"), true);
        p.add_tag("mover", TsValue::String(self.mover.clone()));
        // Turn these counters into point arrays, get the first one and merge
        // the fields with this point
        p.fields.extend(
            self.proc_v2_calls.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields.extend(
            self.proc_v2_failures.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields.extend(
            self.proc_v2_time.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields.extend(
            self.proc_v3_calls.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields.extend(
            self.proc_v3_failures.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields.extend(
            self.proc_v3_time.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields.extend(
            self.cache.into_point(None, is_time_series)[0]
                .fields
                .clone(),
        );
        p.fields
            .extend(self.rpc.into_point(None, is_time_series)[0].fields.clone());

        vec![p]
    }
}

impl FromXml for NfsAllSample {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        let mut reader = Reader::from_str(data);
        reader.trim_text(true);
        let mut buf = Vec::new();

        let mut mover = String::new();
        let mut time = 0;
        let mut stamp = 0;
        let mut proc_v2_calls = NfsV2Counter::default();
        let mut proc_v2_time = NfsV2Counter::default();
        let mut proc_v2_failures = NfsV2Counter::default();
        let mut proc_v3_failures = NfsV3Counter::default();
        let mut proc_v3_calls = NfsV3Counter::default();
        let mut proc_v3_time = NfsV3Counter::default();
        let mut cache = CacheCounter::default();
        let mut rpc = RpcCounter::default();

        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    if b"MoverNfsStats" == e.name() {
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"mover" => {
                                    mover = val.to_string();
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for MoverNfsStats",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                    } else if b"Sample" == e.name() {
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"time" => {
                                    time = u64::from_str(&val)?;
                                }
                                b"stamp" => {
                                    stamp = u64::from_str(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for Sample",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                    }
                }
                Ok(Event::Empty(e)) => {
                    if b"ProcV2Calls" == e.name() {
                        proc_v2_calls = NfsV2Counter::from_xml_attributes(e.attributes())?;
                    } else if b"ProcV2Time" == e.name() {
                        proc_v2_time = NfsV2Counter::from_xml_attributes(e.attributes())?;
                    } else if b"ProcV2Failures" == e.name() {
                        proc_v2_failures = NfsV2Counter::from_xml_attributes(e.attributes())?;
                    } else if b"ProcV3Calls" == e.name() {
                        proc_v3_calls = NfsV3Counter::from_xml_attributes(e.attributes())?;
                    } else if b"ProcV3Time" == e.name() {
                        proc_v3_time = NfsV3Counter::from_xml_attributes(e.attributes())?;
                    } else if b"ProcV3Failures" == e.name() {
                        proc_v3_failures = NfsV3Counter::from_xml_attributes(e.attributes())?;
                    } else if b"Cache" == e.name() {
                        cache = CacheCounter::from_xml_attributes(e.attributes())?;
                    } else if b"Rpc" == e.name() {
                        rpc = RpcCounter::from_xml_attributes(e.attributes())?;
                    } else if b"MoverNfsStats" == e.name() {
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"mover" => {
                                    mover = val.to_string();
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for MoverNfsStats",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                    } else {
                        //warn!("Unknown empty tag: {}", String::from_utf8_lossy(e.name()));
                    }
                }
                Ok(Event::End(_e)) => {}
                Err(e) => {
                    return Err(StorageError::new(format!(
                        "invalid xml data  from server at position: {}: {:?}",
                        reader.buffer_position(),
                        e
                    )));
                }
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }
        Ok(NfsAllSample {
            mover,
            time,
            stamp,
            proc_v2_calls,
            proc_v2_time,
            proc_v2_failures,
            proc_v3_calls,
            proc_v3_time,
            proc_v3_failures,
            cache,
            rpc,
        })
    }
}

#[test]
fn test_disk_info_parser() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/vnx/clariion_disk_query.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = DiskInfo::from_xml(&data).unwrap();
    println!("result: {:#?}", res);
}

#[derive(Clone, Debug)]
pub struct DiskInfo {
    pub disks: Vec<Disk>,
}

#[derive(Clone, Debug, FromXmlAttributes, IntoPoint)]
pub struct Disk {
    pub bus: u64,
    pub enclosure_number: u64,
    pub disk_number: u64,
    pub state: String,
    pub vendor_id: String,
    pub product_id: String,
    pub revision: String,
    pub serial_number: String,
    pub capacity: u64,
    pub used_capacity: u64,
    pub remapped_blocks: u64,
    pub storage: String,
    pub name: String,
}

impl FromXml for DiskInfo {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        let mut reader = Reader::from_str(data);
        reader.trim_text(true);
        let mut buf = Vec::new();

        let mut disks: Vec<Disk> = Vec::new();

        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(_e)) => {}
                Ok(Event::Empty(ref e)) => {
                    if b"ClariionDiskConfig" == e.name() {
                        disks.push(Disk::from_xml_attributes(e.attributes())?);
                    }
                }
                Ok(Event::End(_e)) => {}
                Err(e) => {
                    return Err(StorageError::new(format!(
                        "invalid xml data  from server at position: {}: {:?}",
                        reader.buffer_position(),
                        e
                    )));
                }
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }
        Ok(DiskInfo { disks })
    }
}

impl IntoPoint for DiskInfo {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        for disk in &self.disks {
            points.extend(disk.into_point(name, is_time_series));
        }

        points
    }
}

#[test]
fn test_resources_all_parser() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/vnx/mover_stats_query.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = ResourceUsageSample::from_xml(&data).unwrap();
    println!("result: {:#?}", res);
}

#[derive(Clone, Debug)]
pub struct ResourceUsageSample {
    pub mover: String,
    pub cpu: f64,
    pub mem: f64,
    pub time: u64,
    pub stamp: u64,
}

impl IntoPoint for ResourceUsageSample {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut p = TsPoint::new(name.unwrap_or("resource_usage"), is_time_series);
        p.add_tag("mover", TsValue::String(self.mover.clone()));
        p.add_field("cpu", TsValue::Float(self.cpu));
        p.add_field("memory", TsValue::Float(self.mem));

        vec![p]
    }
}

impl FromXml for ResourceUsageSample {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        let mut reader = Reader::from_str(data);
        reader.trim_text(true);
        let mut buf = Vec::new();

        let mut mover = String::new();
        let mut cpu = 0.0;
        let mut mem = 0.0;
        let mut time = 0;
        let mut stamp = 0;
        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    if b"MoverResourceUsage" == e.name() {
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"mover" => {
                                    mover = val.to_string();
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for MoverResourceUsage",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                    }
                }
                Ok(Event::Empty(e)) => {
                    if b"Sample" == e.name() {
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"cpu" => {
                                    cpu = f64::from_str(&val)?;
                                }
                                b"mem" => {
                                    mem = f64::from_str(&val)?;
                                }
                                b"time" => {
                                    time = u64::from_str(&val)?;
                                }
                                b"stamp" => {
                                    stamp = u64::from_str(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for Sample",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                    } else {
                        debug!("Unknown empty tag: {}", String::from_utf8_lossy(e.name()));
                    }
                }
                Ok(Event::End(_e)) => {}
                Err(e) => {
                    return Err(StorageError::new(format!(
                        "invalid xml data  from server at position: {}: {:?}",
                        reader.buffer_position(),
                        e
                    )));
                }
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }
        Ok(ResourceUsageSample {
            mover,
            cpu,
            mem,
            time,
            stamp,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Filesystem {
    pub filesystem_id: String,
    pub space_total: u64,
    pub space_used: u64,
    pub files_total: u64,
    pub files_used: u64,
}

#[test]
fn test_filesystem_query_parser() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/vnx/filesystem_usage_query.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = FilesystemUsage::from_xml(&data).unwrap();
    println!("result: {:#?}", res);
}

#[derive(Clone, Debug)]
pub struct FilesystemUsage {
    pub filesystems: Vec<Filesystem>,
}

impl IntoPoint for FilesystemUsage {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();

        for f in &self.filesystems {
            let mut p = TsPoint::new(name.unwrap_or("filesystem_usage"), is_time_series);
            p.add_tag("filesystem_id", TsValue::String(f.filesystem_id.clone()));
            p.add_field("space_total", TsValue::Long(f.space_total));
            p.add_field("space_used", TsValue::Long(f.space_used));
            p.add_field("files_used", TsValue::Long(f.files_used));
            p.add_field("files_total", TsValue::Long(f.files_total));
            points.push(p);
        }

        points
    }
}

impl FromXml for FilesystemUsage {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        // Depending on the Volume type given that'll dictate the child element we expect
        let mut reader = Reader::from_str(data);
        reader.trim_text(true);
        let mut buf = Vec::new();
        let mut filesystems = Vec::new();

        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref _e)) => {}
                Ok(Event::Empty(e)) => {
                    if b"Item" == e.name() {
                        let mut filesystem = String::new();
                        let mut space_total = 0;
                        let mut space_used = 0;
                        let mut files_total = 0;
                        let mut files_used = 0;

                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"fileSystem" => {
                                    filesystem = val.to_string();
                                }
                                b"spaceTotal" => {
                                    space_total = u64::from_str(&val)?;
                                }
                                b"spaceUsed" => {
                                    space_used = u64::from_str(&val)?;
                                }
                                b"filesTotal" => {
                                    files_total = u64::from_str(&val)?;
                                }
                                b"filesUsed" => {
                                    files_used = u64::from_str(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for Item",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                        filesystems.push(Filesystem {
                            filesystem_id: filesystem,
                            space_total,
                            space_used,
                            files_total,
                            files_used,
                        })
                    } else {
                        debug!("Unknown empty tag: {}", String::from_utf8_lossy(e.name()));
                    }
                }
                Ok(Event::End(_e)) => {}
                Err(e) => {
                    return Err(StorageError::new(format!(
                        "invalid xml data  from server at position: {}: {:?}",
                        reader.buffer_position(),
                        e
                    )));
                }
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }
        Ok(FilesystemUsage { filesystems })
    }
}

#[derive(Clone, Debug)]
pub struct DiskVolume {
    pub storage_system_id: u64,
    pub lun: String,
    pub disk_type: DiskType,
    pub movers: Vec<String>,
    pub data_service_policies: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct FreeSpace {
    size: u64,
    offset: u64,
}

#[derive(Clone, Debug)]
pub struct MetaVolume {
    member_volumes: Vec<String>,
    client_file_systems: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct PoolVolume {
    pub client_file_systems: Vec<String>,
    pub member_volumes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct SliceVolume {
    sliced_volume: String,
    offset: u64,
}

#[derive(Clone, Debug)]
pub struct StripeVolume {
    striped_volumes: Vec<String>,
    stripe_size: u16,
}

#[derive(Debug)]
pub struct Volume {
    pub name: String,
    pub vol_type: VolumeType,
    pub size: u64,
    pub client_volumes: Vec<String>,
    pub virtual_provisioning: bool,
    pub volume_id: u64,
    pub free_space: Vec<FreeSpace>,
}

impl IntoPoint for Volume {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut p = TsPoint::new(name.unwrap_or("volume"), is_time_series);
        match self.vol_type {
            VolumeType::Disk(ref v) => {
                p.add_tag(
                    "system_id",
                    TsValue::String(v.storage_system_id.to_string()),
                );
                p.add_tag("lun", TsValue::String(v.lun.clone()));
                p.add_tag("disk_type", TsValue::String(v.disk_type.to_string()));
                // TODO: Should we add the movers?
                for (key, value) in &v.data_service_policies {
                    p.add_tag(key, TsValue::String(value.clone()));
                }
            }
            VolumeType::Meta(ref _v) => {
                //TODO: Should we tag all the meta data volumes?
            }
            VolumeType::Pool(ref _v) => {
                // TODO: Should we tag all the client filesystems?
            }
            VolumeType::Slice(ref v) => {
                p.add_tag("sliced_volume", TsValue::String(v.sliced_volume.clone()));
                p.add_field("offset", TsValue::Long(v.offset));
            }
            VolumeType::Stripe(ref v) => {
                // TODO: Should we add tags for all the striped_volume ids?
                p.add_field("stripe_size", TsValue::Short(v.stripe_size));
            }
            VolumeType::Unknown => {}
        };
        p.add_field("size", TsValue::Long(self.size));
        p.add_field(
            "virtual_provisioning",
            TsValue::Boolean(self.virtual_provisioning),
        );
        p.add_field("volume_id", TsValue::Long(self.volume_id));
        for f in self.free_space.iter().enumerate() {
            p.add_field(format!("free_space_size_{}", f.0), TsValue::Long(f.1.size));
            p.add_field(
                format!("free_space_offset_{}", f.0),
                TsValue::Long(f.1.offset),
            );
        }
        vec![p]
    }
}

#[test]
fn test_volume_query_parser() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/vnx/volume_query.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = Volumes::from_xml(&data).unwrap();
    println!("result: {:#?}", res);
}

#[derive(Debug)]
pub struct Volumes {
    pub volumes: Vec<Volume>,
}

impl FromXml for Volumes {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        // Depending on the Volume type given that'll dictate the child element we expect
        let mut reader = Reader::from_str(data);
        reader.trim_text(true);
        let mut buf = Vec::new();

        let mut volumes = Vec::new();

        let mut name = String::new();
        let mut vol_type = VolumeType::Unknown;
        let mut size: u64 = 0;
        let mut client_volumes = Vec::new();
        let mut virtual_provisioning = false;
        let mut volume_id: u64 = 0;
        let mut free_space = Vec::new();

        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    if b"Volume" == e.name() {
                        // Clear the placeholders
                        name = String::new();
                        vol_type = VolumeType::Unknown;
                        size = 0;
                        client_volumes = Vec::new();
                        virtual_provisioning = false;
                        volume_id = 0;
                        free_space = Vec::new();
                        // Grab attributes
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"name" => {
                                    name = val.to_string();
                                }
                                b"type" => {
                                    //VolumeType::from_str(val).map_err(|e| e.to_string())?;
                                }
                                b"size" => {
                                    size = u64::from_str(&val)?;
                                }
                                b"clientVolumes" => {
                                    client_volumes = val
                                        .split_whitespace()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<String>>();
                                }
                                b"virtualProvisioning" => {
                                    virtual_provisioning = bool::from_str(&val)?;
                                }
                                b"volume" => {
                                    volume_id = u64::from_str(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for Volume",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                    } else {
                        warn!("Unknown start tag: {}", String::from_utf8_lossy(e.name()));
                    }
                }
                Ok(Event::Empty(e)) => {
                    if b"DiskVolumeData" == e.name() {
                        let mut storage_system_id = 0;
                        let mut lun = String::new();
                        let mut _disk_type = DiskType::Unknown;
                        let mut movers = Vec::new();
                        let mut data_service_policies = HashMap::new();
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"storageSystem" => {
                                    storage_system_id = u64::from_str(&val)?;
                                }
                                b"lun" => {
                                    lun = val.to_string();
                                }
                                b"diskType" => {
                                    //size = u64::from_str(&val).map_err(|e| e.to_string())?;
                                }
                                b"movers" => {
                                    movers = val
                                        .split_whitespace()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<String>>();
                                }
                                b"dataServicePolicies" => {
                                    data_service_policies = parse_data_services_policies(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for DiskVolumeData",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                        vol_type = VolumeType::Disk(DiskVolume {
                            storage_system_id,
                            lun,
                            disk_type: DiskType::Clstd,
                            movers,
                            data_service_policies,
                        });
                    } else if b"MetaVolumeData" == e.name() {
                        let mut member_volumes = Vec::new();
                        let mut client_file_systems = Vec::new();
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"memberVolumes" => {
                                    member_volumes = val
                                        .split_whitespace()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<String>>();
                                }
                                b"clientFileSystems" => {
                                    client_file_systems = val
                                        .split_whitespace()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<String>>();
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for MetaVolumeData",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                        vol_type = VolumeType::Meta(MetaVolume {
                            member_volumes,
                            client_file_systems,
                        });
                    } else if b"SliceVolumeData" == e.name() {
                        let mut sliced_volume = String::new();
                        let mut offset = 0;
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"slicedVolume" => {
                                    sliced_volume = val.to_string();
                                }
                                b"offset" => {
                                    offset = u64::from_str(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for SliceVolumeData",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                        vol_type = VolumeType::Slice(SliceVolume {
                            sliced_volume,
                            offset,
                        });
                    } else if b"StripeVolumeData" == e.name() {
                        let mut striped_volumes = Vec::new();
                        let mut stripe_size = 0;
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"slicedVolume" => {
                                    striped_volumes = val
                                        .split_whitespace()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<String>>();
                                }
                                b"stripeSize" => {
                                    stripe_size = u16::from_str(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for StripeVolumeData",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                        vol_type = VolumeType::Stripe(StripeVolume {
                            striped_volumes,
                            stripe_size,
                        });
                    } else if b"FreeSpace" == e.name() {
                        let mut size = 0;
                        let mut offset = 0;
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"size" => {
                                    size = u64::from_str(&val)?;
                                }
                                b"offset" => {
                                    offset = u64::from_str(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for FreeSpace",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                        free_space.push(FreeSpace { size, offset });
                    } else if b"PoolVolumeData" == e.name() {
                        let mut member_volumes = Vec::new();
                        let mut client_file_systems = Vec::new();
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"clientFileSystems" => {
                                    client_file_systems = val
                                        .split_whitespace()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<String>>();
                                }
                                b"memberVolumes" => {
                                    member_volumes = val
                                        .split_whitespace()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<String>>();
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for PoolVolumeData",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            }
                        }
                        vol_type = VolumeType::Pool(PoolVolume {
                            client_file_systems,
                            member_volumes,
                        });
                    } else {
                        debug!("Unknown empty tag: {}", String::from_utf8_lossy(e.name()));
                    }
                }
                Ok(Event::End(e)) => {
                    if b"Volume" == e.name() {
                        volumes.push(Volume {
                            name: name.clone(),
                            vol_type: vol_type.clone(),
                            size,
                            client_volumes: client_volumes.clone(),
                            virtual_provisioning,
                            volume_id,
                            free_space: free_space.clone(),
                        });
                    }
                }
                Err(e) => {
                    return Err(StorageError::new(format!(
                        "invalid xml data  from server at position: {}: {:?}",
                        reader.buffer_position(),
                        e
                    )));
                }
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }
        Ok(Volumes { volumes })
    }
}

// Updated the new test file to pull the additional pool information from the VNX which includes pool name
#[test]
fn test_storage_pool_query_parser() {
    use std::fs::File;
    use std::io::Read;

    let data = {
        let mut s = String::new();
        let mut f = File::open("tests/vnx/storage_pool2_query.xml").unwrap();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let res = StoragePools::from_xml(&data).unwrap();
    println!("result: {:#?}", res);
    let _ = res.into_point(None, true);
}

#[derive(Debug)]
pub struct StoragePools {
    pub storage_pools: Vec<StoragePool>,
}

#[derive(Debug)]
pub struct StoragePool {
    pub movers: Vec<String>,
    pub member_volumes: Vec<String>,
    pub name: String,
    pub description: String,
    pub size: u64,
    pub used_size: u64,
    pub auto_size: u64,
    pub stripe_count: u16,
    pub stripe_size: u16,
    pub pool: String,
    pub template_pool: String,
    pub data_service_policies: HashMap<String, String>,
    pub virtual_provisioning: bool,
    pub is_homogeneous: bool,
}

impl IntoPoint for StoragePool {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut p = TsPoint::new(name.unwrap_or("pool"), is_time_series);
        p.add_tag("pool", TsValue::String(self.pool.clone()));
        /* get serial number from description. Assuming this is last token */
        let mut serial_number = self
            .description
            .split_whitespace()
            .last()
            .unwrap_or("Unknown")
            .to_string();
        /* Assuming starts with APM always */
        if !serial_number.starts_with("APM") {
            serial_number = "Unknown".to_string();
        }
        p.add_field("name", TsValue::String(self.name.clone()));
        p.add_field("serial_number", TsValue::String(serial_number));
        p.add_field("size", TsValue::Long(self.size));
        p.add_field("used_size", TsValue::Long(self.used_size));
        p.add_field("auto_size", TsValue::Long(self.auto_size));
        p.add_field("stripe_count", TsValue::Short(self.stripe_count));
        p.add_field("stripe_size", TsValue::Short(self.stripe_size));

        for (key, value) in &self.data_service_policies {
            p.add_tag(key, TsValue::String(value.clone()));
        }
        vec![p]
    }
}

// Collecting the new information to include the pool name and pool ID and for VNXs where there are multiple pools
impl IntoPoint for StoragePools {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        for pool in &self.storage_pools {
            points.extend(pool.into_point(name, is_time_series));
        }
        points
    }
}

impl Default for StoragePool {
    fn default() -> Self {
        StoragePool {
            movers: Vec::new(),
            member_volumes: Vec::new(),
            name: String::new(),
            description: String::new(),
            size: 0,
            used_size: 0,
            auto_size: 0,
            stripe_count: 0,
            stripe_size: 0,
            pool: String::new(),
            template_pool: String::new(),
            data_service_policies: HashMap::new(),
            is_homogeneous: false,
            virtual_provisioning: false,
        }
    }
}

impl FromXml for StoragePools {
    fn from_xml(data: &str) -> MetricsResult<Self> {
        let mut reader = Reader::from_str(data);
        reader.trim_text(true);
        let mut buf = Vec::new();
        let mut storage_pools = Vec::new();
        let mut storage_pool = StoragePool::default();

        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    // Clear any residual variable information from the loop in the case of multiple pools on a VNX
                    storage_pool = StoragePool::default();
                    if b"StoragePool" == e.name() {
                        // Grab attributes
                        for a in e.attributes() {
                            let item = a?;
                            let val = String::from_utf8_lossy(&item.value);
                            match item.key {
                                b"movers" => {
                                    storage_pool.movers = val
                                        .split_whitespace()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<String>>();
                                }
                                b"memberVolumes" => {
                                    storage_pool.member_volumes = val
                                        .split_whitespace()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<String>>();
                                }
                                b"name" => {
                                    storage_pool.name = val.to_string();
                                }
                                b"description" => {
                                    storage_pool.description = val.to_string();
                                }
                                b"size" => {
                                    storage_pool.size = u64::from_str(&val)?;
                                }
                                b"usedSize" => {
                                    storage_pool.used_size = u64::from_str(&val)?;
                                }
                                b"autoSize" => {
                                    storage_pool.auto_size = u64::from_str(&val)?;
                                }
                                b"stripeCount" => {
                                    storage_pool.stripe_count = u16::from_str(&val)?;
                                }
                                b"stripeSize" => {
                                    storage_pool.stripe_size = u16::from_str(&val)?;
                                }
                                b"pool" => {
                                    storage_pool.pool = val.to_string();
                                }
                                b"templatePool" => {
                                    storage_pool.template_pool = val.to_string();
                                }
                                b"dataServicePolicies" => {
                                    storage_pool.data_service_policies =
                                        parse_data_services_policies(&val)?;
                                }
                                b"isHomogeneous" => {
                                    storage_pool.is_homogeneous = bool::from_str(&val)?;
                                }
                                b"virtualProvisioning" => {
                                    storage_pool.virtual_provisioning = bool::from_str(&val)?;
                                }
                                _ => {
                                    debug!(
                                        "unknown xml attribute: {} for StoragePool",
                                        String::from_utf8_lossy(item.key)
                                    );
                                }
                            };
                        }
                    }
                }
                Ok(Event::Empty(_e)) => {}
                Err(e) => {
                    return Err(StorageError::new(format!(
                        "invalid xml data  from server at position: {}: {:?}",
                        reader.buffer_position(),
                        e
                    )));
                }
                Ok(Event::End(e)) => {
                    if b"StoragePool" == e.name() {
                        storage_pools.push(storage_pool);
                        // Clear any residual variable information from the loop in the case of multiple pools on a VNX
                        storage_pool = StoragePool::default();
                    }
                }
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }
        Ok(StoragePools { storage_pools })
    }
}

#[test]
fn test_xml_reader() {
    let data = r#"<ResponsePacket xmlns=\"http://www.emc.com/schemas/celerra/xml_api\">\n
        <Response>\n
            <QueryStatus maxSeverity="ok"/>\n
            <StoragePool movers="1 2 3" memberVolumes="268 279 286 292 295 298 664 748 757 1273 1280 1287 1334 1717 1732 1886 1887 1888 2015 4010 4165 4324 4327 4390 4396 4409 4416 4423 4474 5427 5440 5688 5885 5888 5915 5918 5921 5924 5943 5946 5994 5997 6004 6114 6126 6134 6153 6160 6163 6208 6215 6250 6253 6435 6498 6522 6840 6949 6956 8532 8604 8765 8772 8843 8854 8894 8897 9124 9157 9181 9310 9313 9316 9433 9534 9655 9658 9661 9690 10027 10056 10059" storageSystems="1" name="Pool 0" description="Mapped Pool Pool 0 on APM00121300890" mayContainSlicesDefault="true" diskType="Mixed" size="214957916" usedSize="93945439" autoSize="214957916" virtualProvisioning="false" isHomogeneous="true" dataServicePolicies="Thin=No,Compressed=No,Mirrored=No,Tiering policy=Auto-Tier/Highest Available Tier" templatePool="44" stripeCount="5" stripeSize="256" pool="44">\n            
                <SystemStoragePoolData dynamic="true" greedy="true" potentialAdditionalSize="0" isBackendPool="true"/>\n
            </StoragePool>\n
        </Response>\n
    </ResponsePacket>"#;
    let result = StoragePools::from_xml(&data);

    println!("Result: {:?}", result);
}
pub fn login_request(
    client: &reqwest::Client,
    config: &VnxConfig,
    cookie_jar: &mut CookieJar,
) -> MetricsResult<()> {
    let mut params = HashMap::new();
    params.insert("user", config.user.clone());
    params.insert("password", config.password.clone());
    params.insert("Login", "Login".into());

    let s = client
        .post(&format!("https://{}/Login", config.endpoint))
        .form(&params)
        .send()?
        .error_for_status()?;

    // From here we should get back a cookie
    match s.headers().get(SET_COOKIE) {
        Some(cookie) => {
            debug!("cookie: {:?}", cookie);
            let parsed = Cookie::parse(cookie.to_str()?.to_owned())?;
            cookie_jar.add(parsed);
            Ok(())
        }
        None => Err(StorageError::new(
            "Server responded 200 OK but cookie not set.  Cannot proceed further".into(),
        )),
    }
}

impl Vnx {
    pub fn logout_request(&self) -> MetricsResult<()> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, HeaderValue::from_str("0")?);
        headers.insert(CONTENT_TYPE, HeaderValue::from_str("application/xml")?);

        match self.cookie_jar.get("Ticket") {
            Some(t) => {
                let cookie = format!(
                    "{}={}; path={}",
                    t.name(),
                    t.value(),
                    t.path().unwrap_or("/")
                );
                headers.insert(COOKIE, HeaderValue::from_str(&cookie)?);
            }
            None => {
                return Err(StorageError::new(
                    "Unable to find Ticket cookie from vnx server".into(),
                ));
            }
        };

        match self.cookie_jar.get("JSESSIONID") {
            Some(t) => {
                headers.insert(
                    HeaderName::from_str("CelerraConnector-Sess")?,
                    HeaderValue::from_str(t.value())?,
                );
            }
            None => {
                return Err(StorageError::new(
                    "Unable to find JSESSIONID cookie from vnx server".into(),
                ));
            }
        };
        headers.insert("CelerraConnector-Ctl", HeaderValue::from_str("DISCONNECT")?);

        self.client
            .post(&format!(
                "https://{}/servlets/CelerraManagementServices",
                self.config.endpoint
            ))
            .headers(headers)
            .body("")
            .send()?
            .error_for_status()?;
        Ok(())
    }

    fn api_request<T>(&mut self, req: Vec<u8>) -> MetricsResult<T>
    where
        T: FromXml,
    {
        let mut headers = HeaderMap::new();

        // Set the ticket ID
        let ticket_cookie = match self.cookie_jar.get("Ticket") {
            Some(t) => {
                format!(
                    "{}={}; path={}",
                    t.name(),
                    t.value(),
                    t.path().unwrap_or("/")
                )
                //headers.set_raw("Cookie", ticket_cookie);
            }
            None => {
                return Err(StorageError::new(
                    "Unable to find Ticket cookie from vnx server".into(),
                ));
            }
        };

        // Set the Session ID if available
        match self.cookie_jar.get("JSESSIONID") {
            Some(t) => {
                let session_cookie = format!(
                    "{}; {}={}; path={}; $Secure;",
                    ticket_cookie,
                    t.name(),
                    t.value(),
                    t.path().unwrap_or("/"),
                );
                debug!("session cookie: {}", session_cookie);
                headers.insert(
                    HeaderName::from_str("Cookie")?,
                    HeaderValue::from_str(&session_cookie)?,
                );
                headers.insert(
                    HeaderName::from_str("CelerraConnector-Sess")?,
                    HeaderValue::from_str(t.value())?,
                );
                debug!("headers: {:?}", headers);
            }
            None => {
                headers.insert(COOKIE, HeaderValue::from_str(&ticket_cookie)?);
            }
        };

        let mut s = self
            .client
            .post(&format!(
                "https://{}/servlets/CelerraManagementServices",
                self.config.endpoint
            ))
            .body(req)
            .headers(headers)
            .send()?
            .error_for_status()?;

        // From here we should get back a JSESSIONID cookie
        if let Some(cookie) = s.headers().get(SET_COOKIE) {
            debug!("cookie: {:?}", cookie);
            let parsed = Cookie::parse(cookie.to_str()?.to_owned())?;
            self.cookie_jar.add(parsed);
        };

        let data = s.text()?;
        debug!("api_request response: {}", data);
        let res = T::from_xml(&data)?;

        Ok(res)
    }

    pub fn mover_network_stats_request(&mut self, mover_id: &str) -> MetricsResult<Vec<TsPoint>> {
        self.mover_stats_request::<NetworkAllSample>(mover_id, &MoverStatsRequest::Network)
    }

    pub fn mover_cifs_stats_request(&mut self, mover_id: &str) -> MetricsResult<Vec<TsPoint>> {
        self.mover_stats_request::<CifsAllSample>(mover_id, &MoverStatsRequest::Cifs)
    }

    pub fn mover_resource_stats_request(&mut self, mover_id: &str) -> MetricsResult<Vec<TsPoint>> {
        self.mover_stats_request::<ResourceUsageSample>(mover_id, &MoverStatsRequest::ResourceUsage)
    }

    pub fn mover_nfs_stats_request(&mut self, mover_id: &str) -> MetricsResult<Vec<TsPoint>> {
        self.mover_stats_request::<NfsAllSample>(mover_id, &MoverStatsRequest::Nfs)
    }

    // Helper function
    fn mover_stats_request<T>(
        &mut self,
        mover_id: &str,
        req_type: &MoverStatsRequest,
    ) -> MetricsResult<Vec<TsPoint>>
    where
        T: FromXml + IntoPoint,
    {
        let mut output: Vec<u8> = Vec::new();
        let req_type_str = req_type.to_string();
        {
            let mut writer = EventWriter::new(&mut output);
            begin_query_stats_request(&mut writer)?;
            let e = XmlEvent::start_element("MoverStats")
                .attr("mover", mover_id)
                .attr("statsSet", &req_type_str);
            writer.write(e)?;
            end_element(&mut writer, "MoverStats")?;
            end_query_stats_request(&mut writer)?;
        }
        let res: T = self.api_request(output)?;
        Ok(res.into_point(None, true))
    }

    /*
    pub fn volume_stats_request(
        client: &Client,
        config: &VnxConfig,
        cookie_jar: &CookieJar,
    ) -> MetricsResult<Vec<Point>> {
        let p: Vec<Point> = Vec::new();
        let mut output: Vec<u8> = Vec::new();
        {
            let mut writer = EventWriter::new(&mut output);
            begin_query_stats_request(&mut writer)?;
            start_element(&mut writer, "VolumeStats", None, None)?;
            end_element(&mut writer, "VolumeStats")?;
            end_query_stats_request(&mut writer)?;
        }
        let res: Volumes = api_request(&client, &config, output, &cookie_jar)?;
        Ok(p)
    }

    pub fn volume_query_request(
        client: &Client,
        config: &VnxConfig,
        cookie_jar: &CookieJar,
    ) -> MetricsResult<Vec<Point>> {
        let p: Vec<Point> = Vec::new();
        let mut output: Vec<u8> = Vec::new();
        {
            let mut writer = EventWriter::new(&mut output);
            begin_query_request(&mut writer)?;
            start_element(&mut writer, "VolumeQueryParams", None, None)?;
            end_element(&mut writer, "VolumeQueryParams")?;
            end_query_request(&mut writer)?;
        }
        let res: Volumes = api_request(&client, &config, output, &cookie_jar)?;
        Ok(p)
    }
    */

    pub fn storage_pool_query_request(&mut self) -> MetricsResult<StoragePools> {
        let mut output: Vec<u8> = Vec::new();
        {
            let mut writer = EventWriter::new(&mut output);
            begin_query_request(&mut writer)?;
            start_element(&mut writer, "StoragePoolQueryParams", None, None)?;
            end_element(&mut writer, "StoragePoolQueryParams")?;
            end_query_request(&mut writer)?;
        }
        let res: StoragePools = self.api_request(output)?;
        Ok(res)
    }

    pub fn disk_info_request(&mut self, mover_id: &str) -> MetricsResult<Vec<TsPoint>> {
        let mut output: Vec<u8> = Vec::new();
        {
            let mut writer = EventWriter::new(&mut output);
            let e = XmlEvent::start_element("RequestPacket")
                .default_ns("http://www.emc.com/schemas/celerra/xml_api")
                .attr("apiVersion", "V1_1");
            writer.write(e)?;
            start_element(&mut writer, "RequestEx", None, None)?;
            start_element(&mut writer, "Query", None, None)?;
            let e = XmlEvent::start_element("ClariionDiskQueryParams").attr("clariion", mover_id);
            writer.write(e)?;
            end_element(&mut writer, "ClariionDiskQueryParams")?;
            end_element(&mut writer, "Query")?;
            end_element(&mut writer, "RequestEx")?;
            end_element(&mut writer, "RequestPacket")?;
        }
        debug!("{}", String::from_utf8_lossy(&output));
        let res: DiskInfo = self.api_request(output)?;
        Ok(res.into_point(Some("vnx_disk_info"), true))
    }

    pub fn cifs_server_request(&mut self) -> MetricsResult<Vec<TsPoint>> {
        let mut output: Vec<u8> = Vec::new();

        {
            let mut writer = EventWriter::new(&mut output);
            begin_query_request(&mut writer)?;
            start_element(&mut writer, "CifsServerQueryParams", None, None)?;
            end_element(&mut writer, "CifsServerQueryParams")?;
            end_query_request(&mut writer)?;
        }

        let res: CifsServers = self.api_request(output)?;
        Ok(res.into_point(Some("vnx_cifs_servers"), false))
    }

    pub fn filesystem_capacity_request(&mut self) -> MetricsResult<Vec<TsPoint>> {
        let mut output: Vec<u8> = Vec::new();
        {
            let mut writer = EventWriter::new(&mut output);
            begin_query_request(&mut writer)?;
            start_element(&mut writer, "FileSystemQueryParams", None, None)?;
            let e = XmlEvent::start_element("AspectSelection")
                .attr("fileSystems", "true")
                .attr("fileSystemCapacityInfos", "true");
            writer.write(e)?;
            end_element(&mut writer, "AspectSelection")?;
            end_element(&mut writer, "FileSystemQueryParams")?;
            end_query_request(&mut writer)?;
        }
        let res: FileSystemCapacities = self.api_request(output)?;
        Ok(res.into_point(Some("vnx_filesystem_capacity"), true))
    }

    pub fn filesystem_usage_request(&mut self) -> MetricsResult<Vec<TsPoint>> {
        let mut output: Vec<u8> = Vec::new();
        {
            let mut writer = EventWriter::new(&mut output);
            begin_query_stats_request(&mut writer)?;
            start_element(&mut writer, "FileSystemUsage", None, None)?;
            end_element(&mut writer, "FileSystemUsage")?;
            end_query_stats_request(&mut writer)?;
        }
        let res: FilesystemUsage = self.api_request(output)?;
        Ok(res.into_point(None, true))
    }

    /// A VNX mount is identified by the Data Mover ID and the mount path
    /// (This is a directory where the file system is mounted. In VNX terminology
    /// it is called the mount point.) in the root file system of the mover or VDM.
    /// A mount export is identified by the Data Mover or VDM on which the file
    /// system is mounted and the mount path.
    pub fn mount_listing_request(&mut self) -> MetricsResult<Vec<TsPoint>> {
        let mut output: Vec<u8> = Vec::new();
        // Create the XML request object to send to the VNX
        {
            let mut writer = EventWriter::new(&mut output);
            begin_query_request(&mut writer)?;
            start_element(&mut writer, "MountQueryParams", None, None)?;
            end_element(&mut writer, "MountQueryParams")?;
            end_query_request(&mut writer)?;
        }
        // Request the mount info from the VNX
        let res: Mounts = self.api_request(output)?;
        Ok(res.into_point(Some("vnx_mounts"), false))
    }
}

fn begin_query_request<W: Write>(w: &mut EventWriter<W>) -> MetricsResult<()> {
    start_request(w)?;
    start_element(w, "Request", None, None)?;
    start_element(w, "Query", None, None)?;
    Ok(())
}

fn end_query_request<W: Write>(w: &mut EventWriter<W>) -> MetricsResult<()> {
    end_element(w, "Query")?;
    end_element(w, "Request")?;
    end_element(w, "RequestPacket")?;
    Ok(())
}

fn begin_query_stats_request<W: Write>(w: &mut EventWriter<W>) -> MetricsResult<()> {
    start_request(w)?;
    start_element(w, "Request", None, None)?;
    start_element(w, "QueryStats", None, None)?;
    Ok(())
}

fn end_query_stats_request<W: Write>(w: &mut EventWriter<W>) -> MetricsResult<()> {
    end_element(w, "QueryStats")?;
    end_element(w, "Request")?;
    end_element(w, "RequestPacket")?;
    Ok(())
}

fn start_request<W: Write>(w: &mut EventWriter<W>) -> MetricsResult<()> {
    let e = XmlEvent::start_element("RequestPacket")
        .default_ns("http://www.emc.com/schemas/celerra/xml_api");
    w.write(e)?;
    Ok(())
}

fn start_element<W: Write>(
    w: &mut EventWriter<W>,
    element_name: &str,
    name: Option<&str>,
    element_type: Option<&str>,
) -> MetricsResult<()> {
    if name.is_some() && element_type.is_some() {
        let e = XmlEvent::start_element(element_name)
            .attr("name", name.unwrap())
            .attr("type", element_type.unwrap());
        w.write(e)?;
    } else if name.is_some() && element_type.is_none() {
        let e = XmlEvent::start_element(element_name).attr("name", name.unwrap());
        w.write(e)?;
    } else if name.is_none() && element_type.is_some() {
        let e = XmlEvent::start_element(element_name).attr("type", element_type.unwrap());
        w.write(e)?;
    } else {
        let e = XmlEvent::start_element(element_name);
        w.write(e)?;
    }
    Ok(())
}

fn end_element<W: Write>(w: &mut EventWriter<W>, name: &str) -> MetricsResult<()> {
    let e = XmlEvent::end_element().name(name);
    w.write(e)?;
    Ok(())
}
