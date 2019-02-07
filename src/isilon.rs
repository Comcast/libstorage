//! Isilon has 2 modes of operation.  There's basic authentication mode and then there's
//! session mode with a cookie.  We're going to go with session mode because the docs
//! indicate that basic auth mode is a lot slower.
//! Note: Isilon has a unique api that is self describing.  This could allow someone
//! in the future to write a parser for the api to generate a complete library binding.

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

use std::rc::Rc;

use futures::Future;
use isilon::apis::configuration;
use isilon::apis::{ClusterApi, ClusterNodesApi, StatisticsApi};
use isilon::models::{ClusterStatfs, NodeDrivesNodeDrive, NodeStatus, SummaryProtocolStats};

use crate::error::StorageError;
use crate::ir::{TsPoint, TsValue};
use crate::IntoPoint;

#[derive(Clone, Deserialize, Debug)]
pub struct IsilonConfig {
    /// The isilon endpoint to use
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

impl IntoPoint for ClusterStatfs {
    fn into_point(&self, name: Option<&str>) -> Vec<TsPoint> {
        let mut point = TsPoint::new(name.unwrap_or("isilon_usage"));
        point.add_field("f_bavail", TsValue::Long(self.f_bavail));
        point.add_field("f_bfree", TsValue::Long(self.f_bfree));
        point.add_field("f_blocks", TsValue::Long(self.f_blocks));
        point.add_field("f_bsize", TsValue::Long(self.f_bsize));
        point.add_field("f_ffree", TsValue::Long(self.f_ffree));
        point.add_field("f_files", TsValue::Long(self.f_files));
        point.add_field("f_flags", TsValue::Long(self.f_flags));
        point.add_tag("f_fstypename", TsValue::String(self.f_fstypename.clone()));
        point.add_field("f_iosize", TsValue::Long(self.f_iosize));
        point.add_tag("f_mntfromname", TsValue::String(self.f_mntfromname.clone()));
        point.add_tag("f_mntonname", TsValue::String(self.f_mntonname.clone()));
        point.add_field("f_namemax", TsValue::Long(self.f_namemax));
        point.add_field("f_owner", TsValue::Long(self.f_owner));
        point.add_field("f_type", TsValue::Long(self.f_type));
        point.add_field("f_version", TsValue::Long(self.f_version));

        vec![point]
    }
}

impl IntoPoint for NodeStatus {
    fn into_point(&self, name: Option<&str>) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();
        if let Some(ref nodes) = self.nodes {
            for node in nodes {
                let mut point = TsPoint::new(name.unwrap_or("isilon_node_status"));
                if let Some(ref capacity_items) = node.capacity {
                    for item in capacity_items {
                        if let Some(ref device_type) = item._type {
                            //point.add_field("device_type", TsValue::String(device_type.clone()));
                            if let Some(bytes) = item.bytes {
                                point.add_field(
                                    format!("{}_bytes", device_type),
                                    TsValue::Long(bytes),
                                );
                            }
                            if let Some(count) = item.count {
                                point.add_field(
                                    format!("{}_count", device_type),
                                    TsValue::Integer(count),
                                );
                            }
                        }
                    }
                }
                if let Some(ref cpu) = node.cpu {
                    if let Some(ref model) = cpu.model {
                        point.add_field("model", TsValue::String(model.clone()));
                    }
                    if let Some(ref overtemp) = cpu.overtemp {
                        point.add_field("overtemp", TsValue::String(overtemp.clone()));
                    }
                    if let Some(ref _proc) = cpu._proc {
                        point.add_field("proc", TsValue::String(_proc.clone()));
                    }
                    if let Some(ref speed_limit) = cpu.speed_limit {
                        point.add_field("speed_limit", TsValue::String(speed_limit.clone()));
                    }
                }
                if let Some(id) = node.id {
                    point.add_field("id", TsValue::Integer(id));
                }
                if let Some(lnn) = node.lnn {
                    point.add_field("lnn", TsValue::Integer(lnn));
                }
                if let Some(ref nvram) = node.nvram {
                    if let Some(ref battery_count) = nvram.battery_count {
                        point.add_field("nvmram_battery_count", TsValue::Integer(*battery_count));
                    }
                    if let Some(ref charge_status_number) = nvram.charge_status_number {
                        point.add_field(
                            "nvmram_charge_status_number",
                            TsValue::Integer(*charge_status_number),
                        );
                    }
                    if let Some(ref present) = nvram.present {
                        point.add_field("nvmram_present", TsValue::Boolean(*present));
                    }
                    if let Some(ref supported) = nvram.supported {
                        point.add_field("nvmram_supported", TsValue::Boolean(*supported));
                    }
                    if let Some(ref present_size) = nvram.present_size {
                        point.add_field("nvmram_present_size", TsValue::Long(*present_size));
                    }
                    if let Some(ref supported_size) = nvram.supported_size {
                        point.add_field("nvmram_supported_size", TsValue::Long(*supported_size));
                    }
                }
                if let Some(ref release) = node.release {
                    point.add_field("release", TsValue::String(release.clone()));
                }
                if let Some(status) = node.status {
                    point.add_field("status", TsValue::Integer(status));
                }
                if let Some(uptime) = node.uptime {
                    point.add_field("uptime", TsValue::Integer(uptime));
                }
                if let Some(ref version) = node.version {
                    point.add_field("version", TsValue::String(version.clone()));
                }
                points.push(point);
            }
        }

        points
    }
}

impl IntoPoint for NodeDrivesNodeDrive {
    fn into_point(&self, name: Option<&str>) -> Vec<TsPoint> {
        let mut point = TsPoint::new(name.unwrap_or("isilon_drives"));

        if let Some(ref bay_group) = self.bay_group {
            point.add_field("bay_group", TsValue::String(bay_group.clone()));
        }
        if let Some(baynum) = self.baynum {
            point.add_field("baynum", TsValue::Integer(baynum));
        }
        if let Some(blocks) = self.blocks {
            point.add_field("blocks", TsValue::Integer(blocks));
        }
        if let Some(chassis) = self.chassis {
            point.add_field("chassis", TsValue::Integer(chassis));
        }
        if let Some(ref devname) = self.devname {
            point.add_field("devname", TsValue::String(devname.clone()));
        }

        if let Some(ref firmware) = self.firmware {
            if let Some(ref current_firmware) = firmware.current_firmware {
                if !current_firmware.is_empty() {
                    point.add_field("firmware", TsValue::String(current_firmware.clone()));
                }
            }
        }
        if let Some(handle) = self.handle {
            point.add_field("handle", TsValue::Integer(handle));
        }
        if let Some(ref interface_type) = self.interface_type {
            point.add_field("interface_type", TsValue::String(interface_type.clone()));
        }
        if let Some(lnum) = self.lnum {
            point.add_field("lnum", TsValue::Integer(lnum));
        }
        if let Some(ref locnstr) = self.locnstr {
            point.add_field("locnstr", TsValue::String(locnstr.clone()));
        }
        if let Some(logical_block_length) = self.logical_block_length {
            point.add_field(
                "logical_block_length",
                TsValue::Integer(logical_block_length),
            );
        }
        if let Some(ref media_type) = self.media_type {
            point.add_field("media_type", TsValue::String(media_type.clone()));
        }
        if let Some(ref model) = self.model {
            point.add_field("model", TsValue::String(model.clone()));
        }
        /*
        if let Some(pending_actions) = drive.pending_actions {
            point.add_field(
                "pending_actions",
                TsValue::String(",".join(pending_actions)),
            );
        }
        */
        if let Some(physical_block_length) = self.physical_block_length {
            point.add_field(
                "physical_block_length",
                TsValue::Integer(physical_block_length),
            );
        }
        if let Some(present) = self.present {
            point.add_field("present", TsValue::Boolean(present));
        }
        if let Some(ref purpose) = self.purpose {
            point.add_field("purpose", TsValue::String(purpose.clone()));
        }
        if let Some(ref purpose_description) = self.purpose_description {
            point.add_field(
                "purpose_description",
                TsValue::String(purpose_description.clone()),
            );
        }
        if let Some(ref serial) = self.serial {
            point.add_field("serial", TsValue::String(serial.clone()));
        }
        if let Some(ref ui_state) = self.ui_state {
            point.add_field("ui_state", TsValue::String(ui_state.clone()));
        }
        if let Some(ref wwn) = self.wwn {
            point.add_field("wwn", TsValue::String(wwn.clone()));
        }
        if let Some(x_loc) = self.x_loc {
            point.add_field("x_loc", TsValue::Integer(x_loc));
        }
        if let Some(y_loc) = self.y_loc {
            point.add_field("y_loc", TsValue::Integer(y_loc));
        }
        //errors: Option<Vec<::models::NodeDrivesPurposelistError>>,

        vec![point]
    }
}

impl IntoPoint for SummaryProtocolStats {
    fn into_point(&self, name: Option<&str>) -> Vec<TsPoint> {
        let mut point = TsPoint::new(name.unwrap_or("isilon_perf"));

        if let Some(ref stats) = self.protocol_stats {
            if let Some(ref cpu) = stats.cpu {
                point.add_field("cpu_idle", TsValue::Float(cpu.idle as f64));
                point.add_field("cpu_system", TsValue::Float(cpu.system as f64));
                point.add_field("cpu_user", TsValue::Float(cpu.user as f64));
            }
            if let Some(ref disk) = stats.disk {
                point.add_field("disk_iops", TsValue::Float(disk.iops as f64));
                point.add_field("disk_read", TsValue::Float(disk.read as f64));
                point.add_field("disk_write", TsValue::Float(disk.write as f64));
            }
            if let Some(ref network) = stats.network {
                if let Some(ref net_in) = network._in {
                    point.add_field(
                        "net_in_errors_per_sec",
                        TsValue::Float(net_in.errors_per_sec as f64),
                    );
                    point.add_field(
                        "net_in_mb_per_sec",
                        TsValue::Float(net_in.megabytes_per_sec as f64),
                    );
                    point.add_field(
                        "net_in_packets_per_sec",
                        TsValue::Float(net_in.packets_per_sec as f64),
                    );
                }
                if let Some(ref net_out) = network.out {
                    point.add_field(
                        "net_out_errors_per_sec",
                        TsValue::Float(net_out.errors_per_sec as f64),
                    );
                    point.add_field(
                        "net_out_mb_per_sec",
                        TsValue::Float(net_out.megabytes_per_sec as f64),
                    );
                    point.add_field(
                        "net_out_packets_per_sec",
                        TsValue::Float(net_out.packets_per_sec as f64),
                    );
                }
            }
            if let Some(ref onefs) = stats.onefs {
                // OneFS throughput in MB/s in
                point.add_field("onefs_in", TsValue::Float(onefs._in as f64));
                // OneFS throughput in MB/s out
                point.add_field("onefs_out", TsValue::Float(onefs.out as f64));
                // OneFS throughput in MB/s total
                point.add_field("onefs_total", TsValue::Float(onefs.total as f64));
            }
            if let Some(ref protocol) = stats.protocol {
                for data_item in &protocol.data {
                    // This data_item.value is a serde_json::Value which could be anything
                    // however most of our metrics storage backends don't like flip flopping
                    // types like that. So we're going to use this field if it's a number
                    // and discard otherwise unless someone has a better idea.
                    if data_item.value.is_number() {
                        let ambiguous_num = &data_item.value;
                        if ambiguous_num.is_i64() {
                            point.add_field(
                                data_item.name.clone(),
                                TsValue::Float(ambiguous_num.as_i64().unwrap() as f64),
                            );
                        } else if ambiguous_num.is_u64() {
                            point.add_field(
                                data_item.name.clone(),
                                TsValue::Float(ambiguous_num.as_u64().unwrap() as f64),
                            );
                        } else if ambiguous_num.is_f64() {
                            point.add_field(
                                data_item.name.clone(),
                                TsValue::Float(ambiguous_num.as_f64().unwrap()),
                            );
                        } else {
                            trace!(
                                "discarding isilon protocol value {:?} which isn't a number",
                                data_item.value
                            );
                        }
                    } else {
                        trace!(
                            "discarding isilon protocol value {:?} which isn't a number",
                            data_item.value
                        );
                    }
                }
                if let Some(ref name) = protocol.name {
                    point.add_tag("protocol_name", TsValue::String(name.clone()));
                }
            }
        }

        vec![point]
    }
}

#[test]
fn test_cluster_statfs() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/isilon/cluster_statfs.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: ClusterStatfs = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[test]
fn test_node_status() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/isilon/node_status.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: NodeStatus = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i.into_point(None));
}

// Try to keep these public functions as futures as long as possible to prevent blocking
pub fn get_cluster_usage<C: hyper::client::Connect>(
    rc_config: Rc<configuration::Configuration<C>>,
) -> Box<dyn Future<Item = Vec<TsPoint>, Error = StorageError>> {
    let cluster_api = isilon::apis::ClusterApiClient::new(rc_config.clone());
    Box::new(
        cluster_api
            .get_cluster_statfs()
            .map(|res| res.into_point(Some("isilon_usage")))
            .map_err(|err| StorageError::from(err)),
    )
}

pub fn get_cluster_performance<C: hyper::client::Connect>(
    rc_config: Rc<configuration::Configuration<C>>,
) -> Box<dyn Future<Item = Vec<TsPoint>, Error = StorageError>> {
    let stats_api = isilon::apis::StatisticsApiClient::new(rc_config.clone());
    Box::new(
        stats_api
            .get_summary_protocol_stats(true, None, None, 600)
            .map(|res| res.into_point(Some("isilon_perf")))
            .map_err(|err| StorageError::from(err)),
    )
}

pub fn get_node_status<C: hyper::client::Connect>(
    rc_config: Rc<configuration::Configuration<C>>,
) -> Box<dyn Future<Item = Vec<TsPoint>, Error = StorageError>> {
    let cluster_api = isilon::apis::ClusterNodesApiClient::new(rc_config.clone());
    Box::new(
        cluster_api
            .get_node_status(None)
            .map(|res| res.into_point(Some("isilon_node_status")))
            .map_err(|err| StorageError::from(err)),
    )
}

pub fn get_cluster_drives<C: hyper::client::Connect>(
    rc_config: Rc<configuration::Configuration<C>>,
) -> Box<dyn Future<Item = Vec<TsPoint>, Error = StorageError>> {
    let cluster_api = isilon::apis::ClusterApiClient::new(rc_config.clone());
    Box::new(
        cluster_api
            .get_cluster_nodes(600.0)
            .map(|res| {
                let mut points: Vec<TsPoint> = Vec::new();
                if let Some(cluster_extended_info) = res.nodes {
                    for n in cluster_extended_info {
                        if let Some(drives) = n.drives {
                            for drive in drives {
                                points.extend(drive.into_point(Some("isilon_drives")));
                            }
                        }
                    }
                }

                points
            })
            .map_err(|err| StorageError::from(err)),
    )
}

/*
#[test]
fn test_generated_apis() {
    extern crate simplelog;

    let _ =
        simplelog::TermLogger::init(simplelog::LevelFilter::Debug, simplelog::Config::default());

    let mut core = tokio_core::reactor::Core::new().unwrap();
    let handle = core.handle();

    let mut buf = Vec::new();
    println!("Reading cert: {}", "/tmp/isilon.der");
    File::open("/tmp/isilon.der")
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();

    let cert = native_tls::Certificate::from_der(&buf).unwrap();

    let mut tls_builder = native_tls::TlsConnector::builder().unwrap();
    tls_builder.add_root_certificate(cert).unwrap();

    let tls_conn = tls_builder.build().unwrap();

    let mut http_connector = hyper::client::HttpConnector::new(4, &core.handle());
    // This is enabled by default.  Isilon uses https though
    http_connector.enforce_http(false);
    let https_connector = hyper_tls::HttpsConnector::from((http_connector, tls_conn));
    let client = Client::configure()
        .connector(https_connector)
        .build(&handle);

    let c = configuration::Configuration::new(
        client,
        "{server}",
        true,
        "username",
        "password",
        Some(Path::new("/tmp/isilon.der")),
    );

    // Single threaded reference counted pointer
    let rc_config = Rc::new(c);

    let cluster_api = isilon::apis::ClusterApiClient::new(rc_config.clone());
    let stats_api = isilon::apis::StatisticsApiClient::new(rc_config.clone());
    // Setup the calls we're going to make to the server
    let cluster_usage = cluster_api
        .get_cluster_statfs()
        .map(|res| println!("cluster statfs: {:?}", res))
        .map_err(|err| {
            println!("Error: {:?}", err);
        });

    let cluster_config = cluster_api
        .get_cluster_config()
        .map(|res| println!("cluster config: {:?}", res))
        .map_err(|err| {
            println!("Error: {:?}", err);
        });

    let perf_stats = stats_api
        .get_summary_protocol_stats(true, None, None, 600)
        .map(|res| println!("cluster perf stats: {:#?}", res))
        .map_err(|err| {
            println!("Error: {:?}", err);
        });
    handle.spawn(cluster_usage);
    handle.spawn(cluster_config);

    // Run the server calls
    core.run(perf_stats).unwrap();
}
*/
