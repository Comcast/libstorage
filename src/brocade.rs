use crate::error::{MetricsResult, StorageError};
use crate::ir::{TsPoint, TsValue};
use crate::IntoPoint;
use chrono::offset::Utc;
use chrono::DateTime;
use log::{error, trace};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT};
use serde::de::DeserializeOwned;
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
use std::fmt::Debug;

#[derive(Clone, Deserialize, Debug)]
pub struct BrocadeConfig {
    /// The brocade endpoint to use
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

pub struct Brocade {
    client: reqwest::Client,
    config: BrocadeConfig,
    token: String,
}

impl Brocade {
    /// Initialize and connect to a Brocade switch.
    pub fn new(client: &reqwest::Client, config: BrocadeConfig) -> MetricsResult<Self> {
        let token = login(client, &config)?;
        Ok(Brocade {
            client: client.clone(),
            config,
            token,
        })
    }
}

impl Drop for Brocade {
    fn drop(&mut self) {
        if let Err(e) = self.logout() {
            error!("logout failed: {}", e);
        }
    }
}

#[test]
fn parse_resource_groups() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/brocade/resource_groups.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: ResourceGroups = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}
#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug)]
pub struct ResourceGroups {
    pub resource_groups: Vec<ResourceGroup>,
}

#[derive(Deserialize, Debug)]
pub struct ResourceGroup {
    pub key: String,
    pub name: String,
    #[serde(rename = "type")]
    pub resource_type: String,
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug)]
pub struct BufferCredit {
    pub bb_credit: u64,
    #[serde(rename = "peerBBCredit")]
    pub peer_bb_credit: u64,
    pub round_trip_time: u64,
}

#[test]
fn parse_fc_fabrics() {
    use std::fs::File;
    use std::io::Read;

    sleep_the_collections();

    let mut f = File::open("tests/brocade/fcfabrics.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: FcFabrics = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug)]
pub struct FcFabrics {
    pub fc_fabrics: Vec<FcFabric>,
    pub start_index: Option<i32>,
    pub items_per_page: Option<i32>,
    pub total_results: Option<u64>,
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug, IntoPoint)]
pub struct FcFabric {
    pub key: String,
    pub seed_switch_wwn: String,
    pub name: String,
    pub secure: bool,
    pub ad_environment: bool,
    pub contact: Option<String>,
    pub location: Option<String>,
    pub description: Option<String>,
    pub principal_switch_wwn: String,
    pub fabric_name: String,
    pub virtual_fabric_id: i32,
    pub seed_switch_ip_address: String,
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug)]
pub struct FcPorts {
    pub fc_ports: Vec<FcPort>,
    pub start_index: Option<i32>,
    pub items_per_page: Option<i32>,
    pub total_results: Option<u64>,
}

#[test]
fn parse_fc_ports() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/brocade/fcports.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: FcPorts = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug, IntoPoint)]
pub struct FcPort {
    key: String,
    wwn: String,
    name: String,
    slot_number: u64,
    port_number: u64,
    user_port_number: u64,
    port_id: String,
    port_index: u64,
    area_id: u64,
    #[serde(rename = "type")]
    port_type: String,
    status: String,
    status_message: String,
    locked_port_type: String,
    speed: String,
    speeds_supported: String,
    max_port_speed: u16,
    desired_credits: u64,
    buffer_allocated: u64,
    estimated_distance: u64,
    actual_distance: u64,
    long_distance_setting: u64,
    remote_node_wwn: String,
    remote_port_wwn: String,
    licensed: bool,
    swapped: bool,
    trunked: bool,
    trunk_master: bool,
    persistently_disabled: bool,
    ficon_supported: bool,
    blocked: bool,
    prohibit_port_numbers: Option<String>,
    prohibit_port_count: u64,
    npiv_capable: bool,
    npiv_enabled: bool,
    fc_fast_write_enabled: bool,
    isl_rrdy_enabled: bool,
    rate_limit_capable: bool,
    rate_limited: bool,
    qos_capable: bool,
    qos_enabled: bool,
    fcr_fabric_id: u64,
    state: String,
    occupied: bool,
    master_port_number: i64,
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug)]
pub struct Fec {
    pub corrected_blocks: u64,
    pub uncorrected_blocks: u64,
}

#[test]
fn parse_fc_switches() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/brocade/fcswitches.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: FcSwitches = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug)]
pub struct FcSwitches {
    pub fc_switches: Vec<FcSwitch>,
    pub start_index: Option<i32>,
    pub items_per_page: Option<i32>,
    pub total_results: Option<u64>,
}

#[serde(rename_all = "camelCase")]
#[derive(Deserialize, Debug, IntoPoint)]
pub struct FcSwitch {
    pub key: String,
    #[serde(rename = "type")]
    pub fc_type: u64,
    pub name: String,
    pub wwn: String,
    pub virtual_fabric_id: i64,
    pub domain_id: u64,
    pub base_switch: bool,
    pub role: String,
    pub fcs_role: String,
    pub ad_capable: bool,
    pub operational_status: String,
    pub state: String,
    pub status_reason: Option<String>,
    pub lf_enabled: bool,
    pub default_logical_switch: bool,
    pub fms_mode: bool,
    pub dynamic_load_sharing_capable: bool,
    pub port_based_routing_present: bool,
    pub in_order_delivery_capable: bool,
    pub persistent_did_enabled: bool,
    pub auto_snmp_enabled: bool,
}

pub enum FabricTimeSeries {
    MemoryUtilPercentage,
    CpuUtilPercentage,
    Temperature,
    FanSpeed,
    ResponseTime,
    SystemUpTime,
    PortsNotInUse,
    PingPktLossPercentage,
}

impl ToString for FabricTimeSeries {
    fn to_string(&self) -> String {
        match *self {
            FabricTimeSeries::MemoryUtilPercentage => "timeseriesmemoryutilpercentage".into(),
            FabricTimeSeries::CpuUtilPercentage => "timeseriescpuutilpercentage".into(),
            FabricTimeSeries::Temperature => "timeseriestemperature".into(),
            FabricTimeSeries::FanSpeed => "timeseriesfanspeed".into(),
            FabricTimeSeries::ResponseTime => "timeseriesresponsetime".into(),
            FabricTimeSeries::SystemUpTime => "timeseriessystemuptime".into(),
            FabricTimeSeries::PortsNotInUse => "timeseriesportsnotinuse".into(),
            FabricTimeSeries::PingPktLossPercentage => "timeseriespingpktlosspercentage".into(),
        }
    }
}

pub enum FcIpTimeSeries {
    CompressionRatio,
    Latency,
    DroppedPackets,
    LinkRetransmits,
    TimeoutRetransmits,
    FastRetransmits,
    DupAckRecvd,
    WindowSizeRtt,
    TcpOooSegments,
    SlowStartStatusErrors,
    RealtimeCompressionRatio,
}

impl ToString for FcIpTimeSeries {
    fn to_string(&self) -> String {
        match *self {
            FcIpTimeSeries::CompressionRatio => "timeseriescompressionratio".into(),
            FcIpTimeSeries::Latency => "timeserieslatency".into(),
            FcIpTimeSeries::DroppedPackets => "timeseriesdroppedpackets".into(),
            FcIpTimeSeries::LinkRetransmits => "timeserieslinkretransmits".into(),
            FcIpTimeSeries::TimeoutRetransmits => "timeseriestimeoutretransmits".into(),
            FcIpTimeSeries::FastRetransmits => "timeseriesfastretransmits".into(),
            FcIpTimeSeries::DupAckRecvd => "timeseriesdupackrecvd".into(),
            FcIpTimeSeries::WindowSizeRtt => "timeserieswindowsizertt".into(),
            FcIpTimeSeries::TcpOooSegments => "timeseriestcpooosegments".into(),
            FcIpTimeSeries::SlowStartStatusErrors => "timeseriesslowstartstatuserrors".into(),
            FcIpTimeSeries::RealtimeCompressionRatio => "timeseriesrealtimecompressionratio".into(),
        }
    }
}
pub enum FcTimeSeries {
    UtilPercentage,
    Traffic,
    CrcErrors,
    LinkResets,
    SignalLosses,
    SyncLosses,
    LinkFailures,
    SequenceErrors,
    InvalidTransmissions,
    C3Discards,
    C3DiscardsTxTo,
    C3DiscardsRxTo,
    C3DiscardsUnreachable,
    C3DiscardsOther,
    EncodeErrorOut,
    SfpPower,
    SfpVoltage,
    SfpCurrent,
    SfpTemperature,
    InvalidOrderedSets,
    BbCreditZero,
    TruncatedFrames,
}

impl ToString for FcTimeSeries {
    fn to_string(&self) -> String {
        match *self {
            FcTimeSeries::UtilPercentage => "timeseriesutilpercentage".into(),
            FcTimeSeries::Traffic => "timeseriestraffic".into(),
            FcTimeSeries::CrcErrors => "timeseriescrcerrors".into(),
            FcTimeSeries::LinkResets => "timeserieslinkresets".into(),
            FcTimeSeries::SignalLosses => "timeseriessignallosses".into(),
            FcTimeSeries::SyncLosses => "timeseriessynclosses".into(),
            FcTimeSeries::LinkFailures => "timeserieslinkfailures".into(),
            FcTimeSeries::SequenceErrors => "timeseriessequenceerrors".into(),
            FcTimeSeries::InvalidTransmissions => "timeseriesinvalidtransmissions".into(),
            FcTimeSeries::C3Discards => "timeseriesc3discards".into(),
            FcTimeSeries::C3DiscardsTxTo => "timeseriesc3discardstxto".into(),
            FcTimeSeries::C3DiscardsRxTo => "timeseriesc3discardsrxto".into(),
            FcTimeSeries::C3DiscardsUnreachable => "timeseriesc3discardsunreachable".into(),
            FcTimeSeries::C3DiscardsOther => "timeseriesc3discardsother".into(),
            FcTimeSeries::EncodeErrorOut => "timeseriesencodeerrorout".into(),
            FcTimeSeries::SfpPower => "timeseriessfppower".into(),
            FcTimeSeries::SfpVoltage => "timeseriessfpvoltage".into(),
            FcTimeSeries::SfpCurrent => "timeseriessfpcurrent".into(),
            FcTimeSeries::SfpTemperature => "timeseriessfptemperature".into(),
            FcTimeSeries::InvalidOrderedSets => "timeseriesinvalidorderedsets".into(),
            FcTimeSeries::BbCreditZero => "timeseriesbbcreditzero".into(),
            FcTimeSeries::TruncatedFrames => "timeseriestruncatedframes".into(),
        }
    }
}

pub enum FrameTimeSeries {
    TxFrameCount,
    RxFrameCount,
    TxFrameRate,
    RxFrameRate,
    TxWordCount,
    RxWordCount,
    TxThroughput,
    RxThroughput,
    AvgTxFrameSize,
    AvgRxFrameSize,
    GeneratorTxFrameCount,
    GeneratorRxFrameCount,
    MirroredFramesCount,
    MirroredTxFrames,
    MirroredRxFrames,
}

impl ToString for FrameTimeSeries {
    fn to_string(&self) -> String {
        match *self {
            FrameTimeSeries::TxFrameCount => "timeseriestxframecount".into(),
            FrameTimeSeries::RxFrameCount => "timeseriesrxframecount".into(),
            FrameTimeSeries::TxFrameRate => "timeseriestxframerate".into(),
            FrameTimeSeries::RxFrameRate => "timeseriesrxframerate".into(),
            FrameTimeSeries::TxWordCount => "timeseriestxwordcount".into(),
            FrameTimeSeries::RxWordCount => "timeseriesrxwordcount".into(),
            FrameTimeSeries::TxThroughput => "timeseriestxthroughput".into(),
            FrameTimeSeries::RxThroughput => "timeseriesrxthroughput".into(),
            FrameTimeSeries::AvgTxFrameSize => "timeseriesavgtxframesize".into(),
            FrameTimeSeries::AvgRxFrameSize => "timeseriesavgrxframesize".into(),
            FrameTimeSeries::GeneratorTxFrameCount => "timeseriesgeneratortxframecount".into(),
            FrameTimeSeries::GeneratorRxFrameCount => "timeseriesgeneratorrxframecount".into(),
            FrameTimeSeries::MirroredFramesCount => "timeseriesmirroredframescount".into(),
            FrameTimeSeries::MirroredTxFrames => "timeseriesmirroredtxframes".into(),
            FrameTimeSeries::MirroredRxFrames => "timeseriesmirroredrxframes".into(),
        }
    }
}

pub struct ReadDiagnostic {
    pub switch_name: String,
    pub switch_wwn: String,
    pub number_of_ports: u64,
    pub stats_type: RdpStatsType,
    pub port_wwn: String,
    pub port_type: String,
    pub node_wwn: String,
    pub tx_power: String,
    pub rx_power: String,
    pub temperature: String,
    pub sfp_type: String,
    pub laser_type: String,
    pub voltage: String,
    pub current: String,
    pub connecter_type: String,
    pub supported_speeds: String,
    pub link_failure: u64,
    pub loss_of_sync: u64,
    pub loss_of_signal: u64,
    pub protocol_error: u64,
    pub invalid_word: u64,
    pub invalid_crc: u64,
    pub fec: Fec,
    pub buffer_credit: BufferCredit,
}

pub enum RdpStatsType {
    Historical,
    Realtime,
}

pub enum ScsiTimeSeries {
    ReadFrameCount,
    WriteFrameCount,
    ReadFrameRate,
    WriteFrameRate,
    ReadData,
    WriteData,
    ReadDataRate,
    WriteDataRate,
}

impl ToString for ScsiTimeSeries {
    fn to_string(&self) -> String {
        match *self {
            ScsiTimeSeries::ReadFrameCount => "timeseriesscsireadframecount".into(),
            ScsiTimeSeries::WriteFrameCount => "timeseriesscsiwriteframecount".into(),
            ScsiTimeSeries::ReadFrameRate => "timeseriesscsireadframerate".into(),
            ScsiTimeSeries::WriteFrameRate => "timeseriesscsiwriteframerate".into(),
            ScsiTimeSeries::ReadData => "timeseriesscsireaddata".into(),
            ScsiTimeSeries::WriteData => "timeseriesscsiwritedata".into(),
            ScsiTimeSeries::ReadDataRate => "timeseriesscsireaddatarate".into(),
            ScsiTimeSeries::WriteDataRate => "timeseriesscsiwritedatarate".into(),
        }
    }
}

pub enum TimeSeries {
    Fc(FcTimeSeries),
    FcIp(FcIpTimeSeries),
}

// Connect to the server and request a new api token
pub fn login(client: &reqwest::Client, config: &BrocadeConfig) -> MetricsResult<String> {
    let mut headers = HeaderMap::new();
    headers.insert(
        ACCEPT,
        HeaderValue::from_str("application/vnd.brocade.networkadvisor+json;version=v1")?,
    );
    headers.insert("WSUsername", HeaderValue::from_str(&config.user)?);
    headers.insert("WSPassword", HeaderValue::from_str(&config.password)?);

    let resp = client
        .post(&format!(
            "{}://{}/rest/login",
            match config.certificate {
                Some(_) => "https",
                None => "http",
            },
            config.endpoint
        ))
        .headers(headers)
        .send()?
        .error_for_status()?;

    // We need a WSToken back from the server which takes the place of the
    // password in future requests
    let token = resp.headers().get("WStoken");
    match token {
        Some(data) => Ok(data.to_str()?.to_owned()),
        None => Err(StorageError::new(format!(
            "WSToken multiple lines. {:?}. Please check server",
            token
        ))),
    }
}

// This is to delay the collections so the Brocade SAN switches do not
// get their queue over-ran with requests until they can upgrade to newer version
// which deals with that issue otherwise switch soft resets can occur
// Added the 'use' statement here to be localized so this can all be removed later
fn sleep_the_collections() {
    use std::{thread, time};

    let sleep_time = time::Duration::from_millis(5000);
    let now = time::Instant::now();

    thread::sleep(sleep_time);

    assert!(now.elapsed() >= sleep_time);
}

impl Brocade {
    // Deletes the client session
    pub fn logout(&self) -> MetricsResult<()> {
        let mut headers = HeaderMap::new();
        headers.insert("WStoken", HeaderValue::from_str(&self.token)?);

        self.client
            .post(&format!(
                "{}://{}/rest/logout",
                match self.config.certificate {
                    Some(_) => "https",
                    None => "http",
                },
                self.config.endpoint
            ))
            .headers(headers)
            .send()?
            .error_for_status()?;
        Ok(())
    }

    fn get_server_response<T>(&self, api_call: &str, ws_token: &str) -> MetricsResult<T>
    where
        T: DeserializeOwned + Debug,
    {
        let url = format!(
            "{}://{}/rest/{}",
            match self.config.certificate {
                Some(_) => "https",
                None => "http",
            },
            self.config.endpoint,
            api_call
        );
        let resp = self
            .client
            .get(&url)
            .header(
                ACCEPT,
                "application/vnd.brocade.networkadvisor+json;version=v1",
            )
            .header("WStoken", HeaderValue::from_str(ws_token)?)
            .send()?
            .error_for_status()?
            .text()?;
        trace!("server returned: {}", resp);
        let json: Result<T, serde_json::Error> = serde_json::from_str(&resp);
        trace!("json result: {:?}", json);
        Ok(json?)
    }

    pub fn get_fc_fabrics(&self, t: DateTime<Utc>) -> MetricsResult<Vec<TsPoint>> {
        sleep_the_collections();
        let result =
            self.get_server_response::<FcFabrics>("resourcegroups/All/fcfabrics", &self.token)?;
        let mut points = result
            .fc_fabrics
            .iter()
            .flat_map(|fabric| fabric.into_point(Some("brocade_fc_fabric"), true))
            .collect::<Vec<TsPoint>>();
        for point in &mut points {
            point.timestamp = Some(t)
        }
        Ok(points)
    }

    pub fn get_fc_switch_timeseries(
        &self,
        switch_key: &str,
        timeseries: TimeSeries,
    ) -> MetricsResult<()> {
        // TODO: Not sure if these performance metrics need to be enabled on the switches first
        sleep_the_collections();
        let _url = format!(
            "resourcegroups/All/fcswitches/{}/{}?duration=360",
            switch_key,
            match timeseries {
                TimeSeries::Fc(ts) => ts.to_string(),
                TimeSeries::FcIp(ts) => ts.to_string(),
            }
        );
        Ok(())
    }

    pub fn get_fc_fabric_timeseries(
        &self,
        fabric_key: &str,
        timeseries: &FabricTimeSeries,
    ) -> MetricsResult<()> {
        // TODO: Not sure if these performance metrics need to be enabled on the switches first
        sleep_the_collections();
        let _url = format!(
            "resourcegroups/All/fcfabrics/{}/{}?duration=360",
            fabric_key,
            timeseries.to_string(),
        );

        Ok(())
    }

    pub fn get_fc_fabric_ids(&self) -> MetricsResult<Vec<String>> {
        sleep_the_collections();
        let result = self
            .get_server_response::<FcFabrics>("resourcegroups/All/fcfabrics", &self.token)
            .map(|fabrics| {
                let fabrics: Vec<String> = fabrics
                    .fc_fabrics
                    .iter()
                    .map(|fabric| fabric.key.clone())
                    .collect::<Vec<String>>();
                fabrics
            })?;
        Ok(result)
    }

    pub fn get_fc_ports(&self, fabric_key: &str, t: DateTime<Utc>) -> MetricsResult<Vec<TsPoint>> {
        sleep_the_collections();
        let result = self.get_server_response::<FcPorts>(
            &format!("resourcegroups/All/fcswitches/{}/fcports", fabric_key),
            &self.token,
        )?;
        let mut points = result
            .fc_ports
            .iter()
            .flat_map(|port| port.into_point(Some("brocade_fc_port"), true))
            .collect::<Vec<TsPoint>>();
        for point in &mut points {
            point.timestamp = Some(t)
        }
        Ok(points)
    }

    pub fn get_fc_switch_ids(&self) -> MetricsResult<Vec<String>> {
        sleep_the_collections();
        let result = self
            .get_server_response::<FcSwitches>("resourcegroups/All/fcswitches", &self.token)
            .map(|switches| {
                let switches: Vec<String> = switches
                    .fc_switches
                    .iter()
                    .map(|switch| switch.key.clone())
                    .collect::<Vec<String>>();
                switches
            })?;
        Ok(result)
    }

    pub fn get_fc_switches(&self, t: DateTime<Utc>) -> MetricsResult<Vec<TsPoint>> {
        sleep_the_collections();
        let result =
            self.get_server_response::<FcSwitches>("resourcegroups/All/fcswitches", &self.token)?;
        let mut points = result
            .fc_switches
            .iter()
            .flat_map(|switch| switch.into_point(Some("brocade_fc_switch"), true))
            .collect::<Vec<TsPoint>>();
        for point in &mut points {
            point.timestamp = Some(t)
        }
        Ok(points)
    }

    pub fn get_resource_groups(&self) -> MetricsResult<ResourceGroups> {
        let result = self.get_server_response::<ResourceGroups>("resourcegroups", &self.token)?;
        Ok(result)
    }
}
