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

#[macro_use]
extern crate nom;
#[macro_use]
extern crate point_derive;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate xml_attributes_derive;

use crate::error::MetricsResult;
use std::fmt::Debug;

use log::trace;
use reqwest::header::ACCEPT;
use serde::de::{Deserialize, DeserializeOwned};
use serde::Deserializer;

pub mod brocade;
pub mod error;
pub mod hitachi;
pub mod ir;
#[cfg(feature = "isilon-library")]
pub mod isilon;
pub mod netapp;
pub mod openstack;
pub mod scaleio;
pub mod solidfire;
pub mod telegraf;
pub mod vmax;
pub mod vnx;
pub mod xtremio;

pub trait IntoPoint {
    fn into_point(&self, name: Option<&str>, is_time_series: bool) -> Vec<ir::TsPoint>;
}

pub trait ChildPoint {
    fn sub_point(&self, p: &mut ir::TsPoint);
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum StringOrInt {
    String(String),
    Int(i64),
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum StringOrFloat {
    String(String),
    Float(f64),
}

fn deserialize_string_or_int<'de, D>(deserializer: D) -> ::std::result::Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    match StringOrInt::deserialize(deserializer)? {
        StringOrInt::String(s) => s.parse().map_err(D::Error::custom),
        StringOrInt::Int(i) => Ok(i),
    }
}

fn deserialize_string_or_float<'de, D>(deserializer: D) -> ::std::result::Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    match StringOrFloat::deserialize(deserializer)? {
        StringOrFloat::String(s) => s.parse().map_err(D::Error::custom),
        StringOrFloat::Float(i) => Ok(i),
    }
}

pub fn get<T>(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    user: &str,
    pass: Option<&str>,
) -> MetricsResult<T>
where
    T: DeserializeOwned + Debug,
{
    let res = client
        .get(endpoint)
        .basic_auth(user, pass)
        .header(ACCEPT, "application/json")
        .send()?
        .error_for_status()?
        .text()?;
    trace!("server returned: {}", res);
    let json: Result<T, serde_json::Error> = serde_json::from_str(&res);
    trace!("json result: {:?}", json);
    Ok(json?)
}
