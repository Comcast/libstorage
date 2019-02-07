#[macro_use]
extern crate log;
#[macro_use]
extern crate nom;
#[macro_use]
extern crate point_derive;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate xml_attributes_derive;

use serde::de::Deserialize;
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
    fn into_point(&self, name: Option<&str>) -> Vec<ir::TsPoint>;
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
        StringOrInt::String(s) => s.parse().map_err(|e| D::Error::custom(e)),
        StringOrInt::Int(i) => Ok(i),
    }
}

fn deserialize_string_or_float<'de, D>(deserializer: D) -> ::std::result::Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    match StringOrFloat::deserialize(deserializer)? {
        StringOrFloat::String(s) => s.parse().map_err(|e| D::Error::custom(e)),
        StringOrFloat::Float(i) => Ok(i),
    }
}
