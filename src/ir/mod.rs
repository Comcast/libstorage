//! Inspiration for these structs came from compiler design.  TsPoint
//! is an intermediate representation that is used to abstract 
//! time series data points.  Point is similar but represents 
//! data points that are not time series.
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

use chrono::{DateTime, Utc};
use influx_db_client::keys::{Point, Value};

/// An intermediate representation of time series data points
#[derive(Clone, Debug)]
pub struct TsPoint {
    pub measurement: String,
    pub tags: HashMap<String, TsValue>,
    pub fields: HashMap<String, TsValue>,
    pub timestamp: Option<DateTime<Utc>>,
}

impl TsPoint {
    pub fn new(measurement: &str) -> TsPoint {
        TsPoint {
            measurement: String::from(measurement),
            tags: HashMap::new(),
            fields: HashMap::new(),
            timestamp: Some(Utc::now()),
        }
    }
    /// Add a tag and its value
    pub fn add_tag<T: ToString>(&mut self, tag: T, value: TsValue) {
        self.tags.insert(tag.to_string(), value);
    }

    /// Add a field and its value
    pub fn add_field<T: ToString>(&mut self, field: T, value: TsValue) {
        self.fields.insert(field.to_string(), value);
    }

    /// Set the timestamp for this time point
    pub fn set_time(mut self, t: DateTime<Utc>) -> Self {
        self.timestamp = Some(t);
        self
    }
}

#[derive(Clone, Debug)]
pub enum TsValue {
    Boolean(bool),
    Byte(u8),
    Integer(i32),
    Float(f64),
    Long(u64),
    Short(u16),
    SignedLong(i64),
    String(String),
    Vector(Vec<TsValue>),
}

pub fn point_to_ts(points: Vec<Point>) -> Vec<TsPoint> {
    let mut ts_points: Vec<TsPoint> = Vec::with_capacity(points.len());
    for p in points {
        let mut ts = TsPoint::new(&p.measurement);
        for (t_name, t_val) in p.tags {
            let v = match t_val {
                Value::String(s) => TsValue::String(s),
                Value::Float(f) => TsValue::Float(f),
                Value::Integer(i) => TsValue::SignedLong(i),
                Value::Boolean(b) => TsValue::Boolean(b),
            };
            ts.tags.insert(t_name, v);
        }
        for (f_name, f_val) in p.fields {
            let v = match f_val {
                Value::String(s) => TsValue::String(s),
                Value::Float(f) => TsValue::Float(f),
                Value::Integer(i) => TsValue::SignedLong(i),
                Value::Boolean(b) => TsValue::Boolean(b),
            };
            ts.fields.insert(f_name, v);
        }
        ts_points.push(ts);
    }
    ts_points
}
