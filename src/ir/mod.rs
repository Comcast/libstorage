//! Inspiration for these structs come from compiler design patterns.  TsPoint
//! is an intermediate representation that is used to abstract
//! time series data points.
use crate::error::{MetricsResult, StorageError};
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
use chrono::{DateTime, Utc};
use influx_db_client::keys::{Point, Value};
use std::collections::HashMap;

/// An intermediate representation of time series data points
#[derive(Clone, Debug)]
pub struct TsPoint {
    pub measurement: String,
    pub tags: HashMap<String, TsValue>,
    pub fields: HashMap<String, TsValue>,
    /// This field is generally used for indexing
    pub timestamp: Option<DateTime<Utc>>,
    /// Optionally specify a field that should be used for indexing values.
    /// If not specified then the timestamp field will be used.
    pub index_field: Option<String>,
}

impl TsPoint {
    pub fn new(measurement: &str, is_time_series: bool) -> TsPoint {
        TsPoint {
            measurement: String::from(measurement),
            tags: HashMap::new(),
            fields: HashMap::new(),
            timestamp: if is_time_series {
                Some(Utc::now())
            } else {
                None
            },
            index_field: None,
        }
    }

    /// Add a field and its value
    pub fn add_field<T: ToString>(&mut self, field: T, value: TsValue) {
        self.fields.insert(field.to_string(), value);
    }

    /// Add a tag and its value
    pub fn add_tag<T: ToString>(&mut self, tag: T, value: TsValue) {
        self.tags.insert(tag.to_string(), value);
    }

    /// Set the field to be used for indexing if supported
    pub fn set_index_field(&mut self, index_field: &str) -> MetricsResult<()> {
        if self.fields.contains_key(index_field) || self.tags.contains_key(index_field) {
            self.index_field = Some(index_field.to_string());
            Ok(())
        } else {
            Err(StorageError::new(format!(
                "{} index field is not contained within tags or fields",
                index_field
            )))
        }
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
    BooleanVec(Vec<bool>),
    Byte(u8),
    ByteVec(Vec<u8>),
    Integer(i32),
    IntegerVec(Vec<i32>),
    Float(f64),
    FloatVec(Vec<f64>),
    Long(u64),
    LongVec(Vec<u64>),
    Short(u16),
    ShortVec(Vec<u16>),
    SignedShortVec(Vec<i16>),
    SignedLong(i64),
    SignedLongVec(Vec<i64>),
    String(String),
    StringVec(Vec<String>),
}

/// Convert InfluxDB Points to TsPoints
pub fn point_to_ts(points: Vec<Point>) -> Vec<TsPoint> {
    let mut ts_points: Vec<TsPoint> = Vec::with_capacity(points.len());
    for p in points {
        let mut ts = TsPoint::new(&p.measurement, true);
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
