use std::collections::HashMap;

use chrono::{DateTime, Utc};
use influx_db_client::keys::{Point, Value};

/// An intermediate representation of the data that
/// isn't as lossy as influx_db_client's Point enum
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