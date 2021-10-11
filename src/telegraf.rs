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
use crate::error::MetricsResult;
use crate::ir::{TsPoint, TsValue};

use std::collections::HashMap;
use std::str::FromStr;

/*
Text format:

# HELP conntrack_ip_conntrack_count Telegraf collected metric
# TYPE conntrack_ip_conntrack_count untyped
conntrack_ip_conntrack_count{host="{server}",node_type="physical_host"} 3
# HELP conntrack_ip_conntrack_max Telegraf collected metric
# TYPE conntrack_ip_conntrack_max untyped
conntrack_ip_conntrack_max{host="{server}",node_type="physical_host"} 262144
# HELP cpu_usage_guest Telegraf collected metric
# TYPE cpu_usage_guest gauge
cpu_usage_guest{cpu="cpu-total",host="{server}",node_type="physical_host"} 0
cpu_usage_guest{cpu="cpu0",host="{server}",node_type="physical_host"} 0
cpu_usage_guest{cpu="cpu1",host="{server}",node_type="physical_host"} 0
cpu_usage_guest{cpu="cpu2",host="{server}",node_type="physical_host"} 0
cpu_usage_guest{cpu="cpu3",host="{server}",node_type="physical_host"} 0
cpu_usage_guest{cpu="cpu4",host="{server}",node_type="physical_host"} 0
cpu_usage_guest{cpu="cpu5",host="{server}",node_type="physical_host"} 0
cpu_usage_guest{cpu="cpu6",host="{server}",node_type="physical_host"} 0
cpu_usage_guest{cpu="cpu7",host="{server}",node_type="physical_host"} 0

# HELP diskio_weighted_io_time Telegraf collected metric
# TYPE diskio_weighted_io_time counter
diskio_weighted_io_time{host="5d9df11cb518",name="dm-0"} 3.1767088e+07
diskio_weighted_io_time{host="5d9df11cb518",name="dm-1"} 21104
diskio_weighted_io_time{host="5d9df11cb518",name="loop0"} 16
diskio_weighted_io_time{host="5d9df11cb518",name="loop1"} 28
*/

#[derive(Deserialize, Debug)]
pub struct TelegrafConfig {
    /// The telegraf endpoint to use
    pub endpoints: Vec<String>,
    pub port: u64,
    pub user: String,
    pub password: String,
    /// The region this cluster is located in
    pub region: String,
}

#[test]
fn test_telegraf_parsing() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/telegraf/telegraf.txt").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i = parse_telegraf(&buff, None).unwrap();
    println!("result: {:#?}", i);
}

//diskio_weighted_io_time{host="5d9df11cb518",name="dm-0"} 3.1767088e+07
fn parse_telegraf(output: &str, point_name: Option<&str>) -> MetricsResult<TsPoint> {
    // Data model
    // <metric name>{<label name>=<label value>, ...}
    let mut point = TsPoint::new(point_name.unwrap_or("telegraf"), true);
    for line in output.lines() {
        if line.starts_with('#') {
            continue;
        }
        if line.is_empty() {
            // Skip empty lines
            continue;
        }
        // Case 1: diskio_weighted_io_time{host="5d9df11cb518",name="dm-0"} 3.1767088e+07
        // Case 2: go_gc_duration_seconds_sum 0.000506371
        let start_index = line.chars().position(|c| c == '{');
        let end_index = line.chars().position(|c| c == '}');
        let (counter_name, counter_value, tags_text) = match (start_index, end_index) {
            (Some(s_index), Some(e_index)) => {
                let counter_name = &line[0..s_index];
                let counter_value = line[e_index + 1..].trim_start();
                let tags_text = &line[s_index + 1..e_index];
                (counter_name, counter_value, Some(tags_text))
            }
            _ => {
                // This counter is missing the {}'s
                let parts: Vec<&str> = line.split_whitespace().collect();
                (parts[0], parts[1], None)
            }
        };
        /*if start_index.is_some() && end_index.is_some() {
            let counter_name = &line[0..start_index.unwrap()];
            let counter_value = line[end_index.unwrap() + 1..].trim_start();
            let tags_text = &line[start_index.unwrap() + 1..end_index.unwrap()];
            (counter_name, counter_value, Some(tags_text))
        } else {
            // This counter is missing the {}'s
            let parts: Vec<&str> = line.split_whitespace().collect();
            (parts[0], parts[1], None)
        };*/

        if counter_name.starts_with("go")
            || counter_name.starts_with("disk_inodes")
            || counter_name.starts_with("dm")
            || counter_name.starts_with("disk_used_percent")
            || counter_name.starts_with("net_icmp")
            || counter_name.starts_with("net_udp")
            || counter_name.starts_with("maas_disk")
            || counter_name.starts_with("cpu_usage_guest")
            || counter_name.starts_with("cpu_usage_steal")
        {
            // Skip all the golang counters and other counters we don't care about yet
            continue;
        }

        let mut hmap = HashMap::new();

        if let Some(tags_text) = tags_text {
            let tags = tags_text.split(',').collect::<Vec<&str>>();
            for tag in tags {
                let pair = tag.split_terminator('=').collect::<Vec<&str>>();
                hmap.insert(pair[0].to_string(), pair[1].to_string());
            }
        }

        if hmap.contains_key("name") {
            let n = &hmap["name"].trim_matches('"');
            if n.starts_with("loop") || n.starts_with("dm") {
                continue;
            }
            point.add_field(
                format!("{}_{}", counter_name.trim_matches('"'), n),
                TsValue::Float(f64::from_str(counter_value)?),
            );
        } else if hmap.contains_key("interface") {
            point.add_field(
                format!(
                    "{}_{}",
                    counter_name.trim_matches('"'),
                    &hmap["interface"].trim_matches('"')
                ),
                TsValue::Float(f64::from_str(counter_value)?),
            );
        } else {
            point.add_field(counter_name, TsValue::Float(f64::from_str(counter_value)?));
        }
    }
    Ok(point)
}

// Call out to telegraf and return the result as a Vec<TsPoint>
pub fn get_metrics(
    client: &reqwest::Client,
    config: &TelegrafConfig,
    endpoint: &str,
) -> MetricsResult<TsPoint> {
    let url = format!("http://{}:{}/metrics", endpoint, config.port);
    let text = client
        .get(&url)
        .basic_auth(&config.user, Some(&config.password))
        .send()?
        .error_for_status()?
        .text()?;
    let points = parse_telegraf(&text, Some("ceph_telegraf"))?;
    Ok(points)
}
