use std::fmt::Debug;
use std::str::FromStr;

use crate::error::{MetricsResult, StorageError};
use crate::ir::{TsPoint, TsValue};
use crate::IntoPoint;

use reqwest::{header::HeaderName, header::HeaderValue, StatusCode};
use serde::de::DeserializeOwned;

#[derive(Clone, Deserialize, Debug)]
pub struct OpenstackConfig {
    /// The openstack endpoint to use
    pub endpoint: String,
    pub port: Option<u16>,
    pub user: String,
    /// This gets replaced with the token at runtime
    pub password: String,
    /// Openstack domain to use
    pub domain: String,
    pub project_name: String,
    /// Optional certificate file to use against the server
    /// der encoded
    pub certificate: Option<String>,
    pub region: String,
}

#[derive(Deserialize, Debug)]
pub struct Domain {
    pub description: String,
    pub enabled: bool,
    pub id: String,
    pub name: String,
}

#[derive(Deserialize, Debug)]
pub struct Domains {
    pub domains: Vec<Domain>,
}

#[derive(Deserialize, Debug)]
pub struct Project {
    pub is_domain: Option<bool>,
    pub description: Option<String>,
    pub domain_id: String,
    pub enabled: bool,
    pub id: String,
    pub name: String,
    pub parent_id: Option<String>,
    pub tags: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
pub struct Projects {
    pub projects: Vec<Project>,
}

#[derive(Deserialize, Debug)]
pub struct UserRoot {
    pub user: User,
}

#[derive(Deserialize, Debug)]
pub struct User {
    pub default_project_id: Option<String>,
    pub domain_id: String,
    pub enabled: Option<bool>,
    pub id: String,
    pub name: String,
    pub email: Option<String>,
    pub password_expires_at: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct VolumesAttachment {
    pub server_id: String,
    pub attachment_id: String,
    pub host_name: Option<String>,
    pub volume_id: String,
    pub device: String,
    pub id: String,
}

#[derive(Deserialize, Debug)]
pub struct VolumesMetadatum {
    pub readonly: Option<String>,
    pub attached_mode: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct VolumeImageMetadatum {
    pub kernel_id: Option<String>,
    pub checksum: Option<String>,
    pub min_ram: Option<String>,
    pub ramdisk_id: Option<String>,
    pub disk_format: Option<String>,
    pub image_name: Option<String>,
    pub image_id: Option<String>,
    pub container_format: Option<String>,
    pub min_disk: Option<String>,
    pub size: Option<String>,
}

#[derive(Deserialize, Debug, IntoPoint)]
pub struct Volume {
    pub migration_status: Option<String>,
    pub attachments: Vec<VolumesAttachment>,
    pub availability_zone: String,
    pub os_vol_host_attr_host: Option<String>,
    pub encrypted: bool,
    pub replication_status: String,
    pub snapshot_id: Option<String>,
    pub id: String,
    pub size: u64, // Size is in GB
    pub user_id: String,
    #[serde(rename = "os-vol-tenant-attr:tenant_id")]
    pub os_vol_tenant_attr_tenant_id: String,
    pub os_vol_mig_status_attr_migstat: Option<String>,
    pub metadata: VolumesMetadatum,
    pub status: String,
    pub description: Option<String>,
    pub multiattach: bool,
    pub source_volid: Option<String>,
    pub consistencygroup_id: Option<String>,
    pub os_vol_mig_status_attr_name_id: Option<String>,
    pub name: Option<String>,
    pub bootable: String,
    pub created_at: String,
    pub volume_type: Option<String>,
    pub volume_image_metadata: Option<VolumeImageMetadatum>,
}

#[derive(Deserialize, Debug)]
pub struct Volumes {
    pub volumes: Vec<Volume>,
    pub count: Option<u64>,
}

impl IntoPoint for Volumes {
    fn into_point(&self, name: Option<&str>) -> Vec<TsPoint> {
        let mut points: Vec<TsPoint> = Vec::new();

        for v in &self.volumes {
            points.extend(v.into_point(name));
        }

        points
    }
}

fn get<T>(client: &reqwest::Client, config: &OpenstackConfig, api: &str) -> MetricsResult<T>
where
    T: DeserializeOwned + Debug,
{
    let url = match config.port {
        Some(port) => format!("https://{}:{}/{}", config.endpoint, port, api),
        None => format!("https://{}/{}", config.endpoint, api),
    };

    // This could be more efficient by deserializing immediately but when errors
    // occur it can be really difficult to debug.
    let res: Result<String, reqwest::Error> = client
        .get(&url)
        .header(
            HeaderName::from_str("X-Auth-Token")?,
            HeaderValue::from_str(&config.password)?,
        )
        .send()?
        .error_for_status()?
        .text();
    debug!("raw response: {:?}", res);
    let res = serde_json::from_str(&res?);
    Ok(res?)
}

// Connect to the metadata server and request a new api token
pub fn get_api_token(client: &reqwest::Client, config: &mut OpenstackConfig) -> MetricsResult<()> {
    let auth_json = json!({
        "auth": {
            "identity": {
                "methods": ["password"],
                "password": {
                    "user": {
                        "name": config.user,
                        "domain": {
                            "name": config.domain,
                        },
                        "password": config.password,
                    }
                }
            },
           "scope": {
               "project": {
                   "name": config.project_name,
                   "domain": {
                       "name": "comcast",
                   }
               }
           }
        }
    });
    let url = match config.port {
        Some(port) => format!("https://{}:{}/v3/auth/tokens", config.endpoint, port),
        None => format!("https://{}/v3/auth/tokens", config.endpoint),
    };
    let resp = client
        .post(&url)
        .json(&auth_json)
        .send()?
        .error_for_status()?;
    match resp.status() {
        StatusCode::OK | StatusCode::CREATED => {
            // ok we're good
            let h = resp.headers();

            let token = h.get("X-Subject-Token");
            if token.is_none() {
                return Err(StorageError::new(
                    "openstack token not found in header".to_string(),
                ));
            }
            config.password = token.unwrap().to_str()?.to_owned();
            Ok(())
        }
        StatusCode::UNAUTHORIZED => Err(StorageError::new(format!(
            "Invalid credentials for {}",
            config.user
        ))),
        _ => Err(StorageError::new(format!(
            "Unknown error: {}",
            resp.status()
        ))),
    }
}

pub fn list_domains(
    client: &reqwest::Client,
    config: &OpenstackConfig,
) -> MetricsResult<Vec<Domain>> {
    let domains: Domains = get(&client, &config, "v3/domains")?;

    Ok(domains.domains)
}

pub fn list_projects(
    client: &reqwest::Client,
    config: &OpenstackConfig,
) -> MetricsResult<Vec<Project>> {
    let projects: Projects = get(&client, &config, "v3/projects")?;

    Ok(projects.projects)
}

pub fn list_volumes(
    client: &reqwest::Client,
    config: &OpenstackConfig,
    project_id: &str,
) -> MetricsResult<Vec<TsPoint>> {
    let volumes: Volumes = get(
        &client,
        &config,
        &format!("v3/{}/volumes/detail?all_tenants=True", project_id),
    )?;

    Ok(volumes.into_point(Some("openstack_volume")))
}

#[test]
fn test_list_openstack_volumes() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/openstack/volumes.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: Volumes = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}

pub fn get_user(
    client: &reqwest::Client,
    config: &OpenstackConfig,
    user_id: &str,
) -> MetricsResult<User> {
    let user: UserRoot = get(&client, &config, &format!("/v3/users/{}", user_id))?;

    Ok(user.user)
}

#[test]
fn test_get_openstack_user() {
    use std::fs::File;
    use std::io::Read;

    let mut f = File::open("tests/openstack/user.json").unwrap();
    let mut buff = String::new();
    f.read_to_string(&mut buff).unwrap();

    let i: UserRoot = serde_json::from_str(&buff).unwrap();
    println!("result: {:#?}", i);
}
