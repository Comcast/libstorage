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

#[cfg(feature = "isilon-library")]
extern crate isilon;

use std::error::Error as err;
use std::fmt;
use std::io::Error;
use std::num::{ParseFloatError, ParseIntError};
use std::str::ParseBoolError;
use std::string::{FromUtf8Error, ParseError};

use cookie::ParseError as CookieParseError;
use csv::Error as CsvError;
use influx_db_client::error::Error as InfluxError;
#[cfg(feature = "isilon-library")]
use isilon::apis::Error as IsilonError;
use native_tls::Error as NativeTlsError;
use quick_xml::Error as QuickXmlError;
use rayon::ThreadPoolBuildError;
use reqwest::header::{InvalidHeaderName, InvalidHeaderValue, ToStrError};
use reqwest::Error as ReqwestError;
use serde_json::Error as JsonError;
use treexml::Error as TreeXmlError;
use xml::writer::Error as XmlEmitterError;

pub type MetricsResult<T> = Result<T, StorageError>;

/// Custom error handling
#[derive(Debug)]
pub enum StorageError {
    CookieError(CookieParseError),
    CsvError(CsvError),
    Error(String),
    FromUtf8Error(FromUtf8Error),
    HttpError(ReqwestError),
    InfluxError(InfluxError),
    InvalidHeaderName(InvalidHeaderName),
    InvalidHeaderValue(InvalidHeaderValue),
    IoError(Error),
    #[cfg(feature = "isilon-library")]
    IsilonError(IsilonError),
    JsonError(JsonError),
    NativeTlsError(NativeTlsError),
    ParseBoolError(ParseBoolError),
    ParseError(ParseError),
    ParseFloatError(ParseFloatError),
    ParseIntError(ParseIntError),
    PostgresError(postgres::Error),
    ThreadPoolBuildError(ThreadPoolBuildError),
    ToStrError(ToStrError),
    TreeXmlError(TreeXmlError),
    XmlEmitterError(XmlEmitterError),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.description())
    }
}

impl err for StorageError {
    fn description(&self) -> &str {
        match *self {
            StorageError::CookieError(ref e) => e.description(),
            StorageError::CsvError(ref e) => e.description(),
            StorageError::Error(ref e) => &e,
            StorageError::FromUtf8Error(ref e) => e.description(),
            StorageError::HttpError(ref e) => e.description(),
            StorageError::InfluxError(ref e) => match *e {
                InfluxError::SyntaxError(ref s) => s,
                InfluxError::InvalidCredentials(ref s) => s,
                InfluxError::DataBaseDoesNotExist(ref s) => s,
                InfluxError::RetentionPolicyDoesNotExist(ref s) => s,
                InfluxError::Communication(ref s) => s,
                InfluxError::Unknow(ref s) => s,
            },
            StorageError::InvalidHeaderName(ref e) => e.description(),
            StorageError::InvalidHeaderValue(ref e) => e.description(),
            StorageError::IoError(ref e) => e.description(),
            #[cfg(feature = "isilon-library")]
            StorageError::IsilonError(ref e) => e.description(),
            StorageError::JsonError(ref e) => e.description(),
            StorageError::NativeTlsError(ref e) => e.description(),
            StorageError::ParseBoolError(ref e) => e.description(),
            StorageError::ParseError(ref e) => e.description(),
            StorageError::ParseFloatError(ref e) => e.description(),
            StorageError::ParseIntError(ref e) => e.description(),
            StorageError::PostgresError(ref e) => e.description(),
            StorageError::ThreadPoolBuildError(ref e) => e.description(),
            StorageError::TreeXmlError(ref e) => e.description(),
            StorageError::ToStrError(ref e) => e.description(),
            StorageError::XmlEmitterError(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&dyn err> {
        match *self {
            StorageError::CookieError(ref e) => e.cause(),
            StorageError::CsvError(ref e) => e.cause(),
            StorageError::Error(_) => None,
            StorageError::FromUtf8Error(ref e) => e.cause(),
            StorageError::HttpError(ref e) => e.cause(),
            StorageError::InfluxError(ref _e) => None,
            StorageError::InvalidHeaderName(ref e) => e.cause(),
            StorageError::InvalidHeaderValue(ref e) => e.cause(),
            StorageError::IoError(ref e) => e.cause(),
            #[cfg(feature = "isilon-library")]
            StorageError::IsilonError(ref e) => e.cause(),
            StorageError::JsonError(ref e) => e.cause(),
            StorageError::NativeTlsError(ref e) => e.cause(),
            StorageError::ParseBoolError(ref e) => e.cause(),
            StorageError::ParseError(ref e) => e.cause(),
            StorageError::ParseFloatError(ref e) => e.cause(),
            StorageError::ParseIntError(ref e) => e.cause(),
            StorageError::PostgresError(ref e) => e.cause(),
            StorageError::ThreadPoolBuildError(ref e) => e.cause(),
            StorageError::TreeXmlError(ref e) => e.cause(),
            StorageError::ToStrError(ref e) => e.cause(),
            StorageError::XmlEmitterError(ref e) => e.cause(),
        }
    }
}
impl StorageError {
    /// Create a new StorageError with a String message
    pub fn new(err: String) -> StorageError {
        StorageError::Error(err)
    }

    /// Convert a StorageError into a String representation.
    pub fn to_string(&self) -> String {
        match *self {
            StorageError::CookieError(ref err) => err.to_string(),
            StorageError::CsvError(ref err) => err.to_string(),
            StorageError::Error(ref err) => err.to_string(),
            StorageError::FromUtf8Error(ref err) => err.utf8_error().to_string(),
            StorageError::HttpError(ref err) => err.description().to_string(),
            StorageError::InfluxError(ref err) => err.to_string(),
            StorageError::InvalidHeaderName(ref err) => err.description().to_string(),
            StorageError::InvalidHeaderValue(ref err) => err.description().to_string(),
            StorageError::IoError(ref err) => err.description().to_string(),
            #[cfg(feature = "isilon-library")]
            StorageError::IsilonError(ref err) => err.description().to_string(),
            StorageError::JsonError(ref err) => err.description().to_string(),
            StorageError::NativeTlsError(ref err) => err.description().to_string(),
            StorageError::ParseBoolError(ref err) => err.description().to_string(),
            StorageError::ParseError(ref err) => err.description().to_string(),
            StorageError::ParseFloatError(ref err) => err.description().to_string(),
            StorageError::ParseIntError(ref err) => err.description().to_string(),
            StorageError::PostgresError(ref err) => err.description().to_string(),
            StorageError::ThreadPoolBuildError(ref err) => err.description().to_string(),
            StorageError::TreeXmlError(ref err) => err.description().to_string(),
            StorageError::ToStrError(ref err) => err.description().to_string(),
            StorageError::XmlEmitterError(ref err) => err.description().to_string(),
        }
    }
}

impl From<CookieParseError> for StorageError {
    fn from(err: CookieParseError) -> StorageError {
        StorageError::CookieError(err)
    }
}

impl From<CsvError> for StorageError {
    fn from(err: CsvError) -> StorageError {
        StorageError::CsvError(err)
    }
}

impl From<Error> for StorageError {
    fn from(err: Error) -> StorageError {
        StorageError::IoError(err)
    }
}

impl From<FromUtf8Error> for StorageError {
    fn from(err: FromUtf8Error) -> StorageError {
        StorageError::FromUtf8Error(err)
    }
}

impl From<InvalidHeaderName> for StorageError {
    fn from(err: InvalidHeaderName) -> StorageError {
        StorageError::InvalidHeaderName(err)
    }
}

impl From<InvalidHeaderValue> for StorageError {
    fn from(err: InvalidHeaderValue) -> StorageError {
        StorageError::InvalidHeaderValue(err)
    }
}

impl From<InfluxError> for StorageError {
    fn from(err: InfluxError) -> StorageError {
        StorageError::InfluxError(err)
    }
}

#[cfg(feature = "isilon-library")]
impl From<IsilonError> for StorageError {
    fn from(err: IsilonError) -> StorageError {
        StorageError::IsilonError(err)
    }
}

impl From<JsonError> for StorageError {
    fn from(err: JsonError) -> StorageError {
        StorageError::JsonError(err)
    }
}

impl From<NativeTlsError> for StorageError {
    fn from(err: NativeTlsError) -> StorageError {
        StorageError::NativeTlsError(err)
    }
}

impl From<ParseBoolError> for StorageError {
    fn from(err: ParseBoolError) -> StorageError {
        StorageError::ParseBoolError(err)
    }
}

impl From<ParseFloatError> for StorageError {
    fn from(err: ParseFloatError) -> StorageError {
        StorageError::ParseFloatError(err)
    }
}

impl From<ParseError> for StorageError {
    fn from(err: ParseError) -> StorageError {
        StorageError::ParseError(err)
    }
}

impl From<ParseIntError> for StorageError {
    fn from(err: ParseIntError) -> StorageError {
        StorageError::ParseIntError(err)
    }
}

impl From<postgres::Error> for StorageError {
    fn from(err: postgres::Error) -> StorageError {
        StorageError::PostgresError(err)
    }
}

impl From<String> for StorageError {
    fn from(err: String) -> StorageError {
        StorageError::new(err)
    }
}

impl From<TreeXmlError> for StorageError {
    fn from(err: TreeXmlError) -> StorageError {
        StorageError::TreeXmlError(err)
    }
}

impl From<ThreadPoolBuildError> for StorageError {
    fn from(err: ThreadPoolBuildError) -> StorageError {
        StorageError::ThreadPoolBuildError(err)
    }
}

impl From<ToStrError> for StorageError {
    fn from(err: ToStrError) -> StorageError {
        StorageError::ToStrError(err)
    }
}

impl From<QuickXmlError> for StorageError {
    fn from(err: QuickXmlError) -> StorageError {
        StorageError::new(err.to_string())
    }
}

impl From<ReqwestError> for StorageError {
    fn from(err: ReqwestError) -> StorageError {
        StorageError::HttpError(err)
    }
}

impl From<XmlEmitterError> for StorageError {
    fn from(err: XmlEmitterError) -> StorageError {
        StorageError::XmlEmitterError(err)
    }
}
