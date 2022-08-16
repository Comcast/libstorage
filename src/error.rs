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
use quick_xml::events::attributes::AttrError as QuickXmlAttrError;
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
    QuickXmlAttrError(QuickXmlAttrError)
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            StorageError::CookieError(ref e) => e.fmt(f),
            StorageError::CsvError(ref e) => e.fmt(f),
            StorageError::Error(ref e) => f.write_str(e),
            StorageError::FromUtf8Error(ref e) => e.fmt(f),
            StorageError::HttpError(ref e) => e.fmt(f),
            StorageError::InfluxError(ref e) => match *e {
                InfluxError::SyntaxError(ref s) => f.write_str(s),
                InfluxError::InvalidCredentials(ref s) => f.write_str(s),
                InfluxError::DataBaseDoesNotExist(ref s) => f.write_str(s),
                InfluxError::RetentionPolicyDoesNotExist(ref s) => f.write_str(s),
                InfluxError::Communication(ref s) => f.write_str(s),
                InfluxError::Unknow(ref s) => f.write_str(s),
            },
            StorageError::InvalidHeaderName(ref e) => e.fmt(f),
            StorageError::InvalidHeaderValue(ref e) => e.fmt(f),
            StorageError::IoError(ref e) => e.fmt(f),
            #[cfg(feature = "isilon-library")]
            StorageError::IsilonError(ref e) => e.fmt(f),
            StorageError::JsonError(ref e) => e.fmt(f),
            StorageError::NativeTlsError(ref e) => e.fmt(f),
            StorageError::ParseBoolError(ref e) => e.fmt(f),
            StorageError::ParseError(ref e) => e.fmt(f),
            StorageError::ParseFloatError(ref e) => e.fmt(f),
            StorageError::ParseIntError(ref e) => e.fmt(f),
            StorageError::PostgresError(ref e) => e.fmt(f),
            StorageError::ThreadPoolBuildError(ref e) => e.fmt(f),
            StorageError::TreeXmlError(ref e) => e.fmt(f),
            StorageError::ToStrError(ref e) => e.fmt(f),
            StorageError::XmlEmitterError(ref e) => e.fmt(f),
            StorageError::QuickXmlAttrError( ref e) => e.fmt(f)
        }
    }
}

impl err for StorageError {
    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }
    fn source(&self) -> Option<&(dyn err + 'static)> {
        match *self {
            StorageError::CookieError(ref e) => e.source(),
            StorageError::CsvError(ref e) => e.source(),
            StorageError::Error(_) => None,
            StorageError::FromUtf8Error(ref e) => e.source(),
            StorageError::HttpError(ref e) => e.source(),
            StorageError::InfluxError(ref _e) => None,
            StorageError::InvalidHeaderName(ref e) => e.source(),
            StorageError::InvalidHeaderValue(ref e) => e.source(),
            StorageError::IoError(ref e) => e.source(),
            #[cfg(feature = "isilon-library")]
            StorageError::IsilonError(ref e) => e.source(),
            StorageError::JsonError(ref e) => e.source(),
            StorageError::NativeTlsError(ref e) => e.source(),
            StorageError::ParseBoolError(ref e) => e.source(),
            StorageError::ParseError(ref e) => e.source(),
            StorageError::ParseFloatError(ref e) => e.source(),
            StorageError::ParseIntError(ref e) => e.source(),
            StorageError::PostgresError(ref e) => e.source(),
            StorageError::ThreadPoolBuildError(ref e) => e.source(),
            StorageError::TreeXmlError(ref e) => e.source(),
            StorageError::ToStrError(ref e) => e.source(),
            StorageError::XmlEmitterError(ref e) => e.source(),
            StorageError::QuickXmlAttrError(ref e) => e.source(),
        }
    }
}
impl StorageError {
    /// Create a new StorageError with a String message
    pub fn new(err: String) -> StorageError {
        StorageError::Error(err)
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

impl From<QuickXmlAttrError> for StorageError{
    fn from(err: QuickXmlAttrError) -> StorageError{
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
