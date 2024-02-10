// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use std::{
    collections::HashMap,
    fmt,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

pub fn maybe_open<P>(file_opt: &mut Option<File>, path: P) -> Result<&mut File>
where
    P: AsRef<Path>,
{
    if let Some(file) = file_opt {
        Ok(file)
    } else {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(file_opt.insert(file))
    }
}

// Define a custom error type.
#[derive(Debug)]
pub struct TsConversionError(String);

// Implement the Display trait for TsConversionError.
impl fmt::Display for TsConversionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Implement the Error trait for TsConversionError.
impl std::error::Error for TsConversionError {}

pub fn convert_timestamp_to_float(timestamp: &str) -> Result<f64, TsConversionError> {
    let dt_naive: NaiveDateTime = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S%.fZ")
        .map_err(|e| TsConversionError(e.to_string()))?;
    let dt_utc: DateTime<Utc> = DateTime::<Utc>::from_naive_utc_and_offset(dt_naive, Utc);

    let duration_since_epoch = dt_utc
        .timestamp_nanos_opt()
        .ok_or(TsConversionError("conversion to nanos failed".to_string()))?;
    let float_value = (duration_since_epoch as f64) / 1_000_000_000_f64;

    Ok(float_value)
}

pub fn read_secret_key_base64(secret_key_path: &PathBuf) -> Result<String> {
    std::fs::read_to_string(secret_key_path)
        .with_context(|| format!("Failed to read secret key from {:?}", secret_key_path))
}

pub fn load_node_name_map(node_name_map_path: &PathBuf) -> Result<HashMap<String, String>> {
    let file = File::open(node_name_map_path)?;
    Ok(serde_json::from_reader(file)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_timestamp_to_float() {
        let timestamp = "2023-05-11 19:48:35.588434Z";
        let expected_float = 1683834515.588434;
        let float_tolerance = 1.0e-6;

        let result = convert_timestamp_to_float(timestamp).unwrap();
        assert!(
            (result - expected_float).abs() < float_tolerance,
            "Expected value: {}, computed value: {}",
            expected_float,
            result
        );
    }

    #[test]
    fn test_load_node_name_map() {
        let name_map = load_node_name_map(&PathBuf::from("test-data/node_names.json"))
            .expect("Failed to read names map json");
    }
}
