// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use std::{
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

pub fn convert_timestamp_to_float(timestamp: &str) -> Result<f64, chrono::ParseError> {
    let dt_naive: NaiveDateTime =
        NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S%.fZ")?;
    let dt_utc: DateTime<Utc> = DateTime::<Utc>::from_utc(dt_naive, Utc);

    let duration_since_epoch = dt_utc.timestamp_nanos() as f64;
    let float_value = duration_since_epoch / 1_000_000_000_f64;

    Ok(float_value)
}

pub fn read_secret_key_base64(secret_key_path: &PathBuf) -> Result<String> {
    std::fs::read_to_string(secret_key_path)
        .with_context(|| format!("Failed to read secret key from {:?}", secret_key_path))
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
}
