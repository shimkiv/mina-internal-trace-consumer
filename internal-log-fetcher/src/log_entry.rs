use crate::{
    graphql::internal_logs_query::InternalLogsQueryInternalLogs,
    utils::convert_timestamp_to_float,
};
use serde::ser::{Serialize, SerializeSeq, Serializer};
use serde_json::Map;
use std::error::Error;

pub(crate) enum LogEntry {
    Control { metadata: serde_json::Value },
    Checkpoint { name: String, timestamp: f64 },
}

impl Serialize for LogEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            LogEntry::Control { metadata } => metadata.serialize(serializer),
            LogEntry::Checkpoint { name, timestamp } => {
                let mut checkpoint_seq = serializer.serialize_seq(Some(2))?;
                checkpoint_seq.serialize_element(name)?;
                checkpoint_seq.serialize_element(timestamp)?;
                checkpoint_seq.end()
            }
        }
    }
}

impl TryFrom<InternalLogsQueryInternalLogs> for LogEntry {
    type Error = Box<dyn Error>;

    fn try_from(value: InternalLogsQueryInternalLogs) -> Result<Self, Self::Error> {
        let name = value.message;

        if name == "@control" {
            let metadata_vec = value.metadata;
            let metadata_map = Map::<String, serde_json::Value>::from_iter(
                metadata_vec.into_iter().map(|item| (item.item, item.value)),
            );
            let metadata = serde_json::Value::Object(metadata_map);

            Ok(LogEntry::Control { metadata })
        } else {
            let timestamp = convert_timestamp_to_float(&value.timestamp)?;
            Ok(LogEntry::Checkpoint { name, timestamp })
        }
    }
}
