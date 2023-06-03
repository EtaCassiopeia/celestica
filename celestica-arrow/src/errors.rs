use core::fmt;
use std::error::Error;
use std::fmt::{Debug, Display};

use datafusion::error::DataFusionError;
use lancedb::error::Error as LanceDBError;

pub struct LanceErrorWrapper {
    error: LanceDBError,
}

impl Error for LanceErrorWrapper {}

impl Debug for LanceErrorWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.error {
            LanceDBError::TableNotFound { name } => {
                write!(f, "Table not found: {}", name)
            },
            LanceDBError::InvalidTableName { name } => {
                write!(f, "Invalid table name: {}", name)
            },
            LanceDBError::TableAlreadyExists { name } => {
                write!(f, "Table already exists: {}", name)
            },
            LanceDBError::CreateDir { path, source } => {
                write!(f, "Failed to create directory: {}", path)
            },
            LanceDBError::Store { message } => {
                write!(f, "Store error: {}", message)
            },
            LanceDBError::Lance { message } => {
                write!(f, "Lance error: {}", message)
            },
        }
    }
}

impl Display for LanceErrorWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.error {
            LanceDBError::TableNotFound { name } => {
                write!(f, "Table not found: {}", name)
            },
            LanceDBError::InvalidTableName { name } => {
                write!(f, "Invalid table name: {}", name)
            },
            LanceDBError::TableAlreadyExists { name } => {
                write!(f, "Table already exists: {}", name)
            },
            LanceDBError::CreateDir { path, source } => {
                write!(f, "Failed to create directory: {}", path)
            },
            LanceDBError::Store { message } => {
                write!(f, "Store error: {}", message)
            },
            LanceDBError::Lance { message } => {
                write!(f, "Lance error: {}", message)
            },
        }
    }
}

impl From<LanceDBError> for LanceErrorWrapper {
    fn from(error: LanceDBError) -> Self {
        Self { error }
    }
}

impl From<LanceErrorWrapper> for DataFusionError {
    fn from(e: LanceErrorWrapper) -> Self {
        match e.error {
            LanceDBError::TableNotFound { .. } => {
                DataFusionError::Execution(e.to_string())
            },
            LanceDBError::InvalidTableName { .. } => {
                DataFusionError::Execution(e.to_string())
            },
            LanceDBError::TableAlreadyExists { .. } => {
                DataFusionError::Execution(e.to_string())
            },
            LanceDBError::CreateDir { .. } => DataFusionError::Execution(e.to_string()),
            LanceDBError::Store { .. } => DataFusionError::Execution(e.to_string()),
            LanceDBError::Lance { .. } => DataFusionError::Execution(e.to_string()),
        }
    }
}
