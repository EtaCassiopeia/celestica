use std::fs;
use std::path::Path;

use lance::io::ObjectStore;
use lancedb::error::Error as LanceDBError;

pub async fn create_if_not_exists(uri: &str) -> Result<(), LanceDBError> {
    let object_store =
        ObjectStore::new(uri)
            .await
            .map_err(|error| LanceDBError::Store {
                message: error.to_string(),
            })?;
    if object_store.is_local() {
        try_create_dir(uri)
            .await
            .map_err(|error| LanceDBError::CreateDir {
                path: uri.to_string(),
                source: error,
            })?;
    }

    Ok(())
}

/// Try to create a local directory to store the lance dataset

async fn try_create_dir(path: &str) -> Result<(), std::io::Error> {
    let path = Path::new(path);
    if !path.try_exists()? {
        fs::create_dir_all(&path)?;
    }
    Ok(())
}
