use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use crate::StoreError;

pub fn restrict_permissions(path: &Path) -> Result<(), StoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        if path != Path::new(":memory:") {
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))
                .map_err(|error| io_error(&error))?;
        }
    }
    Ok(())
}

pub fn remove_database(path: &Path) -> Result<(), StoreError> {
    let main_result = fs::remove_file(path);
    remove_sidecar(path, "-wal")?;
    remove_sidecar(path, "-shm")?;
    match main_result {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(&error)),
    }
}

fn remove_sidecar(path: &Path, suffix: &str) -> Result<(), StoreError> {
    let sidecar = PathBuf::from(format!("{}{suffix}", path.to_string_lossy()));
    match fs::remove_file(sidecar) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(&error)),
    }
}

fn io_error(error: &std::io::Error) -> StoreError {
    StoreError::Storage {
        message: error.to_string(),
    }
}
