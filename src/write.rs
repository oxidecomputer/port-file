use atomicwrites::{AtomicFile, DisallowOverwrite};
use camino::{Utf8Path, Utf8PathBuf};
use std::{
    fmt::{self, Display},
    io::{self, Write},
};

/// The stage of an atomic write that failed, returned by
/// [`WriteError::stage`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteStage {
    /// Writing the serialized value to the temporary file failed.
    Write,

    /// Creating the temporary file, syncing it to disk, or atomically
    /// renaming it to the destination failed.
    Persist,
}

/// An error that occurred while writing the port file.
#[derive(Debug)]
pub struct WriteError {
    path: Utf8PathBuf,
    stage: WriteStage,
    source: io::Error,
}

impl WriteError {
    fn new(path: &Utf8Path, error: atomicwrites::Error<io::Error>) -> Self {
        let (stage, source) = match error {
            atomicwrites::Error::Internal(source) => {
                (WriteStage::Persist, source)
            }
            atomicwrites::Error::User(source) => (WriteStage::Write, source),
        };
        WriteError { path: path.to_owned(), stage, source }
    }

    /// Returns the file path we failed to write.
    pub fn path(&self) -> &Utf8Path {
        &self.path
    }

    /// Returns the stage of the atomic write that failed.
    pub fn stage(&self) -> WriteStage {
        self.stage
    }
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.stage {
            WriteStage::Write => write!(
                f,
                "writing value to temporary file for port file {}",
                self.path
            ),
            WriteStage::Persist => write!(
                f,
                "creating, syncing, or renaming temporary file for port \
                 file {}",
                self.path
            ),
        }
    }
}

impl std::error::Error for WriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Writes a value such as a [`SocketAddr`] to the path.
///
/// # Notes
///
/// The path must not already exist. A write to an existing file will fail with
/// a [`WriteStage::Persist`] error.
///
/// The value is written atomically via a temporary file and a rename. This
/// means that readers will not ever see torn or partial writes.
///
/// `write` is generic over [`Display`], so callers can publish any value with a
/// [`FromStr`] counterpart on the reader, assuming [`Display`] and [`FromStr`]
/// roundtrip. In most cases, you will pass in a [`SocketAddr`] here.
///
/// A single trailing newline is written after the value. The read side of this
/// crate expects (and will strip) exactly one trailing newline.
///
/// [`SocketAddr`]: std::net::SocketAddr
/// [`FromStr`]: std::str::FromStr
pub fn write<T: Display>(path: &Utf8Path, value: T) -> Result<(), WriteError> {
    AtomicFile::new(path, DisallowOverwrite)
        .write(|f| writeln!(f, "{value}"))
        .map_err(|error| WriteError::new(path, error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_error_is_write_stage() {
        let path = Utf8Path::new("port");
        let error = atomicwrites::Error::User(io::Error::from(
            io::ErrorKind::StorageFull,
        ));
        let err = WriteError::new(path, error);
        assert_eq!(err.path(), path);
        assert_eq!(err.stage(), WriteStage::Write);
        assert!(
            err.to_string().contains("writing value to temporary file"),
            "unexpected error: {err}"
        );
        assert_eq!(err.source.kind(), io::ErrorKind::StorageFull);
    }

    #[test]
    fn internal_error_is_persist_stage() {
        let path = Utf8Path::new("port");
        let error = atomicwrites::Error::Internal(io::Error::from(
            io::ErrorKind::PermissionDenied,
        ));
        let err = WriteError::new(path, error);
        assert_eq!(err.path(), path);
        assert_eq!(err.stage(), WriteStage::Persist);
        assert!(
            err.to_string()
                .contains("creating, syncing, or renaming temporary file"),
            "unexpected error: {err}"
        );
        assert_eq!(err.source.kind(), io::ErrorKind::PermissionDenied);
    }
}
