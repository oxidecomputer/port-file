use camino::{Utf8Path, Utf8PathBuf};
use std::{
    fmt, io,
    process::ExitStatus,
    str::FromStr,
    thread,
    time::{Duration, Instant},
};

/// The outcome of a [`poll_once`] call.
///
/// This has a similar shape to [`std::task::Poll`], but is defined separately
/// because `Poll` implies waker semantics that don't necessarily apply here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum Readiness<T> {
    /// The port file is available and has been parsed successfully.
    Ready(T),

    /// The port file is not yet available.
    Pending,
}

/// A permanent error indicating the port file could not be read, returned by
/// [`poll_once`].
///
/// This is also part of the [`WaitForError::Poll`] variant.
#[derive(Debug)]
#[non_exhaustive]
pub enum PollError {
    /// The file was present but its contents could not be parsed.
    ///
    /// When the file is produced by [`write`] or another atomic writer, there
    /// is no danger of seeing a partial write, so a parse error is permanent.
    /// However, a non-atomic writer can momentarily expose an incomplete file,
    /// which would be reported as malformed here.
    Malformed {
        /// The path to the port file.
        path: Utf8PathBuf,

        /// The raw contents of the port file read from disk.
        contents: Vec<u8>,

        /// The error describing why the contents could not be parsed.
        ///
        /// For a value that was present but unparseable, this is the error
        /// returned by the [`FromStr`] implementation. It could also be an
        /// error indicating a missing trailing newline or invalid UTF-8.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The file could not be read for a reason other than "not found yet" or
    /// an interrupted read.
    ///
    /// `Io` is returned for all I/O error kinds other than the following:
    ///
    /// * [`io::ErrorKind::NotFound`], since that indicates the port file is
    ///   not present.
    /// * [`io::ErrorKind::Interrupted`], which should be retried.
    /// * On Windows, OS error 32 (`ERROR_SHARING_VIOLATION`), which usually
    ///   indicates an antivirus or other scanner.
    Io {
        /// The path to the port file.
        path: Utf8PathBuf,

        /// The error that occurred while reading the port file.
        source: io::Error,
    },

    /// The child process exited before the port file appeared.
    ProcessExited {
        /// The path to the port file.
        path: Utf8PathBuf,

        /// The exit status of the child process.
        status: ExitStatus,
    },

    /// There was an error determining whether the child process was alive.
    ChildTryWait {
        /// The path to the port file.
        path: Utf8PathBuf,

        /// The error that occurred while trying to wait for the child process.
        source: io::Error,
    },
}

impl PollError {
    /// Returns the path to the port file.
    pub fn path(&self) -> &Utf8Path {
        match self {
            PollError::Malformed { path, .. }
            | PollError::Io { path, .. }
            | PollError::ProcessExited { path, .. }
            | PollError::ChildTryWait { path, .. } => path,
        }
    }
}

impl fmt::Display for PollError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PollError::Malformed { path, contents, .. } => {
                write!(
                    f,
                    "malformed port file {path}: {:?}",
                    String::from_utf8_lossy(contents)
                )
            }
            PollError::Io { path, .. } => {
                write!(f, "failed to read port file {path}")
            }
            PollError::ProcessExited { path, status } => {
                write!(
                    f,
                    "process exited before writing port file {path} \
                     (status: {status})"
                )
            }
            PollError::ChildTryWait { path, .. } => {
                write!(
                    f,
                    "polling whether process was alive for port file {path}"
                )
            }
        }
    }
}

impl std::error::Error for PollError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PollError::Malformed { source, .. } => Some(source.as_ref()),
            PollError::Io { source, .. } => Some(source),
            PollError::ProcessExited { .. } => None,
            PollError::ChildTryWait { source, .. } => Some(source),
        }
    }
}

#[derive(Debug)]
struct MissingTrailingNewline;

impl fmt::Display for MissingTrailingNewline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("file does not end with a newline")
    }
}

impl std::error::Error for MissingTrailingNewline {}

/// Polls a port file once.
///
/// This is the underlying function used to determine whether a port file is
/// available, based on the results of a read attempt and the result of
/// [`Child::try_wait`].
///
/// # Notes
///
/// Callers must call [`Child::try_wait`] _before_ attempting to read the port
/// file. Doing so guarantees that any file the writer produces just before
/// exiting is visible to the reader. The parameter order mirrors this
/// requirement, so it is fine to build the arguments inline as in
/// `poll_once(path, child.try_wait(), std::fs::read(path))`.
///
/// An [`io::ErrorKind::Interrupted`] error from either `exited` or `read` is
/// transient by definition and reported as [`Readiness::Pending`], to be
/// retried on the next poll.
///
/// A file already present at `path` is returned as-is. Callers must ensure that
/// the port file doesn't already exist at the start of the operation, typically
/// by creating a fresh temporary directory to write the port file to. (The
/// [`write`] function enforces this requirement.)
///
/// The read data must be valid UTF-8, and must end with a single trailing
/// newline.
///
/// [`Child::try_wait`]: std::process::Child::try_wait
pub fn poll_once<T>(
    path: &Utf8Path,
    exited: io::Result<Option<ExitStatus>>,
    read: io::Result<Vec<u8>>,
) -> Result<Readiness<T>, PollError>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match read {
        Ok(bytes) => {
            // We reject non-UTF-8 files as malformed.
            let contents = match String::from_utf8(bytes) {
                Ok(contents) => contents,
                Err(error) => {
                    let source = error.utf8_error();
                    return Err(PollError::Malformed {
                        path: path.to_owned(),
                        contents: error.into_bytes(),
                        source: Box::new(source),
                    });
                }
            };
            // Strip the trailing newline that write always adds.
            let Some(payload) = contents.strip_suffix('\n') else {
                return Err(PollError::Malformed {
                    path: path.to_owned(),
                    contents: contents.into_bytes(),
                    source: Box::new(MissingTrailingNewline),
                });
            };
            match payload.parse::<T>() {
                Ok(value) => Ok(Readiness::Ready(value)),
                Err(source) => Err(PollError::Malformed {
                    path: path.to_owned(),
                    contents: contents.into_bytes(),
                    source: Box::new(source),
                }),
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => match exited {
            Ok(Some(status)) => {
                Err(PollError::ProcessExited { path: path.to_owned(), status })
            }
            Ok(None) => Ok(Readiness::Pending),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                Ok(Readiness::Pending)
            }
            Err(source) => {
                Err(PollError::ChildTryWait { path: path.to_owned(), source })
            }
        },
        Err(e) if e.kind() == io::ErrorKind::Interrupted => {
            Ok(Readiness::Pending)
        }
        Err(e) if is_sharing_violation(&e) => Ok(Readiness::Pending),
        Err(source) => Err(PollError::Io { path: path.to_owned(), source }),
    }
}

#[cfg(windows)]
fn is_sharing_violation(error: &io::Error) -> bool {
    // Error 32 (ERROR_SHARING_VIOLATION) is transient, but Rust treats it as
    // ErrorKind::Uncategorized
    error.raw_os_error() == Some(32)
}

#[cfg(not(windows))]
fn is_sharing_violation(_error: &io::Error) -> bool {
    false
}

fn parent_dir_to_check(path: &Utf8Path) -> Option<&Utf8Path> {
    // Bare file names have Some("") as their parent and root paths have None.
    // Neither has a meaningful directory to check.
    let parent = path.parent()?;
    if parent.as_str().is_empty() { None } else { Some(parent) }
}

/// A permanent problem with the port file's parent directory, detected by the
/// wait loops.
#[derive(Debug)]
enum ParentDirProblem {
    /// The parent directory does not exist.
    Missing(Utf8PathBuf),

    /// The parent path exists but is not a directory.
    NotADirectory(Utf8PathBuf),
}

impl ParentDirProblem {
    fn into_error(self, path: &Utf8Path, elapsed: Duration) -> WaitForError {
        match self {
            ParentDirProblem::Missing(parent) => {
                WaitForError::MissingParentDirectory {
                    path: path.to_owned(),
                    parent,
                    elapsed,
                }
            }
            ParentDirProblem::NotADirectory(parent) => {
                WaitForError::ParentNotADirectory {
                    path: path.to_owned(),
                    parent,
                    elapsed,
                }
            }
        }
    }
}

fn classify_parent_dir(
    parent: &Utf8Path,
    metadata: io::Result<std::fs::Metadata>,
) -> Option<ParentDirProblem> {
    match metadata {
        Ok(metadata) if metadata.is_dir() => None,
        // A regular file (or other non-directory) in the parent position means
        // the port file can never be created there. Examining the parent
        // directory is important on Windows, where reading the port file
        // returns "not found" in this situation. (On Unix-like platforms, that
        // read fails with ENOTDIR, which poll_once already treats as
        // permanent.)
        Ok(_) => Some(ParentDirProblem::NotADirectory(parent.to_owned())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Some(ParentDirProblem::Missing(parent.to_owned()))
        }
        // Treat other errors as inconclusive. This check only runs after
        // reading the port file itself reported "not found", and most
        // scenarios that make the metadata call fail (such as permissions
        // issues) make that read also fail with a permanent error.
        Err(_) => None,
    }
}

fn check_parent_dir(path: &Utf8Path) -> Option<ParentDirProblem> {
    let parent = parent_dir_to_check(path)?;
    classify_parent_dir(parent, std::fs::metadata(parent))
}

#[cfg(feature = "tokio")]
async fn check_parent_dir_async(path: &Utf8Path) -> Option<ParentDirProblem> {
    let parent = parent_dir_to_check(path)?;
    classify_parent_dir(parent, tokio::fs::metadata(parent).await)
}

/// How long [`wait_for`] and [`wait_for_blocking`] sleep between polls of the
/// port file.
///
/// A typical value for the poll interval is 25 milliseconds.
///
/// Sleeps are clamped to the time remaining before the [`Timeout`] deadline, so
/// a long poll interval does not end up in the wait functions waiting past the
/// deadline.
///
/// This is a newtype (rather than a bare [`Duration`]) so it cannot be
/// confused with [`Timeout`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PollInterval(pub Duration);

/// How long [`wait_for`] and [`wait_for_blocking`] keep polling before giving
/// up.
///
/// A typical value for the timeout is 30 seconds.
///
/// Note that the wait functions poll for the port file at least once even with
/// a zero timeout, and poll one final time once the deadline passes. This means
/// that a file appearing before the deadline is always found. (A zero timeout
/// acts as a single non-blocking check.)
///
/// This is a newtype (rather than a bare [`Duration`]) so it cannot be
/// confused with [`PollInterval`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timeout(pub Duration);

/// A permanent error returned by [`wait_for`] and [`wait_for_blocking`].
#[derive(Debug)]
#[non_exhaustive]
pub enum WaitForError {
    /// A poll operation returned a permanent error.
    Poll {
        /// The error that occurred.
        error: PollError,
        /// How long the driver had been waiting when the error occurred.
        elapsed: Duration,
    },

    /// The parent directory of the port file does not exist.
    MissingParentDirectory {
        /// The path to the port file.
        path: Utf8PathBuf,

        /// The parent directory that does not exist.
        parent: Utf8PathBuf,

        /// How long the driver had been waiting when the error occurred.
        elapsed: Duration,
    },

    /// The parent path of the port file exists but is not a directory.
    ///
    /// This is most likely to occur on Windows. On Unix-like platforms, this
    /// condition usually surfaces as [`WaitForError::Poll`] with
    /// [`PollError::Io`] instead, because reading the port file itself fails
    /// with a "not a directory" error.
    ParentNotADirectory {
        /// The path to the port file.
        path: Utf8PathBuf,

        /// The parent path that is not a directory.
        parent: Utf8PathBuf,

        /// How long the driver had been waiting when the error occurred.
        elapsed: Duration,
    },

    /// The timeout was reached.
    TimedOut {
        /// The path of the port file that was being waited for.
        path: Utf8PathBuf,
        /// How long the driver had been waiting when the error occurred.
        elapsed: Duration,
    },
}

impl WaitForError {
    /// Returns the path of the port file that was being waited for.
    pub fn path(&self) -> &Utf8Path {
        match self {
            WaitForError::Poll { error, .. } => error.path(),
            WaitForError::MissingParentDirectory { path, .. }
            | WaitForError::ParentNotADirectory { path, .. }
            | WaitForError::TimedOut { path, .. } => path,
        }
    }

    /// Returns how long the driver had been waiting when the error occurred.
    pub fn elapsed(&self) -> Duration {
        match self {
            WaitForError::Poll { elapsed, .. }
            | WaitForError::MissingParentDirectory { elapsed, .. }
            | WaitForError::ParentNotADirectory { elapsed, .. }
            | WaitForError::TimedOut { elapsed, .. } => *elapsed,
        }
    }
}

impl fmt::Display for WaitForError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WaitForError::Poll { error, elapsed } => {
                write!(f, "{error} (after {elapsed:?})")
            }
            WaitForError::MissingParentDirectory { path, parent, elapsed } => {
                write!(
                    f,
                    "parent directory {parent} does not exist, so port file \
                     {path} can never appear (after {elapsed:?})"
                )
            }
            WaitForError::ParentNotADirectory { path, parent, elapsed } => {
                write!(
                    f,
                    "parent path {parent} is not a directory, so port file \
                     {path} can never appear (after {elapsed:?})"
                )
            }
            WaitForError::TimedOut { path, elapsed } => {
                write!(
                    f,
                    "timed out after {elapsed:?} waiting for port file {path}"
                )
            }
        }
    }
}

impl std::error::Error for WaitForError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WaitForError::Poll { error, .. } => {
                std::error::Error::source(error)
            }
            WaitForError::MissingParentDirectory { .. }
            | WaitForError::ParentNotADirectory { .. }
            | WaitForError::TimedOut { .. } => None,
        }
    }
}

/// Repeatedly polls `path` with [`thread::sleep`] until completion.
///
/// `child_try_wait` is typically `|| child.try_wait()` on a
/// [`std::process::Child`].
///
/// Note that unlike [`wait_for`], a blocking wait cannot be cancelled.
///
/// # Stale port files
///
/// A file already present at `path` is returned as-is. Callers must ensure that
/// the port file doesn't already exist at the start of the operation, typically
/// by creating a fresh temporary directory to write the port file to. (The
/// [`write`] function enforces this requirement.)
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use port_file::{PollInterval, Timeout};
/// use std::{net::SocketAddr, time::Duration};
///
/// let dir = camino_tempfile::tempdir()?;
/// let path = dir.path().join("service.port");
///
/// // Ordinarily a child process publishes its address. In this example,
/// // we write it inline here.
/// port_file::write(&path, "[::1]:8080".parse::<SocketAddr>()?)?;
///
/// let addr: SocketAddr = port_file::wait_for_blocking(
///     &path,
///     // With a real child process, pass in `|| child.try_wait()` instead.
///     || Ok(None),
///     PollInterval(Duration::from_millis(25)),
///     Timeout(Duration::from_secs(30)),
/// )?;
/// assert_eq!(addr, "[::1]:8080".parse()?);
/// # Ok(())
/// # }
/// ```
pub fn wait_for_blocking<T>(
    path: &Utf8Path,
    mut child_try_wait: impl FnMut() -> io::Result<Option<ExitStatus>>,
    poll_interval: PollInterval,
    timeout: Timeout,
) -> Result<T, WaitForError>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let start = Instant::now();
    loop {
        // Call child_try_wait before reading the file, as documented by
        // poll_once.
        let exited = child_try_wait();
        let read = std::fs::read(path);
        match poll_once::<T>(path, exited, read) {
            Ok(Readiness::Ready(value)) => return Ok(value),
            Ok(Readiness::Pending) => {
                if let Some(problem) = check_parent_dir(path) {
                    return Err(problem.into_error(path, start.elapsed()));
                }
            }
            Err(error) => {
                return Err(WaitForError::Poll {
                    error,
                    elapsed: start.elapsed(),
                });
            }
        }

        // The deadline check runs only after a poll, and the final sleep is
        // clamped to the deadline. This guarantees that a file appearing before
        // the deadline is always seen, and that a zero timeout still polls
        // exactly once.
        let elapsed = start.elapsed();
        if elapsed >= timeout.0 {
            return Err(WaitForError::TimedOut {
                path: path.to_owned(),
                elapsed,
            });
        }
        thread::sleep(poll_interval.0.min(timeout.0 - elapsed));
    }
}

/// Polls `path` in an asynchronous fashion until completion.
///
/// `child_try_wait` is typically `|| child.try_wait()` on a
/// [`std::process::Child`] or [`tokio::process::Child`].
///
/// # Stale port files
///
/// A file already present at `path` is returned as-is. Callers must ensure that
/// the port file doesn't already exist at the start of the operation, typically
/// by creating a fresh temporary directory to write the port file to. (The
/// [`write`] function enforces this requirement.)
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use port_file::{PollInterval, Timeout};
/// use std::{net::SocketAddr, time::Duration};
///
/// let dir = camino_tempfile::tempdir()?;
/// let path = dir.path().join("service.port");
///
/// // Ordinarily a child process publishes its address. In this example,
/// // we write it inline here.
/// port_file::write(&path, "[::1]:8080".parse::<SocketAddr>()?)?;
///
/// let addr: SocketAddr = port_file::wait_for(
///     &path,
///     // With a real child process, pass in `|| child.try_wait()` instead.
///     || Ok(None),
///     PollInterval(Duration::from_millis(25)),
///     Timeout(Duration::from_secs(30)),
/// )
/// .await?;
/// assert_eq!(addr, "[::1]:8080".parse()?);
/// # Ok(())
/// # }
/// ```
///
/// [`tokio::process::Child`]: https://docs.rs/tokio/1/tokio/process/struct.Child.html
#[cfg(feature = "tokio")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "tokio")))]
pub async fn wait_for<T>(
    path: &Utf8Path,
    mut child_try_wait: impl FnMut() -> io::Result<Option<ExitStatus>>,
    poll_interval: PollInterval,
    timeout: Timeout,
) -> Result<T, WaitForError>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let start = tokio::time::Instant::now();
    // We poll explicitly for the child process being alive rather than waiting
    // for it to exit. We could potentially use a select loop instead, but there
    // isn't much benefit to that and it prevents abstracting out `poll_once`
    // into a separate function.
    loop {
        // Call child_try_wait before reading the file, as documented by
        // poll_once.
        let exited = child_try_wait();
        let read = tokio::fs::read(path).await;
        match poll_once::<T>(path, exited, read) {
            Ok(Readiness::Ready(value)) => return Ok(value),
            Ok(Readiness::Pending) => {
                if let Some(problem) = check_parent_dir_async(path).await {
                    return Err(problem.into_error(path, start.elapsed()));
                }
            }
            Err(error) => {
                return Err(WaitForError::Poll {
                    error,
                    elapsed: start.elapsed(),
                });
            }
        }

        // The deadline check runs only after a poll, and the final sleep is
        // clamped to the deadline. This guarantees that a file appearing before
        // the deadline is always seen, and that a zero timeout still polls
        // exactly once.
        let elapsed = start.elapsed();
        if elapsed >= timeout.0 {
            return Err(WaitForError::TimedOut {
                path: path.to_owned(),
                elapsed,
            });
        }
        tokio::time::sleep(poll_interval.0.min(timeout.0 - elapsed)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn missing() -> io::Result<Vec<u8>> {
        Err(io::Error::from(io::ErrorKind::NotFound))
    }

    fn bytes(contents: &str) -> io::Result<Vec<u8>> {
        Ok(contents.as_bytes().to_vec())
    }

    #[cfg(unix)]
    fn exited_status() -> ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        ExitStatus::from_raw(1 << 8)
    }

    #[cfg(windows)]
    fn exited_status() -> ExitStatus {
        use std::os::windows::process::ExitStatusExt;
        ExitStatus::from_raw(1)
    }

    #[test]
    fn parses_bound_socket_addr() {
        let path = Utf8Path::new("port");
        let addr: SocketAddr = "[::1]:4676".parse().unwrap();
        assert_eq!(
            poll_once::<SocketAddr>(path, Ok(None), bytes("[::1]:4676\n"))
                .unwrap(),
            Readiness::Ready(addr)
        );
    }

    #[test]
    fn empty_file_is_malformed() {
        let path = Utf8Path::new("port");
        let err = poll_once::<SocketAddr>(path, Ok(None), Ok(Vec::new()))
            .unwrap_err();
        let PollError::Malformed { path: p, contents, source } = err else {
            panic!("expected Malformed, got {err:?}");
        };
        assert_eq!(p, path);
        assert_eq!(contents, b"");
        assert!(
            source.downcast_ref::<MissingTrailingNewline>().is_some(),
            "empty file is missing its trailing newline: {source}"
        );
    }

    #[test]
    fn whitespace_only_file_is_malformed() {
        let path = Utf8Path::new("port");
        let err = poll_once::<SocketAddr>(path, Ok(None), bytes("   \n"))
            .unwrap_err();
        let PollError::Malformed { path: p, contents, .. } = err else {
            panic!("expected Malformed, got {err:?}");
        };
        assert_eq!(p, path);
        assert_eq!(contents, b"   \n");
    }

    #[test]
    fn non_utf8_file_is_malformed() {
        let path = Utf8Path::new("port");
        let err =
            poll_once::<SocketAddr>(path, Ok(None), Ok(vec![0xff, b'\n']))
                .unwrap_err();
        let PollError::Malformed { path: p, contents, source } = err else {
            panic!("expected Malformed, got {err:?}");
        };
        assert_eq!(p, path);
        assert_eq!(contents, b"\xff\n");
        assert!(
            source.downcast_ref::<std::str::Utf8Error>().is_some(),
            "source should be a Utf8Error: {source}"
        );
    }

    #[test]
    fn missing_trailing_newline_is_malformed() {
        // Test that the trailing newline is mandatory.
        let path = Utf8Path::new("port");
        let err = poll_once::<SocketAddr>(path, Ok(None), bytes("[::1]:4676"))
            .unwrap_err();
        let PollError::Malformed { path: p, contents, .. } = err else {
            panic!("expected Malformed, got {err:?}");
        };
        assert_eq!(p, path);
        assert_eq!(contents, b"[::1]:4676");
    }

    #[test]
    fn only_trailing_newline_is_stripped() {
        // Test that we strip exactly one trailing newline.
        let path = Utf8Path::new("port");
        assert_eq!(
            poll_once::<String>(path, Ok(None), bytes("  spaced  \n")).unwrap(),
            Readiness::Ready("  spaced  ".to_owned())
        );
    }

    #[test]
    fn missing_file_is_pending_while_alive() {
        let path = Utf8Path::new("port");
        assert_eq!(
            poll_once::<SocketAddr>(path, Ok(None), missing()).unwrap(),
            Readiness::Pending
        );
    }

    #[test]
    fn missing_file_after_exit_is_permanent() {
        // Test that a "not found" read paired with a dead writer is a permanent
        // error.
        let path = Utf8Path::new("port");
        let err =
            poll_once::<SocketAddr>(path, Ok(Some(exited_status())), missing())
                .unwrap_err();
        let PollError::ProcessExited { path: p, .. } = err else {
            panic!("expected ProcessExited, got {err:?}");
        };
        assert_eq!(p, path);
    }

    #[test]
    fn malformed_file_is_permanent() {
        let path = Utf8Path::new("port");
        let err =
            poll_once::<SocketAddr>(path, Ok(None), bytes("not-an-addr\n"))
                .unwrap_err();
        let PollError::Malformed { path: p, contents, .. } = err else {
            panic!("expected Malformed, got {err:?}");
        };
        assert_eq!(p, path);
        assert_eq!(contents, b"not-an-addr\n");
    }

    #[test]
    fn malformed_file_wins_over_exit() {
        let path = Utf8Path::new("port");
        let err = poll_once::<SocketAddr>(
            path,
            Ok(Some(exited_status())),
            bytes("garbage\n"),
        )
        .unwrap_err();
        let PollError::Malformed { .. } = err else {
            panic!("unexpected error: {err:?}");
        };
    }

    #[test]
    fn valid_file_wins_over_exit() {
        let path = Utf8Path::new("port");
        let addr: SocketAddr = "[::1]:4676".parse().unwrap();
        assert_eq!(
            poll_once::<SocketAddr>(
                path,
                Ok(Some(exited_status())),
                bytes("[::1]:4676\n")
            )
            .unwrap(),
            Readiness::Ready(addr)
        );
    }

    #[test]
    fn io_error_is_permanent() {
        let path = Utf8Path::new("port");
        let err = poll_once::<SocketAddr>(
            path,
            Ok(None),
            Err(io::Error::from(io::ErrorKind::PermissionDenied)),
        )
        .unwrap_err();
        let PollError::Io { path: p, source } = err else {
            panic!("expected Io, got {err:?}");
        };
        assert_eq!(p, path);
        assert_eq!(source.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn child_try_wait_error_is_permanent() {
        let path = Utf8Path::new("port");
        let err = poll_once::<SocketAddr>(
            path,
            Err(io::Error::from(io::ErrorKind::Other)),
            missing(),
        )
        .unwrap_err();
        let PollError::ChildTryWait { path: p, .. } = err else {
            panic!("expected ChildTryWait, got {err:?}");
        };
        assert_eq!(p, path);
    }

    #[test]
    fn interrupted_read_is_pending() {
        // Test that an interrupted read is transient, not a permanent Io
        // error.
        let path = Utf8Path::new("port");
        assert_eq!(
            poll_once::<SocketAddr>(
                path,
                Ok(None),
                Err(io::Error::from(io::ErrorKind::Interrupted)),
            )
            .unwrap(),
            Readiness::Pending
        );
    }

    #[test]
    fn interrupted_child_try_wait_is_pending() {
        // Test that an interrupted child_try_wait is transient, not a
        // permanent ChildTryWait error.
        let path = Utf8Path::new("port");
        assert_eq!(
            poll_once::<SocketAddr>(
                path,
                Err(io::Error::from(io::ErrorKind::Interrupted)),
                missing(),
            )
            .unwrap(),
            Readiness::Pending
        );
    }

    #[test]
    #[cfg(windows)]
    fn sharing_violation_is_pending() {
        let path = Utf8Path::new("port");
        assert_eq!(
            poll_once::<SocketAddr>(
                path,
                Ok(None),
                Err(io::Error::from_raw_os_error(32)),
            )
            .unwrap(),
            Readiness::Pending
        );
    }

    #[test]
    fn wait_for_error_poll_is_transparent() {
        let inner = PollError::Io {
            path: Utf8PathBuf::from("port"),
            source: io::Error::from(io::ErrorKind::PermissionDenied),
        };
        let inner_message = inner.to_string();

        let wrapped =
            WaitForError::Poll { error: inner, elapsed: Duration::ZERO };
        assert_eq!(wrapped.to_string(), format!("{inner_message} (after 0ns)"));

        let source = std::error::Error::source(&wrapped)
            .expect("Poll delegates to the PollError's source");
        assert!(
            source.downcast_ref::<io::Error>().is_some(),
            "source is the underlying io::Error, not the PollError: {source}"
        );
    }

    #[test]
    fn parent_dir_to_check_skips_bare_and_root_paths() {
        assert_eq!(
            parent_dir_to_check(Utf8Path::new("port")),
            None,
            "a bare file name has no directory to check"
        );
        assert_eq!(
            parent_dir_to_check(Utf8Path::new("/")),
            None,
            "the root has no parent"
        );
        assert_eq!(
            parent_dir_to_check(Utf8Path::new("dir/port")),
            Some(Utf8Path::new("dir"))
        );
        assert_eq!(
            parent_dir_to_check(Utf8Path::new("/port")),
            Some(Utf8Path::new("/"))
        );
    }

    #[test]
    fn check_parent_dir_reports_missing_directory() {
        let dir = camino_tempfile::tempdir().unwrap();
        let present = dir.path().join("port");
        assert!(
            check_parent_dir(&present).is_none(),
            "an existing parent directory is not an error"
        );

        let missing = dir.path().join("missing_dir").join("port");
        let problem = check_parent_dir(&missing)
            .expect("a missing parent directory is a permanent problem");
        let ParentDirProblem::Missing(parent) = &problem else {
            panic!("expected Missing, got {problem:?}");
        };
        assert_eq!(parent, &dir.path().join("missing_dir"));

        let error = problem.into_error(&missing, Duration::from_secs(1));
        let WaitForError::MissingParentDirectory { path, parent, elapsed } =
            &error
        else {
            panic!("expected MissingParentDirectory, got {error:?}");
        };
        assert_eq!(path, &missing);
        assert_eq!(parent, &dir.path().join("missing_dir"));
        assert_eq!(*elapsed, Duration::from_secs(1));
    }

    #[test]
    fn check_parent_dir_reports_file_in_parent_position() {
        // Test that a regular file where the parent directory should be is a
        // permanent problem. This matters on Windows, where reading the port
        // file itself reports "not found" rather than "not a directory".
        let dir = camino_tempfile::tempdir().unwrap();
        let occupied = dir.path().join("occupied");
        std::fs::write(&occupied, "not a directory").unwrap();

        let path = occupied.join("port");
        let problem = check_parent_dir(&path)
            .expect("a file in the parent position is a permanent problem");
        let ParentDirProblem::NotADirectory(parent) = &problem else {
            panic!("expected NotADirectory, got {problem:?}");
        };
        assert_eq!(parent, &occupied);

        let error = problem.into_error(&path, Duration::from_secs(1));
        let WaitForError::ParentNotADirectory { path: p, parent, elapsed } =
            &error
        else {
            panic!("expected ParentNotADirectory, got {error:?}");
        };
        assert_eq!(p, &path);
        assert_eq!(parent, &occupied);
        assert_eq!(*elapsed, Duration::from_secs(1));
    }
}
