//! Integration tests for port-file.

#[cfg(feature = "tokio")]
mod wait_for;
mod wait_for_blocking;
mod write;

use std::{net::SocketAddr, process::ExitStatus};

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

fn socket_addr() -> SocketAddr {
    "127.0.0.1:8080".parse().unwrap()
}
