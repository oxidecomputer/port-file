use crate::{exited_status, socket_addr};
use port_file::{
    PollError, PollInterval, Timeout, WaitForError, wait_for_blocking,
};
use std::{net::SocketAddr, time::Duration};

const INTERVAL: PollInterval = PollInterval(Duration::from_millis(5));

#[test]
fn returns_written_value() {
    let dir = camino_tempfile::tempdir().unwrap();
    let path = dir.path().join("port");
    let addr = socket_addr();
    port_file::write(&path, addr).unwrap();

    let discovered: SocketAddr = wait_for_blocking(
        &path,
        || Ok(None),
        INTERVAL,
        Timeout(Duration::from_secs(5)),
    )
    .expect("value is already present");
    assert_eq!(discovered, addr);
}

#[test]
fn samples_child_try_wait_before_read() {
    let dir = camino_tempfile::tempdir().unwrap();
    let path = dir.path().join("port");
    let addr = socket_addr();

    let mut wrote = false;
    let discovered: SocketAddr = wait_for_blocking(
        &path,
        || {
            if !wrote {
                wrote = true;
                port_file::write(&path, addr).unwrap();
            }
            Ok(Some(exited_status()))
        },
        INTERVAL,
        Timeout(Duration::from_secs(5)),
    )
    .expect("a file written just before exit must still be found");
    assert_eq!(discovered, addr);
}

// Test Pending -> Ready.
#[test]
fn retries_until_written() {
    let dir = camino_tempfile::tempdir().unwrap();
    let path = dir.path().join("port");
    let addr = socket_addr();

    let mut calls = 0u32;
    let discovered: SocketAddr = wait_for_blocking(
        &path,
        || {
            calls += 1;
            if calls == 3 {
                port_file::write(&path, addr).unwrap();
            }
            Ok(None)
        },
        INTERVAL,
        Timeout(Duration::from_secs(30)),
    )
    .expect("value appears on the third poll");
    assert_eq!(discovered, addr);
    assert!(calls >= 3, "expected at least three polls, got {calls}");
}

#[test]
fn rejects_malformed() {
    let dir = camino_tempfile::tempdir().unwrap();
    let path = dir.path().join("port");
    std::fs::write(&path, "not-an-addr\n").unwrap();

    let err = wait_for_blocking::<SocketAddr>(
        &path,
        || Ok(None),
        INTERVAL,
        Timeout(Duration::from_secs(5)),
    )
    .expect_err("a present-but-unparseable file must fail fast");
    let WaitForError::Poll { error: PollError::Malformed { .. }, .. } = err
    else {
        panic!("unexpected error: {err:?}");
    };
}

#[test]
fn times_out_when_file_never_appears() {
    let dir = camino_tempfile::tempdir().unwrap();
    let path = dir.path().join("never_written");

    let mut calls = 0u32;
    let err = wait_for_blocking::<SocketAddr>(
        &path,
        || {
            calls += 1;
            Ok(None)
        },
        INTERVAL,
        Timeout(Duration::ZERO),
    )
    .expect_err("the file never appears");
    let WaitForError::TimedOut { path: p, .. } = err else {
        panic!("expected TimedOut, got {err:?}");
    };
    assert_eq!(p, path);
    assert_eq!(calls, 1, "a zero timeout polls exactly once");
}

#[test]
fn zero_timeout_finds_existing_file() {
    let dir = camino_tempfile::tempdir().unwrap();
    let path = dir.path().join("port");
    let addr = socket_addr();
    port_file::write(&path, addr).unwrap();

    let discovered: SocketAddr = wait_for_blocking(
        &path,
        || Ok(None),
        INTERVAL,
        Timeout(Duration::ZERO),
    )
    .expect("a zero timeout acts as a single check, not an automatic failure");
    assert_eq!(discovered, addr);
}

#[test]
fn missing_parent_dir_fails_fast() {
    let dir = camino_tempfile::tempdir().unwrap();
    let path = dir.path().join("missing_dir").join("port");

    let err = wait_for_blocking::<SocketAddr>(
        &path,
        || Ok(None),
        INTERVAL,
        Timeout(Duration::from_secs(30)),
    )
    .expect_err("the parent directory can never contain the file");
    let WaitForError::MissingParentDirectory { path: p, parent, .. } = err
    else {
        panic!("expected MissingParentDirectory, got {err:?}");
    };
    assert_eq!(p, path);
    assert_eq!(parent, dir.path().join("missing_dir"));
}

#[test]
fn parent_is_regular_file_fails_fast() {
    let dir = camino_tempfile::tempdir().unwrap();
    let occupied = dir.path().join("occupied");
    std::fs::write(&occupied, "not a directory").unwrap();
    let path = occupied.join("port");

    let err = wait_for_blocking::<SocketAddr>(
        &path,
        || Ok(None),
        INTERVAL,
        Timeout(Duration::from_secs(30)),
    )
    .expect_err(
        "a regular file in the parent position can never contain the file",
    );
    match err {
        // On Unix-like platforms, reading the port file itself fails with
        // ENOTDIR, which is a permanent I/O error.
        WaitForError::Poll { error: PollError::Io { path: p, .. }, .. } => {
            assert_eq!(p, path);
        }
        // On Windows, that read reports "not found", and the parent directory
        // check catches the misconfiguration instead.
        WaitForError::ParentNotADirectory { path: p, parent, .. } => {
            assert_eq!(p, path);
            assert_eq!(parent, occupied);
        }
        other => panic!("expected a permanent error, got {other:?}"),
    }
}

// Integration test with a real child.
#[cfg(unix)]
#[test]
fn real_child_exit_is_detected() {
    use std::process::{Command, Stdio};

    let dir = camino_tempfile::tempdir().unwrap();
    let path = dir.path().join("never_written");
    let mut child = Command::new("false")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawned `false`");

    let err = wait_for_blocking::<SocketAddr>(
        &path,
        || child.try_wait(),
        INTERVAL,
        Timeout(Duration::from_secs(5)),
    )
    .expect_err("child exited without writing the port file");
    let WaitForError::Poll { error: PollError::ProcessExited { .. }, .. } = err
    else {
        panic!("unexpected error: {err:?}");
    };
}
