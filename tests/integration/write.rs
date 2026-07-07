use port_file::WriteStage;
use std::{io, net::SocketAddr};

#[test]
fn round_trips_socket_addr() {
    let dir = camino_tempfile::tempdir().unwrap();
    let path = dir.path().join("port");
    let addr: SocketAddr = "[::1]:41065".parse().unwrap();

    port_file::write(&path, addr).unwrap();

    let contents = std::fs::read_to_string(&path).unwrap();
    assert_eq!(contents, "[::1]:41065\n");
    assert_eq!(contents.trim().parse::<SocketAddr>().unwrap(), addr);
}

#[test]
fn refuses_to_overwrite_existing_file() {
    let dir = camino_tempfile::tempdir().unwrap();
    let path = dir.path().join("port");
    let first: SocketAddr = "[::1]:1".parse().unwrap();
    let second: SocketAddr = "[::1]:2".parse().unwrap();

    port_file::write(&path, first).unwrap();
    let err = port_file::write(&path, second).unwrap_err();
    assert_eq!(err.path(), path.as_path());
    assert_eq!(
        err.stage(),
        WriteStage::Persist,
        "an existing destination is a persist failure: {err}"
    );
    let source = std::error::Error::source(&err)
        .expect("the underlying io::Error is preserved as the source");
    let io_err = source
        .downcast_ref::<io::Error>()
        .expect("source is the underlying io::Error");
    assert_eq!(io_err.kind(), io::ErrorKind::AlreadyExists);

    let contents = std::fs::read_to_string(&path).unwrap();
    assert_eq!(
        contents.trim().parse::<SocketAddr>().unwrap(),
        first,
        "a failed write leaves the existing file untouched"
    );
}

#[test]
fn error_names_the_path() {
    let dir = camino_tempfile::tempdir().unwrap();
    let path = dir.path().join("missing_dir").join("port");
    let addr: SocketAddr = "[::1]:1".parse().unwrap();

    let err = port_file::write(&path, addr).unwrap_err();
    assert_eq!(err.path(), path.as_path());
    assert_eq!(
        err.stage(),
        WriteStage::Persist,
        "creating the temporary file in a missing directory is a persist \
         failure: {err}"
    );
    assert!(
        err.to_string().contains("temporary file for port file"),
        "unexpected error: {err}"
    );
    let source = std::error::Error::source(&err)
        .expect("the underlying io::Error is preserved as the source");
    assert!(
        source.downcast_ref::<io::Error>().is_some(),
        "source is the underlying io::Error: {source}"
    );
}
