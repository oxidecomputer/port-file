#![cfg_attr(doc_cfg, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Facilities for reading and writing port files.
//!
//! # Motivation
//!
//! In many situations, services don't have a fixed TCP or UDP port assigned to
//! them. In that case, these services typically bind to port 0. All operating
//! systems use port 0 as a signal to find an available _ephemeral port_ for the
//! service to bind to.
//!
//! But, if a supervisor process (such as a test) wants to find out the actual
//! port the service was bound to, there must be a protocol to report that
//! information from the service to the supervisor. A _port file_ is one such
//! very simple protocol, using a text file to do this reporting.
//!
//! A port file is most useful when the service lives in another _process_ from
//! the supervisor. Sometimes, the service is spun up in-process—in that case,
//! it's likely easier for the supervisor to query the service through some kind
//! of in-memory function call. (But a port file can still be used for external
//! synchronization.)
//!
//! # Terminology
//!
//! As of this writing, _port file_ is not standard terminology, but it is the
//! term that the author believes communicates the intent best.
//!
//! Some other terms that have been used for this concept:
//!
//! * _Listening URL file_, which usually indicates an entire URL; this crate
//!   isn't necessarily tied to a _port_ ([`SocketAddr`]) specifically, and can be
//!   used just as well for such a file
//!
//! * _Connection file_, as used by
//!   [Jupyter](https://github.com/jupyter/jupyter_client/blob/main/docs/kernels.rst#connection-files)
//!
//! * _Rendezvous file_, which is a term that comes from distributed systems. At
//!   Oxide, we have an analogous concept called _rendezvous tables_ (see
//!   [RFD 541](https://rfd.shared.oxide.computer/rfd/0541) if you have access).
//!
//! # Usage
//!
//! We're going to assume a simple scenario where a parent process asks a child
//! process to bind a single service to TCP port 0.
//!
//! In the child process, expose the bind address and the port file location
//! as command-line options:
//!
//! ```no_run
//! use camino::Utf8PathBuf;
//! use clap::Parser;
//! use std::net::{SocketAddr, TcpListener};
//!
//! /// A service that publishes its bound address to a port file.
//! #[derive(Parser)]
//! struct Args {
//!     /// The address to bind to (use port 0 for an ephemeral port).
//!     #[arg(long)]
//!     bind_address: SocketAddr,
//!
//!     /// If set, the file to publish the bound address to.
//!     #[arg(long)]
//!     port_file: Option<Utf8PathBuf>,
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let args = Args::parse();
//! let listener = TcpListener::bind(args.bind_address)?;
//!
//! // Publish the address that was actually bound.
//! if let Some(path) = &args.port_file {
//!     port_file::write(path, listener.local_addr()?)?;
//! }
//!
//! // Now, serve connections on `listener`.
//! # Ok(())
//! # }
//! ```
//!
//! In the parent process:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use port_file::{PollInterval, Timeout};
//! use std::{net::SocketAddr, process::Command, time::Duration};
//!
//! // Pick a location for the port file. A fresh temporary directory keeps
//! // concurrent runs isolated from each other.
//! let dir = camino_tempfile::tempdir()?;
//! let path = dir.path().join("service.port");
//!
//! // Spawn the child, telling it where to bind and where to write the
//! // port file.
//! let mut child = Command::new("my-service")
//!     .args(["--bind-address", "[::1]:0"])
//!     .args(["--port-file", path.as_str()])
//!     .spawn()?;
//!
//! // Wait for the child to publish its address. Passing `child.try_wait()`
//! // means a child that dies before binding fails fast rather than hanging
//! // until the timeout.
//! let addr: SocketAddr = match port_file::wait_for_blocking(
//!     &path,
//!     || child.try_wait(),
//!     PollInterval(Duration::from_millis(25)),
//!     Timeout(Duration::from_secs(30)),
//! ) {
//!     Ok(addr) => addr,
//!     Err(error) => {
//!         // The child is not stopped automatically on failure, so remember
//!         // to kill and reap it so it doesn't outlive the supervisor.
//!         //
//!         // In this example, we ignore cleanup errors, since the error
//!         // produced by wait_for_blocking is the one worth reporting.
//!         let _ = child.kill();
//!         let _ = child.wait();
//!         return Err(error.into());
//!     }
//! };
//!
//! // The service is now reachable at `addr`.
//! println!("service is listening at {addr}");
//!
//! // Now you can make requests to addr.
//! # Ok(())
//! # }
//! ```
//!
//! ## Notes
//!
//! The port file must not already exist. A write to an existing file will fail
//! with a [`WriteStage::Persist`] error. It is strongly recommended that a
//! fresh temporary directory is used to store the port file in.
//!
//! If you're using Tokio, use [`wait_for`] instead, which is the async
//! equivalent of [`wait_for_blocking`].
//!
//! ## Extensions
//!
//! The port file logic is not tied to [`SocketAddr`]–rather, it is generic over
//! any type that implements both `FromStr` and `Display`. To communicate
//! information more complicated than a single IP-port pair, use your own type
//! that implements these traits with roundtrip serialization. (This can even be
//! something like a JSON blob.)
//!
//! # Alternatives
//!
//! ## Roll it yourself
//!
//! This crate is quite minimal and straightforward. You're welcome to write
//! your own version of this. There are a few details this crate gets right that
//! you may want to copy. In particular:
//!
//! * The write side uses [atomic writes](atomicwrites) and refuses to overwrite
//!   existing files.
//! * The read side has careful handling for permanent errors to avoid waiting
//!   out the entire timeout.
//!
//! ## In-process services
//!
//! If your service can run within the same process as the supervisor, you may
//! simply be able to make an in-memory function call to the service to get the
//! bound port.
//!
//! ## Unix domain sockets
//!
//! Instead of a TCP or UDP port, you can use a [Unix domain socket] (UDS), also
//! known as a local socket. If possible, this is the preferred way to bind to
//! ephemeral ports. But this has a few limitations:
//!
//! * While UDS (despite the name) are available on Windows, as of Rust 1.96
//!   they're [not part](https://github.com/rust-lang/rust/issues/150487) of the
//!   stable Rust standard library yet. Many ecosystem tools might only support
//!   UDS on Unix-like platforms.
//! * Many tools do not support UDS. For example, it is uncommon for HTTP servers
//!   and clients to support UDS as their transport (though there are crates like
//!   [hyperlocal] for this, and reqwest 0.13 has a [`unix_socket` method]).
//! * If UDS are only used in tests, this would introduce a divergence between
//!   test and production code. Bugs can often be specific to a particular
//!   transport layer.
//!
//! [`SocketAddr`]: std::net::SocketAddr
//! [Unix domain socket]: std::os::unix::net::UnixListener
//! [hyperlocal]: https://crates.io/crates/hyperlocal
//! [`unix_socket` method]: https://docs.rs/reqwest/0.13/reqwest/struct.ClientBuilder.html#method.unix_socket

mod read;
mod write;

#[cfg(feature = "tokio")]
pub use read::wait_for;
pub use read::{
    PollError, PollInterval, Readiness, Timeout, WaitForError, poll_once,
    wait_for_blocking,
};
pub use write::{WriteError, WriteStage, write};
