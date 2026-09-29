<!-- cargo-sync-rdme title [[ -->
# port-file
<!-- cargo-sync-rdme ]] -->
<!-- cargo-sync-rdme badge [[ -->
![License: MIT OR Apache-2.0](https://img.shields.io/crates/l/port-file.svg?)
[![crates.io](https://img.shields.io/crates/v/port-file.svg?logo=rust)](https://crates.io/crates/port-file)
[![docs.rs](https://img.shields.io/docsrs/port-file.svg?logo=docs.rs)](https://docs.rs/port-file)
[![Rust: ^1.86.0](https://img.shields.io/badge/rust-^1.86.0-93450a.svg?logo=rust)](https://doc.rust-lang.org/cargo/reference/manifest.html#the-rust-version-field)
<!-- cargo-sync-rdme ]] -->
<!-- cargo-sync-rdme rustdoc [[ -->
Facilities for reading and writing port files.

## Motivation

In many situations, services don’t have a fixed TCP or UDP port assigned to
them. In that case, these services typically bind to port 0. All operating
systems use port 0 as a signal to find an available *ephemeral port* for the
service to bind to.

But, if a supervisor process (such as a test) wants to find out the actual
port the service was bound to, there must be a protocol to report that
information from the service to the supervisor. A *port file* is one such
very simple protocol, using a text file to do this reporting.

A port file is most useful when the service lives in another *process* from
the supervisor. Sometimes, the service is spun up in-process—in that case,
it’s likely easier for the supervisor to query the service through some kind
of in-memory function call. (But a port file can still be used for external
synchronization.)

## Terminology

As of this writing, *port file* is not standard terminology, but it is the
term that the author believes communicates the intent best.

Some other terms that have been used for this concept:

* *Listening URL file*, which usually indicates an entire URL; this crate
  isn’t necessarily tied to a *port* ([`SocketAddr`]) specifically, and can be
  used just as well for such a file

* *Connection file*, as used by
  [Jupyter](https://github.com/jupyter/jupyter_client/blob/main/docs/kernels.rst#connection-files)

* *Rendezvous file*, which is a term that comes from distributed systems. At
  Oxide, we have an analogous concept called *rendezvous tables* (see
  [RFD 541](https://rfd.shared.oxide.computer/rfd/0541) if you have access).

## Usage

We’re going to assume a simple scenario where a parent process asks a child
process to bind a single service to TCP port 0.

In the child process, expose the bind address and the port file location
as command-line options:

````rust,no_run
use camino::Utf8PathBuf;
use clap::Parser;
use std::net::{SocketAddr, TcpListener};

/// A service that publishes its bound address to a port file.
#[derive(Parser)]
struct Args {
    /// The address to bind to (use port 0 for an ephemeral port).
    #[arg(long)]
    bind_address: SocketAddr,

    /// If set, the file to publish the bound address to.
    #[arg(long)]
    port_file: Option<Utf8PathBuf>,
}

let args = Args::parse();
let listener = TcpListener::bind(args.bind_address)?;

// Publish the address that was actually bound.
if let Some(path) = &args.port_file {
    port_file::write(path, listener.local_addr()?)?;
}

// Now, serve connections on `listener`.
````

In the parent process:

````rust,no_run
use port_file::{PollInterval, Timeout};
use std::{net::SocketAddr, process::Command, time::Duration};

// Pick a location for the port file. A fresh temporary directory keeps
// concurrent runs isolated from each other.
let dir = camino_tempfile::tempdir()?;
let path = dir.path().join("service.port");

// Spawn the child, telling it where to bind and where to write the
// port file.
let mut child = Command::new("my-service")
    .args(["--bind-address", "[::1]:0"])
    .args(["--port-file", path.as_str()])
    .spawn()?;

// Wait for the child to publish its address. Passing `child.try_wait()`
// means a child that dies before binding fails fast rather than hanging
// until the timeout.
let addr: SocketAddr = match port_file::wait_for_blocking(
    &path,
    || child.try_wait(),
    PollInterval(Duration::from_millis(25)),
    Timeout(Duration::from_secs(30)),
) {
    Ok(addr) => addr,
    Err(error) => {
        // The child is not stopped automatically on failure, so remember
        // to kill and reap it so it doesn't outlive the supervisor.
        //
        // In this example, we ignore cleanup errors, since the error
        // produced by wait_for_blocking is the one worth reporting.
        let _ = child.kill();
        let _ = child.wait();
        return Err(error.into());
    }
};

// The service is now reachable at `addr`.
println!("service is listening at {addr}");

// Now you can make requests to addr.
````

### Notes

The port file must not already exist. A write to an existing file will fail
with a [`WriteStage::Persist`] error. It is strongly recommended that a
fresh temporary directory is used to store the port file in.

If you’re using Tokio, use [`wait_for`] instead, which is the async
equivalent of [`wait_for_blocking`].

### Extensions

The port file logic is not tied to [`SocketAddr`]–rather, it is generic over
any type that implements both `FromStr` and `Display`. To communicate
information more complicated than a single IP-port pair, use your own type
that implements these traits with roundtrip serialization. (This can even be
something like a JSON blob.)

## Alternatives

### Roll it yourself

This crate is quite minimal and straightforward. You’re welcome to write
your own version of this. There are a few details this crate gets right that
you may want to copy. In particular:

* The write side uses [atomic writes][atomicwrites] and refuses to overwrite
  existing files.
* The read side has careful handling for permanent errors to avoid waiting
  out the entire timeout.

### In-process services

If your service can run within the same process as the supervisor, you may
simply be able to make an in-memory function call to the service to get the
bound port.

### Query the kernel

You can use tools like [`ss`] or [`lsof`] on Linux, or equivalents on other
operating systems, to see which sockets a child process has active. But this
is a heavyweight approach for a purely user-level concern. This approach has
also been known to [cause process
crashes](https://www.illumos.org/issues/18222) in some situations.

### Unix domain sockets

Instead of a TCP or UDP port, you can use a [Unix domain socket] (UDS), also
known as a local socket. If possible, this is the preferred way to bind to
ephemeral ports. But this has a few limitations:

* While UDS (despite the name) are available on Windows, as of Rust 1.96
  they’re [not part](https://github.com/rust-lang/rust/issues/150487) of the
  stable Rust standard library yet. Many ecosystem tools might only support
  UDS on Unix-like platforms.
* Many tools do not support UDS. For example, it is uncommon for HTTP servers
  and clients to support UDS as their transport (though there are crates like
  [hyperlocal] for this, and reqwest 0.13 has a [`unix_socket` method]).
* If UDS are only used in tests, this would introduce a divergence between
  test and production code. Bugs can often be specific to a particular
  transport layer.

[`SocketAddr`]: https://doc.rust-lang.org/stable/core/net/socket_addr/enum.SocketAddr.html "enum core::net::socket_addr::SocketAddr"
[`WriteStage::Persist`]: https://docs.rs/port-file/0.1.0/port_file/write/enum.WriteStage.html#variant.Persist "variant port_file::write::WriteStage::Persist"
[`wait_for`]: https://docs.rs/port-file/0.1.0/port_file/read/fn.wait_for.html "fn port_file::read::wait_for"
[`wait_for_blocking`]: https://docs.rs/port-file/0.1.0/port_file/read/fn.wait_for_blocking.html "fn port_file::read::wait_for_blocking"
[atomicwrites]: https://docs.rs/atomicwrites/0.4.4/atomicwrites/index.html "mod atomicwrites"
[`ss`]: https://man7.org/linux/man-pages/man8/ss.8.html
[`lsof`]: https://man7.org/linux/man-pages/man8/lsof.8.html
[Unix domain socket]: https://doc.rust-lang.org/stable/std/os/unix/net/listener/struct.UnixListener.html "struct std::os::unix::net::listener::UnixListener"
[hyperlocal]: https://crates.io/crates/hyperlocal
[`unix_socket` method]: https://docs.rs/reqwest/0.13/reqwest/struct.ClientBuilder.html#method.unix_socket
<!-- cargo-sync-rdme ]] -->

## License

This project is available under the terms of either the [Apache 2.0 license](LICENSE-APACHE) or the [MIT license](LICENSE-MIT).
