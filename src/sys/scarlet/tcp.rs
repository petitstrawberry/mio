use std::io;
use std::net::{self, SocketAddr};
use std::os::fd::{FromRawFd, IntoRawFd};

use socket2::{Domain, Protocol, SockRef, Socket, Type};

pub(crate) fn new_for_addr(address: SocketAddr) -> io::Result<i32> {
    let domain = Domain::for_address(address);
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_nonblocking(true)?;
    Ok(socket.into_raw_fd())
}

pub(crate) fn bind(socket: &net::TcpListener, address: SocketAddr) -> io::Result<()> {
    SockRef::from(socket).bind(&address.into())
}

pub(crate) fn connect(socket: &net::TcpStream, address: SocketAddr) -> io::Result<()> {
    match SockRef::from(socket).connect(&address.into()) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(()),
        Err(error) => Err(error),
    }
}

pub(crate) fn listen(socket: &net::TcpListener, backlog: i32) -> io::Result<()> {
    SockRef::from(socket).listen(backlog)
}

pub(crate) fn set_reuseaddr(_socket: &net::TcpListener, _reuseaddr: bool) -> io::Result<()> {
    // Scarlet currently has no SO_REUSEADDR equivalent. Its native listener
    // lifecycle does not retain a POSIX-style socket option to configure.
    Ok(())
}

pub(crate) fn accept(listener: &net::TcpListener) -> io::Result<(net::TcpStream, SocketAddr)> {
    let (socket, address) = SockRef::from(listener).accept()?;
    socket.set_nonblocking(true)?;
    let address = address.as_socket().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Scarlet accept returned a non-IP address",
        )
    })?;
    // SAFETY: `socket.into_raw_fd()` transfers an accepted TCP stream handle.
    let stream = unsafe { net::TcpStream::from_raw_fd(socket.into_raw_fd()) };
    Ok((stream, address))
}
