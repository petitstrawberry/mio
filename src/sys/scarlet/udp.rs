use std::io;
use std::net::{self, SocketAddr};

use socket2::{Domain, Protocol, SockRef, Socket, Type};

pub(crate) fn bind(address: SocketAddr) -> io::Result<net::UdpSocket> {
    let socket = Socket::new(
        Domain::for_address(address),
        Type::DGRAM,
        Some(Protocol::UDP),
    )?;
    socket.set_nonblocking(true)?;
    socket.bind(&address.into())?;
    Ok(socket.into())
}

pub(crate) fn only_v6(socket: &net::UdpSocket) -> io::Result<bool> {
    match SockRef::from(socket).only_v6() {
        Err(error) if error.kind() == io::ErrorKind::Unsupported => Ok(false),
        result => result,
    }
}
