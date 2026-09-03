use std::io;
use std::os::fd::RawFd;
use std::sync::Mutex;

use scarlet_os::handle::capability::StreamError;
use scarlet_os::ipc::pipe;
use scarlet_os::Handle;

/// Pipe-backed notification source used to interrupt a Scarlet poll call.
#[derive(Debug)]
pub(crate) struct NotifyWaker {
    reader: Handle,
    writer: Handle,
    woken: Mutex<bool>,
}

impl NotifyWaker {
    pub(crate) fn new_unregistered() -> io::Result<NotifyWaker> {
        let (reader, writer) = pipe().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "failed to create Scarlet wake pipe")
        })?;
        reader.set_nonblocking(true).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "failed to make Scarlet wake pipe nonblocking",
            )
        })?;
        writer.set_nonblocking(true).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "failed to make Scarlet wake pipe nonblocking",
            )
        })?;

        Ok(NotifyWaker {
            reader,
            writer,
            woken: Mutex::new(false),
        })
    }

    pub(crate) fn wake(&self) -> io::Result<()> {
        let mut woken = self.woken.lock().unwrap();
        if *woken {
            return Ok(());
        }

        let writer = self.writer.as_stream().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "invalid Scarlet wake pipe writer")
        })?;
        match writer.write(&[1]) {
            Ok(_) | Err(StreamError::WouldBlock) => {
                *woken = true;
                Ok(())
            }
            Err(error) => Err(stream_error(error)),
        }
    }

    pub(crate) fn fd(&self) -> RawFd {
        self.reader.as_raw()
    }

    pub(crate) fn woken(&self) -> bool {
        *self.woken.lock().unwrap()
    }

    pub(crate) fn ack_and_reset(&self) {
        let mut woken = self.woken.lock().unwrap();
        let Ok(reader) = self.reader.as_stream() else {
            *woken = false;
            return;
        };
        let mut buffer = [0; 64];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) | Err(StreamError::WouldBlock) => break,
                Ok(_) => {}
                Err(_) => break,
            }
        }
        *woken = false;
    }
}

fn stream_error(error: StreamError) -> io::Error {
    let kind = match error {
        StreamError::Interrupted => io::ErrorKind::Interrupted,
        StreamError::WouldBlock => io::ErrorKind::WouldBlock,
        StreamError::EndOfStream => io::ErrorKind::UnexpectedEof,
        StreamError::PermissionDenied => io::ErrorKind::PermissionDenied,
        StreamError::InvalidParameter => io::ErrorKind::InvalidInput,
        StreamError::Unsupported => io::ErrorKind::Unsupported,
        StreamError::InvalidHandle | StreamError::IoError | StreamError::SystemError(_) => {
            io::ErrorKind::Other
        }
    };
    io::Error::new(kind, "Scarlet wake pipe operation failed")
}
