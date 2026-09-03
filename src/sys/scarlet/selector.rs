use std::collections::HashMap;
use std::fmt;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use std::{io, mem};

use scarlet_os::poll::{
    poll as scarlet_poll, PollHandle, POLLERR, POLLHUP, POLLIN, POLLNVAL, POLLOUT, POLLPRI,
};

use super::waker::NotifyWaker;
use crate::{Interest, Token};

/// Unique id for use as `SelectorId`.
#[cfg(debug_assertions)]
static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

#[derive(Debug)]
pub struct Selector {
    state: Arc<SelectorState>,
}

impl Selector {
    pub fn new() -> io::Result<Selector> {
        Ok(Selector {
            state: Arc::new(SelectorState::new()?),
        })
    }

    pub fn try_clone(&self) -> io::Result<Selector> {
        Ok(Selector {
            state: Arc::clone(&self.state),
        })
    }

    pub fn select(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        self.state.select(events, timeout)
    }

    pub(crate) fn register_internal(
        &self,
        fd: RawFd,
        token: Token,
        interests: Interest,
    ) -> io::Result<Arc<RegistrationRecord>> {
        self.state.register_internal(fd, token, interests)
    }

    pub fn reregister(&self, fd: RawFd, token: Token, interests: Interest) -> io::Result<()> {
        self.state.reregister(fd, token, interests)
    }

    pub fn deregister(&self, fd: RawFd) -> io::Result<()> {
        self.state.deregister(fd)
    }

    pub fn wake(&self, token: Token) -> io::Result<()> {
        self.state.wake(token)
    }

    #[cfg(debug_assertions)]
    pub fn id(&self) -> usize {
        self.state.id
    }
}

#[derive(Debug)]
struct SelectorState {
    fds: Mutex<Fds>,
    pending_removal: Mutex<Vec<RawFd>>,
    pending_wake_token: Mutex<Option<Token>>,
    notify_waker: NotifyWaker,
    waiting_operations: AtomicUsize,
    operations_complete: Condvar,
    #[cfg(debug_assertions)]
    id: usize,
}

#[derive(Debug)]
struct Fds {
    poll_fds: Vec<PollFd>,
    fd_data: HashMap<RawFd, FdData>,
}

#[derive(Clone)]
struct PollFd {
    fd: RawFd,
    events: u16,
    revents: u16,
}

impl fmt::Debug for PollFd {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PollHandle")
            .field("handle", &self.fd)
            .field("events", &self.events)
            .field("revents", &self.revents)
            .finish()
    }
}

#[derive(Debug, Clone)]
struct FdData {
    poll_fds_index: usize,
    token: Token,
    shared_record: Arc<RegistrationRecord>,
}

impl SelectorState {
    fn new() -> io::Result<SelectorState> {
        let notify_waker = NotifyWaker::new_unregistered()?;
        let notify_fd = notify_waker.fd();

        Ok(SelectorState {
            fds: Mutex::new(Fds {
                poll_fds: vec![PollFd {
                    fd: notify_fd,
                    events: POLLIN,
                    revents: 0,
                }],
                fd_data: HashMap::new(),
            }),
            pending_removal: Mutex::new(Vec::new()),
            pending_wake_token: Mutex::new(None),
            notify_waker,
            waiting_operations: AtomicUsize::new(0),
            operations_complete: Condvar::new(),
            #[cfg(debug_assertions)]
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        })
    }

    fn select(&self, events: &mut Events, mut timeout: Option<Duration>) -> io::Result<()> {
        events.clear();
        let mut fds = self.fds.lock().unwrap();
        let mut closed_fds = Vec::new();

        loop {
            while self.waiting_operations.load(Ordering::SeqCst) != 0 {
                fds = self.operations_complete.wait(fds).unwrap();
            }

            if self.notify_waker.woken() {
                timeout = Some(Duration::ZERO);
            }

            trace!("Polling on {:?}", &fds);
            let event_count = poll(&mut fds.poll_fds, timeout)?;
            trace!("Poll finished: {:?}", &fds);
            if event_count == 0 {
                return Ok(());
            }

            let waker_events = fds.poll_fds[0].revents;
            let notified = waker_events != 0;
            let mut source_event_count = if notified {
                event_count.saturating_sub(1)
            } else {
                event_count
            };
            let pending_wake_token = self.pending_wake_token.lock().unwrap().take();

            if notified {
                self.notify_waker.ack_and_reset();
                if pending_wake_token.is_some() {
                    source_event_count += 1;
                }
            }

            let mut pending_removal_guard = self.pending_removal.lock().unwrap();
            let mut pending_removal = mem::take(&mut *pending_removal_guard);
            drop(pending_removal_guard);

            if source_event_count == 0 {
                continue;
            }

            events.reserve(source_event_count);
            if let Some(token) = pending_wake_token {
                events.push(Event {
                    token,
                    events: waker_events | POLLIN,
                });
            }

            let fds = &mut *fds;
            for data in fds.fd_data.values_mut() {
                let poll_fd = &mut fds.poll_fds[data.poll_fds_index];
                if pending_removal.contains(&poll_fd.fd) || poll_fd.revents == 0 {
                    continue;
                }

                events.push(Event {
                    token: data.token,
                    events: poll_fd.revents,
                });

                if poll_fd.revents & (POLLHUP | POLLERR | POLLNVAL) != 0 {
                    pending_removal.push(poll_fd.fd);
                    closed_fds.push(poll_fd.fd);
                }

                poll_fd.events &= !poll_fd.revents;
                if events.len() == source_event_count {
                    break;
                }
            }
            break;
        }

        drop(fds);
        let _ = self.deregister_all(&closed_fds);
        Ok(())
    }

    fn register_internal(
        &self,
        fd: RawFd,
        token: Token,
        interests: Interest,
    ) -> io::Result<Arc<RegistrationRecord>> {
        if fd < 0 || fd == self.notify_waker.fd() {
            return Err(io::ErrorKind::InvalidInput.into());
        }

        let mut pending_removal = self.pending_removal.lock().unwrap();
        if let Some(index) = pending_removal.iter().position(|pending| *pending == fd) {
            pending_removal.swap_remove(index);
        }
        drop(pending_removal);

        self.modify_fds(|fds| {
            if fds.fd_data.contains_key(&fd) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "I/O source is already registered with this Registry",
                ));
            }

            let index = fds.poll_fds.len();
            let record = Arc::new(RegistrationRecord::new());
            fds.fd_data.insert(
                fd,
                FdData {
                    poll_fds_index: index,
                    token,
                    shared_record: Arc::clone(&record),
                },
            );
            fds.poll_fds.push(PollFd {
                fd,
                events: interests_to_poll(interests),
                revents: 0,
            });
            Ok(record)
        })
    }

    fn reregister(&self, fd: RawFd, token: Token, interests: Interest) -> io::Result<()> {
        self.modify_fds(|fds| {
            let data = fds
                .fd_data
                .get_mut(&fd)
                .ok_or(io::ErrorKind::NotFound)?;
            data.token = token;
            let index = data.poll_fds_index;
            fds.poll_fds[index].events = interests_to_poll(interests);
            fds.poll_fds[index].revents = 0;
            Ok(())
        })
    }

    fn deregister(&self, fd: RawFd) -> io::Result<()> {
        self.deregister_all(&[fd])
            .map_err(|_| io::ErrorKind::NotFound.into())
    }

    fn modify_fds<T>(&self, operation: impl FnOnce(&mut Fds) -> T) -> T {
        self.waiting_operations.fetch_add(1, Ordering::SeqCst);
        let sent_notification = self.notify_waker.wake().is_ok();
        let mut fds = self.fds.lock().unwrap();
        if sent_notification {
            self.notify_waker.ack_and_reset();
        }

        let result = operation(&mut fds);
        if self.waiting_operations.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.operations_complete.notify_one();
        }
        result
    }

    fn deregister_all(&self, targets: &[RawFd]) -> Result<(), ()> {
        if targets.is_empty() {
            return Ok(());
        }

        self.pending_removal
            .lock()
            .unwrap()
            .extend_from_slice(targets);

        self.modify_fds(|fds| {
            let mut all_removed = true;
            for target in targets {
                match fds.fd_data.remove(target) {
                    Some(data) => {
                        data.shared_record.mark_unregistered();
                        fds.poll_fds.swap_remove(data.poll_fds_index);
                        if let Some(swapped) = fds.poll_fds.get(data.poll_fds_index) {
                            fds.fd_data
                                .get_mut(&swapped.fd)
                                .expect("swapped Scarlet poll handle must be registered")
                                .poll_fds_index = data.poll_fds_index;
                        }
                    }
                    None => all_removed = false,
                }
            }
            all_removed.then_some(()).ok_or(())
        })
    }

    fn wake(&self, token: Token) -> io::Result<()> {
        self.pending_wake_token.lock().unwrap().replace(token);
        self.notify_waker.wake()
    }
}

fn interests_to_poll(interests: Interest) -> u16 {
    let mut flags = 0;
    if interests.is_readable() {
        flags |= POLLIN;
    }
    if interests.is_writable() {
        flags |= POLLOUT;
    }
    if interests.is_priority() {
        flags |= POLLPRI;
    }
    flags
}

fn poll(fds: &mut [PollFd], timeout: Option<Duration>) -> io::Result<usize> {
    let timeout_ns = timeout
        .map(|duration| i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(-1);
    let mut handles: Vec<PollHandle> = fds
        .iter()
        .map(|fd| PollHandle {
            handle: fd.fd as u32,
            events: fd.events,
            revents: 0,
        })
        .collect();
    let ready = scarlet_poll(&mut handles, timeout_ns)
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "Scarlet poll failed"))?;
    for (fd, handle) in fds.iter_mut().zip(handles) {
        fd.revents = handle.revents;
    }
    Ok(ready)
}

#[derive(Debug, Clone)]
pub struct Event {
    token: Token,
    events: u16,
}

pub type Events = Vec<Event>;

pub mod event {
    use std::fmt;

    use scarlet_os::poll;
    use scarlet_os::poll::{POLLERR, POLLHUP, POLLIN, POLLNVAL, POLLOUT, POLLPRI};

    use super::Event;
    use crate::Token;

    pub fn token(event: &Event) -> Token {
        event.token
    }

    pub fn is_readable(event: &Event) -> bool {
        event.events & (POLLIN | POLLPRI) != 0
    }

    pub fn is_writable(event: &Event) -> bool {
        event.events & POLLOUT != 0
    }

    pub fn is_error(event: &Event) -> bool {
        event.events & (POLLERR | POLLNVAL) != 0
    }

    pub fn is_read_closed(event: &Event) -> bool {
        event.events & POLLHUP != 0
    }

    pub fn is_write_closed(event: &Event) -> bool {
        event.events & POLLHUP != 0
            || event.events & (POLLOUT | POLLERR) == (POLLOUT | POLLERR)
            || event.events == POLLERR
    }

    pub fn is_priority(event: &Event) -> bool {
        event.events & POLLPRI != 0
    }

    pub fn is_aio(_: &Event) -> bool {
        false
    }

    pub fn is_lio(_: &Event) -> bool {
        false
    }

    pub fn debug_details(formatter: &mut fmt::Formatter<'_>, event: &Event) -> fmt::Result {
        fn has_flag(events: &u16, flag: &u16) -> bool {
            events & flag != 0
        }
        debug_detail!(
            EventDetails(u16),
            has_flag,
            poll::POLLIN,
            poll::POLLPRI,
            poll::POLLOUT,
            poll::POLLERR,
            poll::POLLHUP,
            poll::POLLNVAL,
        );
        formatter
            .debug_struct("scarlet_poll_event")
            .field("token", &event.token)
            .field("events", &EventDetails(event.events))
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct Waker {
    selector: Selector,
    token: Token,
}

impl Waker {
    pub(crate) fn new(selector: &Selector, token: Token) -> io::Result<Waker> {
        Ok(Waker {
            selector: selector.try_clone()?,
            token,
        })
    }

    pub(crate) fn wake(&self) -> io::Result<()> {
        self.selector.wake(self.token)
    }
}

#[derive(Debug)]
pub(crate) struct RegistrationRecord {
    is_unregistered: std::sync::atomic::AtomicBool,
}

impl RegistrationRecord {
    fn new() -> RegistrationRecord {
        RegistrationRecord {
            is_unregistered: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn mark_unregistered(&self) {
        self.is_unregistered.store(true, Ordering::Relaxed);
    }

    fn is_registered(&self) -> bool {
        !self.is_unregistered.load(Ordering::Relaxed)
    }
}

pub(crate) struct IoSourceState {
    inner: Option<Box<InternalState>>,
}

struct InternalState {
    selector: Selector,
    token: Token,
    interests: Interest,
    fd: RawFd,
    shared_record: Arc<RegistrationRecord>,
}

impl IoSourceState {
    pub(crate) fn new() -> IoSourceState {
        IoSourceState { inner: None }
    }

    pub(crate) fn do_io<T, F, R>(&self, operation: F, io: &T) -> io::Result<R>
    where
        F: FnOnce(&T) -> io::Result<R>,
    {
        let result = operation(io);
        if result
            .as_ref()
            .is_err_and(|error| error.kind() == io::ErrorKind::WouldBlock)
        {
            if let Some(state) = &self.inner {
                state
                    .selector
                    .reregister(state.fd, state.token, state.interests)?;
            }
        }
        result
    }

    pub(crate) fn register(
        &mut self,
        registry: &crate::Registry,
        token: Token,
        interests: Interest,
        fd: RawFd,
    ) -> io::Result<()> {
        if self.inner.is_some() {
            return Err(io::ErrorKind::AlreadyExists.into());
        }

        let selector = registry.selector().try_clone()?;
        let shared_record = selector.register_internal(fd, token, interests)?;
        self.inner = Some(Box::new(InternalState {
            selector,
            token,
            interests,
            fd,
            shared_record,
        }));
        Ok(())
    }

    pub(crate) fn reregister(
        &mut self,
        registry: &crate::Registry,
        token: Token,
        interests: Interest,
        fd: RawFd,
    ) -> io::Result<()> {
        let state = self.inner.as_mut().ok_or(io::ErrorKind::NotFound)?;
        registry.selector().reregister(fd, token, interests)?;
        state.token = token;
        state.interests = interests;
        Ok(())
    }

    pub(crate) fn deregister(
        &mut self,
        registry: &crate::Registry,
        fd: RawFd,
    ) -> io::Result<()> {
        if let Some(state) = self.inner.take() {
            state.shared_record.mark_unregistered();
        }
        registry.selector().deregister(fd)
    }
}

impl Drop for InternalState {
    fn drop(&mut self) {
        if self.shared_record.is_registered() {
            let _ = self.selector.deregister(self.fd);
        }
    }
}
