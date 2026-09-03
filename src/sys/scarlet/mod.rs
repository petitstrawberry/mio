cfg_os_poll! {
    mod selector;
    mod waker;

    pub(crate) use self::selector::*;

    cfg_net! {
        pub(crate) mod tcp;
        pub(crate) mod udp;
    }
}
