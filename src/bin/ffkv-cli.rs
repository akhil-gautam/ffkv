#[path = "../cli/mod.rs"]
mod cli;
use ffkv::client;

fn main() {
    #[cfg(unix)]
    let terminal = TerminalRestore::capture();
    // The line editor and pipe reader use detached OS threads, not Tokio's blocking
    // pool: a blocked stdin must never delay Ctrl-C or a broken connection exit.
    let code = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => {
            let code = runtime.block_on(cli::run());
            runtime.shutdown_background();
            code
        }
        Err(_) => {
            eprintln!("ffkv-cli: could not initialize asynchronous runtime");
            2
        }
    };
    #[cfg(unix)]
    drop(terminal);
    std::process::exit(code);
}

#[cfg(unix)]
struct TerminalRestore {
    state: Option<libc::termios>,
    tty: Option<std::fs::File>,
}

#[cfg(unix)]
impl TerminalRestore {
    fn capture() -> Self {
        use std::os::fd::AsRawFd;
        let mut state = std::mem::MaybeUninit::uninit();
        // SAFETY: tcgetattr initializes the termios only on success.
        if unsafe { libc::tcgetattr(libc::STDIN_FILENO, state.as_mut_ptr()) } == 0 {
            return Self {
                state: Some(unsafe { state.assume_init() }),
                tty: None,
            };
        }
        let tty = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/tty")
            .ok();
        let saved = tty.as_ref().and_then(|tty| {
            // SAFETY: the file descriptor remains live in this guard.
            (unsafe { libc::tcgetattr(tty.as_raw_fd(), state.as_mut_ptr()) } == 0)
                .then(|| unsafe { state.assume_init() })
        });
        Self { state: saved, tty }
    }
}

#[cfg(unix)]
impl Drop for TerminalRestore {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        if let Some(state) = &self.state {
            let fd = self
                .tty
                .as_ref()
                .map_or(libc::STDIN_FILENO, AsRawFd::as_raw_fd);
            // SAFETY: state belongs to this still-live terminal descriptor.
            unsafe {
                libc::tcsetattr(fd, libc::TCSANOW, state);
            }
        }
    }
}
