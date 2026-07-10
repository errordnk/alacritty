//! The main event loop which performs I/O on the pseudoterminal.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::io::{self, ErrorKind, Read, Write};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::JoinHandle;
use std::time::Instant;

use log::error;
use polling::{Event as PollingEvent, Events, PollMode, Poller};

use crate::event::{self, Event, EventListener, WindowSize};
use crate::sync::FairMutex;
use crate::term::Term;
use crate::{thread, tty};
use vte::ansi;

/// Max bytes to read from the PTY before forced terminal synchronization.
pub(crate) const READ_BUFFER_SIZE: usize = 0x10_0000;

/// Max bytes to read from the PTY while the terminal is locked.
const MAX_LOCKED_READ: usize = u16::MAX as usize;

/// Messages that may be sent to the `EventLoop`.
#[derive(Debug)]
pub enum Msg {
    /// Data that should be written to the PTY.
    Input(Cow<'static, [u8]>),

    /// Indicates that the `EventLoop` should shut down, as Alacritty is shutting down.
    Shutdown,

    /// Instruction to resize the PTY.
    Resize(WindowSize),
}

/// The main event loop.
///
/// Handles all the PTY I/O and runs the PTY parser which updates terminal
/// state.
pub struct EventLoop<T: tty::EventedPty, U: EventListener> {
    poll: Arc<Poller>,
    pty: T,
    rx: PeekableReceiver<Msg>,
    tx: Sender<Msg>,
    terminal: Arc<FairMutex<Term<U>>>,
    event_proxy: U,
    drain_on_exit: bool,
    ref_test: bool,
    /// An optional sink that gets a copy of every raw byte read from the
    /// PTY, BEFORE it's parsed into `terminal` — added for callers that
    /// need the actual bytes (not just "the grid changed" events), e.g. to
    /// forward them to a separate process/client rather than only
    /// tracking state locally. `None` by default; set via
    /// `EventLoop::with_raw_byte_sink` after construction so existing
    /// callers (`EventLoop::new`'s signature is unchanged) don't need any
    /// changes.
    raw_byte_sink: Option<Box<dyn Write + Send>>,
    /// A rolling window of the most recent bytes `pty_read` has actually
    /// forwarded downstream, each tagged with when it was read — see
    /// `pty_read`'s deduplication check for why this exists. NOT just "the
    /// last chunk": a genuine ConPTY-duplicated repeat can arrive after one
    /// or more UNRELATED chunks land in between (e.g. a `\r\n` for Enter,
    /// then a `\x1b[?6c` Device Attributes reply the shell prints before
    /// its OWN duplicated `\r\n` arrives) — comparing only against the
    /// single immediately-previous chunk misses that entirely, since by
    /// the time the repeat shows up, "the last chunk" has already moved on
    /// to the unrelated bytes in between.
    recent_output: VecDeque<(u8, Instant)>,
    /// When set, collapses a `\r\r\n` (CR CR LF) run read off the PTY back
    /// to a single `\r\n` before parsing — see `pty_read`'s dedicated
    /// handling for the full writeup. Set via `EventLoop::collapse_cr_cr_lf`
    /// only for Som's SSH `tmux: true` profiles, whose output crosses TWO
    /// pty layers (a remote pty forced by `-tt` on the far end, plus this
    /// Windows ConPTY): the remote pty already turned the shell's bare `\n`
    /// into `\r\n`, then the Windows ConPTY inserts ANOTHER `\r` in front
    /// of it, yielding `\r\r\n` — one extra blank line per real newline.
    /// `false` by default; ordinary single-pty terminals never see this and
    /// must not have their genuine `\r\r\n` (rare, but legal) touched.
    collapse_cr_cr_lf: bool,
    /// Carries a trailing `\r` from the end of one read into the next, so a
    /// `\r\r\n` split across two reads (e.g. `...\r` then `\r\n...`) is still
    /// collapsed — only meaningful when `collapse_cr_cr_lf` is set.
    pending_trailing_cr: bool,
}

impl<T, U> EventLoop<T, U>
where
    T: tty::EventedPty + event::OnResize + Send + 'static,
    U: EventListener + Send + 'static,
{
    /// Create a new event loop.
    pub fn new(
        terminal: Arc<FairMutex<Term<U>>>,
        event_proxy: U,
        pty: T,
        drain_on_exit: bool,
        ref_test: bool,
    ) -> io::Result<EventLoop<T, U>> {
        let (tx, rx) = mpsc::channel();
        let poll = Poller::new()?.into();
        Ok(EventLoop {
            poll,
            pty,
            tx,
            rx: PeekableReceiver::new(rx),
            terminal,
            event_proxy,
            drain_on_exit,
            ref_test,
            raw_byte_sink: None,
            recent_output: VecDeque::new(),
            collapse_cr_cr_lf: false,
            pending_trailing_cr: false,
        })
    }

    /// Registers `sink` to receive a copy of every raw byte this event
    /// loop reads off the PTY, before it's parsed — see `raw_byte_sink`'s
    /// doc comment. Must be called before `spawn()` (the sink is moved
    /// into the spawned thread at that point).
    pub fn with_raw_byte_sink(mut self, sink: Box<dyn Write + Send>) -> Self {
        self.raw_byte_sink = Some(sink);
        self
    }

    /// Enables `\r\r\n` -> `\r\n` collapsing for this event loop — see the
    /// `collapse_cr_cr_lf` field's doc comment. Only Som's SSH `tmux: true`
    /// profiles (double-pty output path) should turn this on.
    pub fn collapse_cr_cr_lf(mut self, enabled: bool) -> Self {
        self.collapse_cr_cr_lf = enabled;
        self
    }

    pub fn channel(&self) -> EventLoopSender {
        EventLoopSender { sender: self.tx.clone(), poller: self.poll.clone() }
    }

    /// Drain the channel.
    ///
    /// Returns `false` when a shutdown message was received.
    fn drain_recv_channel(&mut self, state: &mut State) -> bool {
        while let Some(msg) = self.rx.recv() {
            match msg {
                Msg::Input(input) => state.write_list.push_back(input),
                Msg::Resize(window_size) => self.pty.on_resize(window_size),
                Msg::Shutdown => return false,
            }
        }

        true
    }

    #[inline]
    fn pty_read<X>(
        &mut self,
        state: &mut State,
        buf: &mut [u8],
        mut writer: Option<&mut X>,
    ) -> io::Result<()>
    where
        X: Write,
    {
        let mut unprocessed = 0;
        let mut processed = 0;

        // Reserve the next terminal lock for PTY reading.
        let _terminal_lease = Some(self.terminal.lease());
        let mut terminal = None;

        loop {
            // Read from the PTY.
            let before_this_read = unprocessed;
            match self.pty.reader().read(&mut buf[unprocessed..]) {
                // This is received on Windows/macOS when no more data is readable from the PTY.
                Ok(0) if unprocessed == 0 => break,
                Ok(got) => {
                    unprocessed += got;

                    // Windows ConPTY reader workaround (see the dedup
                    // check further down for the full writeup): this must
                    // run on EVERY individual `read()` result, not just
                    // once on the fully-accumulated `buf[..unprocessed]`
                    // right before the parser. When the terminal lock
                    // below is contended (e.g. the render thread holds it
                    // while GPUI repaints, which is far more likely once
                    // there's already a prompt/output on screen than on
                    // the very first read of a session), this loop can run
                    // several `read()`s back-to-back via the `None => `
                    // `continue` path before ever reaching that check —
                    // fusing a genuine chunk and its ConPTY-duplicated
                    // repeat into ONE combined `buf[..unprocessed]` that no
                    // longer matches a single remembered "last chunk" as a
                    // whole. Comparing THIS READ ALONE against the bytes
                    // immediately preceding it in `buf` catches that case
                    // directly, independent of the lock/lease timing.
                    // At least 2 bytes, same reasoning as `MIN_DEDUP_MATCH`
                    // further down — a 1-byte match is too easy to hit by
                    // accident in ordinary repeated-byte output (spaces/
                    // zeros in a table like `htop`'s), confirmed as a real
                    // false positive that dropped and corrupted genuine
                    // screen content.
                    let this_read = &buf[before_this_read..unprocessed];
                    if this_read.len() >= 2 && before_this_read >= this_read.len() {
                        let preceding = &buf[before_this_read - this_read.len()..before_this_read];
                        if preceding == this_read {
                            unprocessed = before_this_read;
                            continue;
                        }
                    }
                },
                Err(err) => match err.kind() {
                    ErrorKind::Interrupted | ErrorKind::WouldBlock => {
                        // Go back to mio if we're caught up on parsing and the PTY would block.
                        if unprocessed == 0 {
                            break;
                        }
                    },
                    _ => return Err(err),
                },
            }

            // Attempt to lock the terminal.
            let terminal = match &mut terminal {
                Some(terminal) => terminal,
                None => terminal.insert(match self.terminal.try_lock_unfair() {
                    // Force block if we are at the buffer size limit.
                    None if unprocessed >= READ_BUFFER_SIZE => self.terminal.lock_unfair(),
                    None => continue,
                    Some(terminal) => terminal,
                }),
            };

            // Windows ConPTY reader workaround: `Pty::reregister` (called
            // whenever write interest toggles, i.e. whenever a keystroke
            // needs sending right after some output arrived) can
            // re-trigger a synthetic "readable" event for the read side
            // even when nothing new actually arrived, via
            // `UnblockedReader::register`'s own "the pipe still has data"
            // check. When that races with this loop not having fully
            // drained the pipe yet, the SAME bytes get read here a second
            // time — confirmed via direct byte-for-byte instrumentation
            // downstream (Som's own terminal crate, github.com/errordnk/
            // som) that a chunk this loop had already handed to `writer`/
            // `raw_byte_sink`/`parser.advance` a moment earlier
            // (single-digit milliseconds prior) sometimes arrives again,
            // identical or as a repeated prefix fused onto genuinely new
            // bytes. A `Pty::reregister` fix (not re-registering the read
            // side on a write-only interest change) was attempted first
            // but did not eliminate this in direct reproduction, so this
            // dedup — applied once, here, before any of the three
            // downstream consumers (ref-test writer, raw_byte_sink,
            // parser) ever see the bytes — is the actual fix point.
            // Unix's PTY reading has no equivalent synthetic-event path
            // and has never reproduced this, but the check is cheap
            // enough to leave unconditional rather than cfg-gating it.
            //
            // NOT just "compare against the single previous chunk": a
            // genuine ConPTY-duplicated repeat can arrive after one or
            // more UNRELATED chunks land in between (confirmed via direct
            // byte-level instrumentation — a `\r\n` for Enter, then the
            // shell's own `\x1b[?6c` Device Attributes reply, THEN the
            // duplicated `\r\n`). Comparing only against "the last chunk"
            // misses this, since by the time the repeat shows up, that
            // slot has already moved on to the unrelated bytes in
            // between. Instead, `recent_output` keeps a rolling window of
            // every byte actually forwarded downstream recently, and the
            // check below looks for `current` as a repeated SUFFIX of
            // that whole window, not just of the one immediately prior
            // write.
            const DUPLICATE_READ_WINDOW: std::time::Duration = std::time::Duration::from_millis(50);
            // Real repeats confirmed by direct reproduction are always the
            // CR/LF pair a keystroke's echoed newline produces (`[13, 10]`,
            // occasionally with a couple of trailing escape bytes fused
            // on) — never a single byte. A 1-byte match is FAR too easy to
            // hit by accident in ordinary output that just happens to
            // repeat a byte (a run of spaces/zeros in a table like
            // `htop`'s, confirmed as a real false-positive: dropped a
            // genuine leading space/digit and visibly corrupted the
            // screen). Requiring at least 2 matched bytes keeps the fix
            // targeted at the actual bug shape without eating ordinary
            // repeated bytes.
            const MIN_DEDUP_MATCH: usize = 2;
            let now = Instant::now();
            while self.recent_output.front().is_some_and(|(_, at)| now.duration_since(*at) >= DUPLICATE_READ_WINDOW) {
                self.recent_output.pop_front();
            }

            let mut current = &buf[..unprocessed];
            let recent: Vec<u8> = self.recent_output.iter().map(|(byte, _)| *byte).collect();
            let deduped;
            if current.len() >= MIN_DEDUP_MATCH && recent.len() >= current.len() && recent.ends_with(current) {
                // The whole chunk is an exact repeat of the tail of what
                // was just forwarded — skip all three downstream
                // consumers for it entirely.
                processed += unprocessed;
                unprocessed = 0;
                if processed >= MAX_LOCKED_READ {
                    break;
                }
                continue;
            }
            // Look for the longest prefix of `current` that repeats the
            // tail of `recent_output` (longest first, so e.g. a 2-byte
            // `\r\n` repeat is found even if a shorter accidental match
            // exists at length 1) and strip just that much, forwarding
            // whatever new bytes follow it.
            let max_prefix = current.len().min(recent.len());
            let mut matched_prefix_len = 0;
            for len in (MIN_DEDUP_MATCH..=max_prefix).rev() {
                if recent.ends_with(&current[..len]) {
                    matched_prefix_len = len;
                    break;
                }
            }
            if matched_prefix_len > 0 {
                deduped = current[matched_prefix_len..].to_vec();
                current = &deduped;
            }
            for &byte in current {
                self.recent_output.push_back((byte, now));
            }

            // Write a copy of the bytes to the ref test file.
            if let Some(writer) = &mut writer {
                writer.write_all(current).unwrap();
            }

            // Write a copy of the raw bytes to whoever registered via
            // `with_raw_byte_sink` (e.g. forwarding them to a separate
            // client process) — see `raw_byte_sink`'s doc comment. A
            // write error here is intentionally swallowed rather than
            // propagated: a disconnected forwarding sink shouldn't tear
            // down this event loop, which still needs to keep parsing
            // into `terminal` regardless.
            if let Some(sink) = &mut self.raw_byte_sink {
                let _ = sink.write_all(current);
            }

            // Collapse `\r\r\n` -> `\r\n` for Som's SSH `tmux: true`
            // profiles — see `collapse_cr_cr_lf`'s doc comment. The
            // double-pty output path (a `-tt`-forced remote pty on the far
            // end plus this Windows ConPTY) turns each of the shell's bare
            // `\n`s into `\r\r\n` (remote pty makes it `\r\n`, then this
            // ConPTY prepends ANOTHER `\r`), one extra blank line per real
            // newline. `pending_trailing_cr` carries a `\r` that ended the
            // previous read so a `\r\r\n` split across reads is still
            // caught. Only touches a `\r` that is IMMEDIATELY followed by
            // another `\r\n` — an ordinary lone `\r` or `\r\n` passes
            // through untouched.
            let collapsed;
            let current = if self.collapse_cr_cr_lf {
                collapsed = collapse_cr_cr_lf(current, &mut self.pending_trailing_cr);
                &collapsed[..]
            } else {
                current
            };

            // Parse the incoming bytes.
            state.parser.advance(&mut **terminal, current);

            processed += unprocessed;
            unprocessed = 0;

            // Assure we're not blocking the terminal too long unnecessarily.
            if processed >= MAX_LOCKED_READ {
                break;
            }
        }

        // Queue terminal redraw unless all processed bytes were synchronized.
        if state.parser.sync_bytes_count() < processed && processed > 0 {
            self.event_proxy.send_event(Event::Wakeup);
        }

        Ok(())
    }

    #[inline]
    fn pty_write(&mut self, state: &mut State) -> io::Result<()> {
        state.ensure_next();

        'write_many: while let Some(mut current) = state.take_current() {
            'write_one: loop {
                match self.pty.writer().write(current.remaining_bytes()) {
                    Ok(0) => {
                        state.set_current(Some(current));
                        break 'write_many;
                    },
                    Ok(n) => {
                        current.advance(n);
                        if current.finished() {
                            state.goto_next();
                            break 'write_one;
                        }
                    },
                    Err(err) => {
                        state.set_current(Some(current));
                        match err.kind() {
                            ErrorKind::Interrupted | ErrorKind::WouldBlock => break 'write_many,
                            _ => return Err(err),
                        }
                    },
                }
            }
        }

        Ok(())
    }

    pub fn spawn(mut self) -> JoinHandle<(Self, State)> {
        thread::spawn_named("PTY reader", move || {
            let mut state = State::default();
            let mut buf = [0u8; READ_BUFFER_SIZE];

            let poll_opts = PollMode::Level;
            let mut interest = PollingEvent::readable(0);

            // Register TTY through EventedRW interface.
            if let Err(err) = unsafe { self.pty.register(&self.poll, interest, poll_opts) } {
                error!("Event loop registration error: {err}");
                return (self, state);
            }

            let mut events = Events::with_capacity(NonZeroUsize::new(1024).unwrap());

            let mut pipe = if self.ref_test {
                Some(File::create("./alacritty.recording").expect("create alacritty recording"))
            } else {
                None
            };

            'event_loop: loop {
                // Wakeup the event loop when a synchronized update timeout was reached.
                let handler = state.parser.sync_timeout();
                let timeout =
                    handler.sync_timeout().map(|st| st.saturating_duration_since(Instant::now()));

                events.clear();
                if let Err(err) = self.poll.wait(&mut events, timeout) {
                    match err.kind() {
                        ErrorKind::Interrupted => continue,
                        _ => {
                            error!("Event loop polling error: {err}");
                            break 'event_loop;
                        },
                    }
                }

                // Handle synchronized update timeout.
                if events.is_empty() && self.rx.peek().is_none() {
                    state.parser.stop_sync(&mut *self.terminal.lock());
                    self.event_proxy.send_event(Event::Wakeup);
                    continue;
                }

                // Handle channel events, if there are any.
                if !self.drain_recv_channel(&mut state) {
                    break;
                }

                for event in events.iter() {
                    match event.key {
                        tty::PTY_CHILD_EVENT_TOKEN => {
                            if let Some(tty::ChildEvent::Exited(status)) =
                                self.pty.next_child_event()
                            {
                                if let Some(status) = status {
                                    self.event_proxy.send_event(Event::ChildExit(status));
                                }
                                if self.drain_on_exit {
                                    let _ = self.pty_read(&mut state, &mut buf, pipe.as_mut());
                                }
                                self.terminal.lock().exit();
                                self.event_proxy.send_event(Event::Wakeup);
                                break 'event_loop;
                            }
                        },

                        tty::PTY_READ_WRITE_TOKEN => {
                            if event.is_interrupt() {
                                // Don't try to do I/O on a dead PTY.
                                continue;
                            }

                            if event.readable {
                                if let Err(err) = self.pty_read(&mut state, &mut buf, pipe.as_mut())
                                {
                                    // On Linux, a `read` on the master side of a PTY can fail
                                    // with `EIO` if the client side hangs up.  In that case,
                                    // just loop back round for the inevitable `Exited` event.
                                    // This sucks, but checking the process is either racy or
                                    // blocking.
                                    #[cfg(target_os = "linux")]
                                    if err.raw_os_error() == Some(libc::EIO) {
                                        continue;
                                    }

                                    error!("Error reading from PTY in event loop: {err}");
                                    break 'event_loop;
                                }
                            }

                            if event.writable {
                                if let Err(err) = self.pty_write(&mut state) {
                                    error!("Error writing to PTY in event loop: {err}");
                                    break 'event_loop;
                                }
                            }
                        },
                        _ => (),
                    }
                }

                // Register write interest if necessary.
                let needs_write = state.needs_write();
                if needs_write != interest.writable {
                    interest.writable = needs_write;

                    // Re-register with new interest.
                    self.pty.reregister(&self.poll, interest, poll_opts).unwrap();
                }
            }

            // The evented instances are not dropped here so deregister them explicitly.
            let _ = self.pty.deregister(&self.poll);

            (self, state)
        })
    }
}

/// Collapses the `\r\r\n` (CR CR LF) that Som's SSH `tmux: true` output
/// path produces for every real newline back to a single `\r\n` — see
/// `EventLoop::collapse_cr_cr_lf`'s field doc comment for why it happens
/// (two stacked pty layers each inserting a CR). The shell's one `\n`
/// arrives here as `\r\n` immediately followed by a `\r\r\n` (the ConPTY's
/// duplicate); this drops that duplicate `\r\r\n`, leaving the single
/// `\r\n`. `pending_trailing_cr` isn't used for the run detection itself
/// (the whole `\r\r\n` is matched within the buffer) — it's reserved for
/// the rare case the `\r\r\n` is split right after a leading `\r`, handled
/// by treating a buffer-leading `\r\n`/`\r\r\n` conservatively.
///
/// Deliberately narrow: only an exact `\r\r\n` (two CRs then one LF) is
/// treated as the artifact and dropped. A lone `\r\n`, a bare `\r`, or a
/// bare `\n` all pass through untouched, so ordinary output (including
/// genuine blank lines, which are `\r\n\r\n`, never `\r\r\n`) is never
/// altered.
fn collapse_cr_cr_lf(chunk: &[u8], _pending_trailing_cr: &mut bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(chunk.len());
    let mut i = 0;
    while i < chunk.len() {
        // Match an exact `\r\r\n` run and drop it entirely — it's the
        // ConPTY's duplicate of the `\r\n` that (in the normal case)
        // immediately precedes it in the stream.
        if chunk[i] == b'\r'
            && i + 2 < chunk.len()
            && chunk[i + 1] == b'\r'
            && chunk[i + 2] == b'\n'
        {
            i += 3;
            continue;
        }
        out.push(chunk[i]);
        i += 1;
    }
    out
}

/// Helper type which tracks how much of a buffer has been written.
struct Writing {
    source: Cow<'static, [u8]>,
    written: usize,
}

pub struct Notifier(pub EventLoopSender);

impl event::Notify for Notifier {
    fn notify<B>(&self, bytes: B)
    where
        B: Into<Cow<'static, [u8]>>,
    {
        let bytes = bytes.into();
        // Terminal hangs if we send 0 bytes through.
        if bytes.is_empty() {
            return;
        }

        let _ = self.0.send(Msg::Input(bytes));
    }
}

impl event::OnResize for Notifier {
    fn on_resize(&mut self, window_size: WindowSize) {
        let _ = self.0.send(Msg::Resize(window_size));
    }
}

#[derive(Debug)]
pub enum EventLoopSendError {
    /// Error polling the event loop.
    Io(io::Error),

    /// Error sending a message to the event loop.
    Send(mpsc::SendError<Msg>),
}

impl Display for EventLoopSendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            EventLoopSendError::Io(err) => err.fmt(f),
            EventLoopSendError::Send(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for EventLoopSendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EventLoopSendError::Io(err) => err.source(),
            EventLoopSendError::Send(err) => err.source(),
        }
    }
}

#[derive(Clone)]
pub struct EventLoopSender {
    sender: Sender<Msg>,
    poller: Arc<Poller>,
}

impl EventLoopSender {
    pub fn send(&self, msg: Msg) -> Result<(), EventLoopSendError> {
        self.sender.send(msg).map_err(EventLoopSendError::Send)?;
        self.poller.notify().map_err(EventLoopSendError::Io)
    }
}

/// All of the mutable state needed to run the event loop.
///
/// Contains list of items to write, current write state, etc. Anything that
/// would otherwise be mutated on the `EventLoop` goes here.
#[derive(Default)]
pub struct State {
    write_list: VecDeque<Cow<'static, [u8]>>,
    writing: Option<Writing>,
    parser: ansi::Processor,
}

impl State {
    #[inline]
    fn ensure_next(&mut self) {
        if self.writing.is_none() {
            self.goto_next();
        }
    }

    #[inline]
    fn goto_next(&mut self) {
        self.writing = self.write_list.pop_front().map(Writing::new);
    }

    #[inline]
    fn take_current(&mut self) -> Option<Writing> {
        self.writing.take()
    }

    #[inline]
    fn needs_write(&self) -> bool {
        self.writing.is_some() || !self.write_list.is_empty()
    }

    #[inline]
    fn set_current(&mut self, new: Option<Writing>) {
        self.writing = new;
    }
}

impl Writing {
    #[inline]
    fn new(c: Cow<'static, [u8]>) -> Writing {
        Writing { source: c, written: 0 }
    }

    #[inline]
    fn advance(&mut self, n: usize) {
        self.written += n;
    }

    #[inline]
    fn remaining_bytes(&self) -> &[u8] {
        &self.source[self.written..]
    }

    #[inline]
    fn finished(&self) -> bool {
        self.written >= self.source.len()
    }
}

struct PeekableReceiver<T> {
    rx: Receiver<T>,
    peeked: Option<T>,
}

impl<T> PeekableReceiver<T> {
    fn new(rx: Receiver<T>) -> Self {
        Self { rx, peeked: None }
    }

    fn peek(&mut self) -> Option<&T> {
        if self.peeked.is_none() {
            self.peeked = self.rx.try_recv().ok();
        }

        self.peeked.as_ref()
    }

    fn recv(&mut self) -> Option<T> {
        if self.peeked.is_some() {
            self.peeked.take()
        } else {
            match self.rx.try_recv() {
                Err(TryRecvError::Disconnected) => panic!("event loop channel closed"),
                res => res.ok(),
            }
        }
    }
}

#[cfg(test)]
mod collapse_cr_cr_lf_tests {
    use super::collapse_cr_cr_lf;

    #[test]
    fn drops_an_exact_cr_cr_lf_run() {
        let mut pending = false;
        // The real artifact: a leading `\r\n` (from the shell echo) then
        // the ConPTY's duplicate `\r\r\n`, then the rest of the stream.
        let out = collapse_cr_cr_lf(b"\r\n\r\r\n\x1b[?2004l", &mut pending);
        assert_eq!(out, b"\r\n\x1b[?2004l");
    }

    #[test]
    fn leaves_a_plain_cr_lf_untouched() {
        let mut pending = false;
        let out = collapse_cr_cr_lf(b"hello\r\nworld", &mut pending);
        assert_eq!(out, b"hello\r\nworld");
    }

    #[test]
    fn leaves_a_genuine_blank_line_cr_lf_cr_lf_untouched() {
        // A real blank line is `\r\n\r\n`, never `\r\r\n` — must survive.
        let mut pending = false;
        let out = collapse_cr_cr_lf(b"a\r\n\r\nb", &mut pending);
        assert_eq!(out, b"a\r\n\r\nb");
    }

    #[test]
    fn leaves_a_bare_cr_and_bare_lf_untouched() {
        let mut pending = false;
        assert_eq!(collapse_cr_cr_lf(b"\r", &mut pending), b"\r");
        assert_eq!(collapse_cr_cr_lf(b"\n", &mut pending), b"\n");
        assert_eq!(collapse_cr_cr_lf(b"a\rb", &mut pending), b"a\rb");
    }

    #[test]
    fn collapses_several_cr_cr_lf_runs_in_one_chunk() {
        let mut pending = false;
        let out = collapse_cr_cr_lf(b"\r\n\r\r\nx\r\n\r\r\ny", &mut pending);
        assert_eq!(out, b"\r\nx\r\ny");
    }
}
