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

/// Max size a chunk can be and still be considered for the ConPTY
/// duplicate-read workarounds below (both the per-`read()` immediate-repeat
/// check and the rolling-window check). The duplicated-read bug these exist
/// for only ever duplicates the handful of bytes a keystroke echoes back —
/// there's no reason to inspect anything bigger, and a real reason not to:
/// bulk output (an image transmission's base64, most of all) contains long
/// genuinely-repeating runs, which this style of matcher happily mistakes
/// for a duplicated read and deletes. Losing an arbitrary slice out of the
/// middle of an APC string truncates the image transmission: the terminal
/// then waits forever for a final chunk that already went by.
const MAX_DEDUP_CHUNK: usize = 256;

/// A filter over the byte stream read off the PTY, run after the ConPTY
/// duplicate-read workaround and the `\r\r\n` collapse and before the bytes
/// reach the parser.
///
/// For callers that carry their own messages inside the PTY stream (Som's
/// `somsrv` relay multiplexes its rich-content channel into the output of the
/// tab's remote pty): the filter takes those messages out and pushes the rest,
/// unchanged and in order, onto `out`. A filter may hold back a trailing part
/// of `input` that could be the start of one of its messages, and push it
/// with the next call.
pub trait StreamFilter: Send {
    fn filter(&mut self, input: &[u8], out: &mut Vec<u8>);
}

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
    /// See [`StreamFilter`]. `None` by default; set via
    /// `EventLoop::with_stream_filter`.
    stream_filter: Option<Box<dyn StreamFilter>>,
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
            collapse_cr_cr_lf: false,
            pending_trailing_cr: false,
            stream_filter: None,
        })
    }

    /// Runs `filter` over everything read off the PTY before it is parsed -
    /// see [`StreamFilter`]. Must be called before `spawn()`.
    pub fn with_stream_filter(mut self, filter: Box<dyn StreamFilter>) -> Self {
        self.stream_filter = Some(filter);
        self
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
                    //
                    // ALSO gated on `cfg(windows)` (this bug is ConPTY-
                    // specific, same reasoning as the rolling-window check's
                    // own `cfg(windows)` gate below) and on `MAX_DEDUP_CHUNK`
                    // (same reasoning as that check's own cap): a large
                    // single `read()` off the PTY is exactly the shape of a
                    // Kitty graphics APC chunk (`somcat`/`yazi` write up to
                    // 4096 base64 bytes at once), and base64-encoded/
                    // PNG-compressed image data routinely contains
                    // genuinely-repeating equal-length runs purely by
                    // chance — this check used to have neither guard, so it
                    // silently ate a same-length repeated run out of the
                    // middle of a real animation frame's payload roughly
                    // 1-in-N transmissions (confirmed via `somcat`'s own
                    // per-chunk logging: the sender's `send_chunked` loop
                    // completed and flushed every chunk, but the receiving
                    // `ImageStore` never saw enough bytes to finish decoding
                    // that frame — bytes went missing strictly between the
                    // PTY write and the parser, not in either endpoint).
                    let this_read = &buf[before_this_read..unprocessed];
                    #[cfg(windows)]
                    let this_read_dedup_eligible = this_read.len() <= MAX_DEDUP_CHUNK;
                    #[cfg(not(windows))]
                    let this_read_dedup_eligible = false;
                    if this_read_dedup_eligible
                        && this_read.len() >= 2
                        && before_this_read >= this_read.len()
                    {
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

            // A second, rolling-window dedup used to live here (comparing
            // each fully-accumulated `buf[..unprocessed]` against a
            // `recent_output` history of recently-forwarded bytes, gated
            // on `last_write_interest_toggle` recency). Removed
            // (2026-09-15): it matched on the bytes' SHAPE alone (does
            // `current` look like a repeat of something recently sent),
            // which is fundamentally unreliable — confirmed twice over via
            // live reproduction. First as a false positive that ate the
            // legitimate blank line between a MOTD's kernel banner and the
            // rest of `/etc/motd` (any ordinary blank-line-after-`\r\n` is
            // byte-for-byte indistinguishable from a real duplicate by
            // shape alone). Then, after narrowing the check to require an
            // exact whole-chunk match, it STILL falsely triggered during a
            // fast held-backspace burst against a remote shell's line
            // editor (which naturally repeats similar/identical short
            // erase sequences across consecutive keystrokes) — silently
            // dropping a genuine new chunk and leaving permanent leftover
            // characters on a wrapped line's second visual row that no
            // amount of further backspacing could ever clear. The
            // in-buffer check above (comparing THIS READ against the bytes
            // immediately preceding it in the SAME `buf`, within the SAME
            // `pty_read` call) is a strictly narrower, structurally-
            // grounded condition — it can only match the actual mechanism
            // of the ConPTY duplicate-read bug (the same already-forwarded
            // bytes getting handed back by a second `read()` in this same
            // drain loop), not merely similar-looking-but-genuinely-new
            // output arriving later. That check alone is kept; no
            // replacement for this wider, history-based one was added.
            let current = &buf[..unprocessed];

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

            // Take out whatever the caller carries inside the stream.
            let filtered;
            let current = match &mut self.stream_filter {
                Some(filter) => {
                    let mut out = Vec::with_capacity(current.len());
                    filter.filter(current, &mut out);
                    filtered = out;
                    &filtered[..]
                },
                None => current,
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
                let sync_timeout =
                    handler.sync_timeout().map(|st| st.saturating_duration_since(Instant::now()));

                // Windows ConPTY workaround: never block `poll.wait`
                // indefinitely (no sync-timeout in flight -> `sync_timeout`
                // is `None` -> would otherwise wait forever). `conout`'s
                // read side is registered exactly ONCE at startup and
                // never re-registered on write-interest toggles (see
                // `Pty::reregister`'s own doc comment for why) — its
                // wakeups instead flow through `UnblockedReader`'s internal
                // `piper` pipe waker, posted from a completely separate OS
                // thread that continuously drains the real ConPTY handle.
                // Confirmed via direct instrumentation (headless benchmark
                // transmitting a real animated GIF, dozens of Kitty
                // graphics APC chunks arriving faster and more
                // continuously than any interactive shell output ever
                // would) that under a sustained fast write burst, this
                // waker's completion packet can go missing entirely: reads
                // stop advancing mid-transmission and `poll.wait` then
                // blocks for the remainder of the test's timeout with
                // ZERO events, even though the child process kept writing
                // successfully the whole time (confirmed via the writer's
                // own per-write logging). A capped wait bounds the damage
                // of any single missed wakeup to this interval — the next
                // iteration's own `try_read` call re-registers a fresh
                // waker regardless of whether `poll.wait` returned because
                // of a real event or just the cap expiring, so a missed
                // notification costs at most one extra `poll.wait` cycle
                // instead of stalling the whole session. Kept strictly
                // separate from `sync_timeout` itself (NOT folded into the
                // same `Option`) — the "handle synchronized update
                // timeout" branch right below must only fire on a REAL
                // sync timeout elapsing, not on this cap's own expiry,
                // or every idle period would spuriously call `stop_sync`
                // and emit a `Wakeup` every `MAX_POLL_WAIT`.
                #[cfg(windows)]
                const MAX_POLL_WAIT: std::time::Duration = std::time::Duration::from_millis(50);
                #[cfg(windows)]
                let wait_timeout = Some(sync_timeout.map_or(MAX_POLL_WAIT, |t| t.min(MAX_POLL_WAIT)));
                #[cfg(not(windows))]
                let wait_timeout = sync_timeout;

                events.clear();
                if let Err(err) = self.poll.wait(&mut events, wait_timeout) {
                    match err.kind() {
                        ErrorKind::Interrupted => continue,
                        _ => {
                            error!("Event loop polling error: {err}");
                            break 'event_loop;
                        },
                    }
                }

                // Handle synchronized update timeout.
                if events.is_empty() && self.rx.peek().is_none() && sync_timeout.is_some() {
                    state.parser.stop_sync(&mut *self.terminal.lock());
                    self.event_proxy.send_event(Event::Wakeup);
                    continue;
                }

                // Handle channel events, if there are any.
                if !self.drain_recv_channel(&mut state) {
                    break;
                }

                // Windows ConPTY workaround: proactively try a read even
                // when `poll.wait` reported zero events, instead of relying
                // solely on `PTY_READ_WRITE_TOKEN` showing up in `events`.
                // `conout`'s completion packet is posted by a `Waker`
                // registered deep inside `UnblockedReader::try_read` (via
                // `piper`'s own internal reader-waker plumbing, itself
                // driven by a THIRD, separate OS thread continuously
                // draining the real ConPTY handle) — three independent
                // async layers between "bytes are sitting in the buffer"
                // and "poll.wait actually returns a readable event for
                // them". Confirmed via direct instrumentation (a real
                // animated GIF's sustained Kitty graphics APC burst) that
                // this can go permanently silent: `piper`'s reader-side
                // buffer fills, its background thread parks waiting to be
                // woken by the NEXT `try_read` call — but that next call
                // never happens, because `poll.wait` never posts the event
                // that would trigger it, because nothing is producing that
                // event either. A genuine lost-wakeup between async layers,
                // not merely a slow one. Since `MAX_POLL_WAIT` above
                // already guarantees this loop iterates at least every
                // 50ms regardless, spending one of those idle iterations on
                // a real `pty_read` call directly breaks that cycle: it
                // re-invokes `try_read`, which re-registers a fresh waker
                // AND drains whatever already-buffered bytes are sitting
                // there — cheap when there's truly nothing to read (a
                // single non-blocking check), and the only thing that
                // actually recovers a silently-stuck read side otherwise.
                #[cfg(windows)]
                if events.is_empty() {
                    if let Err(err) = self.pty_read(&mut state, &mut buf, pipe.as_mut()) {
                        error!("Error reading from PTY in event loop: {err}");
                        break 'event_loop;
                    }
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

                    // Re-register with new interest — this is the exact
                    // moment that can spuriously re-trigger a readable
                    // event for already-drained data (see the in-buffer
                    // dedup check earlier in `pty_read`, which is what
                    // actually catches that case).
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
fn collapse_cr_cr_lf(chunk: &[u8], after_crlf: &mut bool) -> Vec<u8> {
    // `after_crlf`: the stream so far (this chunk or the previous one) ends in
    // `\r\n`. Only then is a `\r\r\n` the duplicate; on its own it is a real
    // newline (a shell's `\r\n` through the pty's `\n` -> `\r\n`) and is kept.
    let mut out = Vec::with_capacity(chunk.len());
    let mut i = 0;
    while i < chunk.len() {
        if *after_crlf && chunk[i..].starts_with(b"\r\r\n") {
            i += 3;
            continue;
        }
        out.push(chunk[i]);
        i += 1;
        *after_crlf = out.ends_with(b"\r\n") || (*after_crlf && out.is_empty());
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
    fn keeps_a_lone_cr_cr_lf_it_is_a_real_newline() {
        let mut pending = false;
        assert_eq!(collapse_cr_cr_lf(b"curl x\r\r\nprompt", &mut pending), b"curl x\r\r\nprompt");
    }

    #[test]
    fn drops_the_duplicate_split_across_chunks() {
        let mut pending = false;
        assert_eq!(collapse_cr_cr_lf(b"a\r\n", &mut pending), b"a\r\n");
        assert_eq!(collapse_cr_cr_lf(b"\r\r\nb", &mut pending), b"b");
    }

    #[test]
    fn collapses_several_cr_cr_lf_runs_in_one_chunk() {
        let mut pending = false;
        let out = collapse_cr_cr_lf(b"\r\n\r\r\nx\r\n\r\r\ny", &mut pending);
        assert_eq!(out, b"\r\nx\r\ny");
    }
}
