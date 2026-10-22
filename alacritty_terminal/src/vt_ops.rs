//! The trace of every escape sequence the parser hands to the terminal
//! ("Going to: line=..", "Setting attribute: ..", "Configuring charset ..") lives
//! in a module of its own on purpose. A screenful of redrawn text is hundreds of
//! these, so with the whole program at `trace` they bury everything else and
//! make a file logger the bottleneck. As `alacritty_terminal::vt_ops` they can
//! be filtered separately: an embedder caps this one module (Som does, at
//! `debug`) and anyone who wants the firehose asks for it by name.

/// Logs one escape-sequence event at `trace` level.
#[inline]
pub fn trace(args: core::fmt::Arguments<'_>) {
    log::trace!("{args}");
}
