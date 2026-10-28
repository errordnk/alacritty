use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::io::{Error, Result};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::IntoRawHandle;
use std::{mem, ptr};

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, S_OK};
use windows_sys::Win32::System::Console::{
    COORD, ClosePseudoConsole, CreatePseudoConsole, HPCON, ResizePseudoConsole,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_BREAKAWAY_OK,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    SetInformationJobObject,
};
use windows_sys::Win32::System::LibraryLoader::{GetProcAddress, LoadLibraryW};
use windows_sys::core::{HRESULT, PWSTR};
use windows_sys::{s, w};

use windows_sys::Win32::System::Threading::{
    CREATE_UNICODE_ENVIRONMENT, CreateProcessW, EXTENDED_STARTUPINFO_PRESENT,
    InitializeProcThreadAttributeList, PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, PROCESS_INFORMATION,
    STARTF_USESTDHANDLES, STARTUPINFOEXW, STARTUPINFOW, UpdateProcThreadAttribute,
};

use crate::event::{OnResize, WindowSize};
use crate::tty::Options;
use crate::tty::windows::blocking::{UnblockedReader, UnblockedWriter};
use crate::tty::windows::child::ChildExitWatcher;
use crate::tty::windows::{Pty, cmdline, win32_string};

const PIPE_CAPACITY: usize = crate::event_loop::READ_BUFFER_SIZE;

/// Size of `UnblockedReader`'s own in-process `piper` buffer for `conout`
/// specifically (NOT the OS-level pipe, NOT `conin`'s write-side buffer —
/// see `UnblockedReader::new`'s reader-thread doc comment). Deliberately
/// much smaller than `PIPE_CAPACITY`: `piper::fill_inner`'s read loop does
/// not return — and so does not wake the outer `pty_read`/`poll.wait`
/// consumer — until this buffer fills up or the underlying blocking
/// `ReadFile` stalls waiting for the next byte. Confirmed via direct
/// instrumentation (a real animated GIF sent over Kitty's graphics
/// protocol) that with a 1MB buffer here, a single `poll_fill` call
/// legitimately took 5+ seconds to return — the sender was writing in
/// bursts with brief pauses between `print!` calls, and `ReadFile` blocked
/// through every one of those pauses waiting for the buffer to fill
/// further, before ever handing control back. A much smaller buffer forces
/// `fill_inner` to return (and wake the consumer) far more often, at the
/// cost of slightly more read-loop overhead — an easy trade for output
/// this size and burstiness.
const CONOUT_READER_BUFFER_CAPACITY: usize = 64 * 1024;

/// Load the pseudoconsole API from conpty.dll if possible, otherwise use the
/// standard Windows API.
///
/// The conpty.dll from the Windows Terminal project
/// supports loading OpenConsole.exe, which offers many improvements and
/// bugfixes compared to the standard conpty that ships with Windows.
///
/// The conpty.dll and OpenConsole.exe files will be searched in PATH and in
/// the directory where Alacritty's executable is located.
type CreatePseudoConsoleFn =
    unsafe extern "system" fn(COORD, HANDLE, HANDLE, u32, *mut HPCON) -> HRESULT;
type ResizePseudoConsoleFn = unsafe extern "system" fn(HPCON, COORD) -> HRESULT;
type ClosePseudoConsoleFn = unsafe extern "system" fn(HPCON);

struct ConptyApi {
    create: CreatePseudoConsoleFn,
    resize: ResizePseudoConsoleFn,
    close: ClosePseudoConsoleFn,
}

impl ConptyApi {
    fn new() -> Self {
        match Self::load_conpty() {
            Some(conpty) => {
                info!("Using conpty.dll for pseudoconsole");
                conpty
            },
            None => {
                // Cannot load conpty.dll - use the standard Windows API.
                info!("Using Windows API for pseudoconsole");
                Self {
                    create: CreatePseudoConsole,
                    resize: ResizePseudoConsole,
                    close: ClosePseudoConsole,
                }
            },
        }
    }

    /// Try loading ConptyApi from conpty.dll library.
    fn load_conpty() -> Option<Self> {
        type LoadedFn = unsafe extern "system" fn() -> isize;
        unsafe {
            let hmodule = LoadLibraryW(w!("conpty.dll"));
            if hmodule.is_null() {
                return None;
            }
            let create_fn = GetProcAddress(hmodule, s!("CreatePseudoConsole"))?;
            let resize_fn = GetProcAddress(hmodule, s!("ResizePseudoConsole"))?;
            let close_fn = GetProcAddress(hmodule, s!("ClosePseudoConsole"))?;

            Some(Self {
                create: mem::transmute::<LoadedFn, CreatePseudoConsoleFn>(create_fn),
                resize: mem::transmute::<LoadedFn, ResizePseudoConsoleFn>(resize_fn),
                close: mem::transmute::<LoadedFn, ClosePseudoConsoleFn>(close_fn),
            })
        }
    }
}

/// RAII Pseudoconsole.
pub struct Conpty {
    pub handle: HPCON,
    api: ConptyApi,
    /// Windows Job Object this session's real child process (the shell,
    /// and transitively everything IT spawns — `somsrp`, `ssh`, any
    /// nested command) was assigned to at creation time, with
    /// `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` set — closing the job's last
    /// handle (here, in `Drop` below) tells Windows to terminate every
    /// process still in it. Without this, Windows has no equivalent of
    /// POSIX's process-group `SIGKILL` (`killpg`, already used on the
    /// Unix side of this same lifecycle — see `pty_info.rs`'s
    /// `kill_current_process`): a plain `TerminateProcess` on the shell
    /// alone leaves any orphaned grandchild (a `somsrp` a user left
    /// running inside a live full-terminal widget, mid-video, when the
    /// whole terminal app closes) running forever with nothing left to
    /// read its output. `None` if job creation/assignment failed for any
    /// reason (best-effort — a PTY session still works without this, it
    /// would just leak orphans on close the same way it always has).
    job_handle: Option<HANDLE>,
}

impl Drop for Conpty {
    fn drop(&mut self) {
        // XXX: This will block until the conout pipe is drained. Will cause a deadlock if the
        // conout pipe has already been dropped by this point.
        //
        // See PR #3084 and https://docs.microsoft.com/en-us/windows/console/closepseudoconsole.
        unsafe { (self.api.close)(self.handle) }
        // Closing the job handle AFTER the pseudoconsole — order doesn't
        // matter for correctness (the job's kill-on-close only touches
        // the real Win32 process tree, entirely separate from the
        // ConPTY's own pipe/console-buffer teardown above), kept this way
        // simply to mirror this function's existing pseudoconsole-first
        // structure.
        if let Some(job_handle) = self.job_handle.take() {
            unsafe {
                CloseHandle(job_handle);
            }
        }
    }
}

// The ConPTY handle can be sent between threads.
unsafe impl Send for Conpty {}

pub fn new(config: &Options, window_size: WindowSize) -> Result<Pty> {
    let api = ConptyApi::new();
    let mut pty_handle: HPCON = 0;

    // Passing 0 as the size parameter allows the "system default" buffer
    // size to be used. There may be small performance and memory advantages
    // to be gained by tuning this in the future, but it's likely a reasonable
    // start point.
    //
    // A larger explicit size (previously tried: PIPE_CAPACITY, 1MB) makes
    // things WORSE, not better, for a real animated-GIF Kitty graphics
    // transmission — see UnblockedReader::new's own reader-thread doc
    // comment for why: the actual bottleneck is `piper::fill_inner`'s loop
    // not returning (and so not waking the outer `pty_read`/`poll.wait`
    // consumer) until ITS OWN internal buffer — sized by the
    // `pipe_capacity` argument to `UnblockedReader::new`, i.e.
    // `PIPE_CAPACITY` below, not this OS-level pipe — fills up or the
    // blocking `ReadFile` underneath it stalls waiting for the next byte.
    // A larger OS-level pipe just lets more bytes pile up before that
    // inner loop notices, making the stall longer, not shorter.
    let (conout, conout_pty_handle) = miow::pipe::anonymous(0)?;
    let (conin_pty_handle, conin) = miow::pipe::anonymous(0)?;

    // Create the Pseudo Console, using the pipes.
    let result = unsafe {
        (api.create)(
            window_size.into(),
            conin_pty_handle.into_raw_handle() as HANDLE,
            conout_pty_handle.into_raw_handle() as HANDLE,
            0,
            &mut pty_handle as *mut _,
        )
    };

    assert_eq!(result, S_OK);

    let mut success;

    // Prepare child process startup info.

    let mut size: usize = 0;

    let mut startup_info_ex: STARTUPINFOEXW = unsafe { mem::zeroed() };

    startup_info_ex.StartupInfo.lpTitle = std::ptr::null_mut() as PWSTR;

    startup_info_ex.StartupInfo.cb = mem::size_of::<STARTUPINFOEXW>() as u32;

    // Setting this flag but leaving all the handles as default (null) ensures the
    // PTY process does not inherit any handles from this Alacritty process.
    startup_info_ex.StartupInfo.dwFlags |= STARTF_USESTDHANDLES;

    // Create the appropriately sized thread attribute list.
    unsafe {
        let failure =
            InitializeProcThreadAttributeList(ptr::null_mut(), 1, 0, &mut size as *mut usize) > 0;

        // This call was expected to return false.
        if failure {
            return Err(Error::last_os_error());
        }
    }

    let mut attr_list: Box<[u8]> = vec![0; size].into_boxed_slice();

    // Set startup info's attribute list & initialize it
    //
    // Lint failure is spurious; it's because winapi's definition of PROC_THREAD_ATTRIBUTE_LIST
    // implies it is one pointer in size (32 or 64 bits) but really this is just a dummy value.
    // Casting a *mut u8 (pointer to 8 bit type) might therefore not be aligned correctly in
    // the compiler's eyes.
    #[allow(clippy::cast_ptr_alignment)]
    {
        startup_info_ex.lpAttributeList = attr_list.as_mut_ptr() as _;
    }

    unsafe {
        success = InitializeProcThreadAttributeList(
            startup_info_ex.lpAttributeList,
            1,
            0,
            &mut size as *mut usize,
        ) > 0;

        if !success {
            return Err(Error::last_os_error());
        }
    }

    // Set thread attribute list's Pseudo Console to the specified ConPTY.
    unsafe {
        success = UpdateProcThreadAttribute(
            startup_info_ex.lpAttributeList,
            0,
            PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE as usize,
            pty_handle as *mut std::ffi::c_void,
            mem::size_of::<HPCON>(),
            ptr::null_mut(),
            ptr::null_mut(),
        ) > 0;

        if !success {
            return Err(Error::last_os_error());
        }
    }

    // Prepare child process creation arguments.
    let cmdline = win32_string(&cmdline(config));
    let cwd = config.working_directory.as_ref().map(win32_string);
    let mut creation_flags = EXTENDED_STARTUPINFO_PRESENT;
    let custom_env_block = convert_custom_env(&config.env);
    let custom_env_block_pointer = match &custom_env_block {
        Some(custom_env_block) => {
            creation_flags |= CREATE_UNICODE_ENVIRONMENT;
            custom_env_block.as_ptr() as *mut std::ffi::c_void
        },
        None => ptr::null_mut(),
    };

    let mut proc_info: PROCESS_INFORMATION = unsafe { mem::zeroed() };
    unsafe {
        success = CreateProcessW(
            ptr::null(),
            cmdline.as_ptr() as PWSTR,
            ptr::null_mut(),
            ptr::null_mut(),
            false as i32,
            creation_flags,
            custom_env_block_pointer,
            cwd.as_ref().map_or_else(ptr::null, |s| s.as_ptr()),
            &mut startup_info_ex.StartupInfo as *mut STARTUPINFOW,
            &mut proc_info as *mut PROCESS_INFORMATION,
        ) > 0;

        if !success {
            return Err(Error::last_os_error());
        }
    }

    // Best-effort — a failure here (job creation, or a process already
    // being part of another job that itself forbids nesting on older
    // Windows versions predating job nesting support) still leaves a
    // perfectly working PTY session, just without the "close kills the
    // whole tree" guarantee this job exists to provide. See `Conpty::
    // job_handle`'s own doc comment for why this matters at all.
    let job_handle = create_kill_on_close_job_and_assign(proc_info.hProcess);

    let conin = UnblockedWriter::new(conin, PIPE_CAPACITY);
    let conout = UnblockedReader::new(conout, CONOUT_READER_BUFFER_CAPACITY);

    let child_watcher = ChildExitWatcher::new(proc_info.hProcess)?;
    let conpty = Conpty { handle: pty_handle as HPCON, api, job_handle };

    Ok(Pty::new(conpty, conout, conin, child_watcher))
}

/// Creates an unnamed Windows Job Object with `JOB_OBJECT_LIMIT_KILL_ON_
/// JOB_CLOSE` and assigns `process_handle` (the shell this PTY session
/// just spawned) to it. Returns `None` on any failure — see this
/// function's own call site for why that's an acceptable degraded mode,
/// not treated as a hard error for the whole PTY creation.
///
/// Assigning only the immediate child (the shell) is sufficient to catch
/// its whole subtree: Windows child processes inherit their parent's job
/// membership by default (unless a child is itself created with
/// `CREATE_BREAKAWAY_FROM_JOB`, which nothing in this codebase's own
/// spawn paths — `somsrp`, `ssh`, etc. — ever requests), so `somsrp` (or
/// any other program a user runs inside this shell) ends up in the SAME
/// job transitively, with no per-grandchild bookkeeping needed here.
fn create_kill_on_close_job_and_assign(process_handle: HANDLE) -> Option<HANDLE> {
    unsafe {
        let job_handle = CreateJobObjectW(ptr::null(), ptr::null());
        if job_handle.is_null() {
            return None;
        }

        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = mem::zeroed();
        // BREAKAWAY_OK lets a process that asks for it (`CREATE_BREAKAWAY_FROM_JOB`)
        // leave the job: a daemon meant to outlive the terminal (somsrv) does.
        // Everything else still dies with the job.
        info.BasicLimitInformation.LimitFlags =
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | JOB_OBJECT_LIMIT_BREAKAWAY_OK;
        let set_ok = SetInformationJobObject(
            job_handle,
            JobObjectExtendedLimitInformation,
            &info as *const _ as *const std::ffi::c_void,
            mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        );
        if set_ok == 0 {
            CloseHandle(job_handle);
            return None;
        }

        if AssignProcessToJobObject(job_handle, process_handle) == 0 {
            CloseHandle(job_handle);
            return None;
        }

        Some(job_handle)
    }
}

// Windows environment variables are case-insensitive, and the caller is responsible for
// deduplicating environment variables, so do that here while converting.
//
// https://learn.microsoft.com/en-us/previous-versions/troubleshoot/windows/win32/createprocess-cannot-eliminate-duplicate-variables#environment-variables
fn convert_custom_env(custom_env: &HashMap<String, String>) -> Option<Vec<u16>> {
    // Windows inherits parent's env when no `lpEnvironment` parameter is specified.
    if custom_env.is_empty() {
        return None;
    }

    let mut converted_block = Vec::new();
    let mut all_env_keys = HashSet::new();
    for (custom_key, custom_value) in custom_env {
        let custom_key_os = OsStr::new(custom_key);
        if all_env_keys.insert(custom_key_os.to_ascii_uppercase()) {
            add_windows_env_key_value_to_block(
                &mut converted_block,
                custom_key_os,
                OsStr::new(&custom_value),
            );
        } else {
            warn!(
                "Omitting environment variable pair with duplicate key: \
                 '{custom_key}={custom_value}'"
            );
        }
    }

    // Pull the current process environment after, to avoid overwriting the user provided one.
    for (inherited_key, inherited_value) in std::env::vars_os() {
        if all_env_keys.insert(inherited_key.to_ascii_uppercase()) {
            add_windows_env_key_value_to_block(
                &mut converted_block,
                &inherited_key,
                &inherited_value,
            );
        }
    }

    converted_block.push(0);
    Some(converted_block)
}

// According to the `lpEnvironment` parameter description:
// https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-createprocessa#parameters
//
// > An environment block consists of a null-terminated block of null-terminated strings. Each
// string is in the following form:
// >
// > name=value\0
fn add_windows_env_key_value_to_block(block: &mut Vec<u16>, key: &OsStr, value: &OsStr) {
    block.extend(key.encode_wide());
    block.push('=' as u16);
    block.extend(value.encode_wide());
    block.push(0);
}

impl OnResize for Conpty {
    fn on_resize(&mut self, window_size: WindowSize) {
        let result = unsafe { (self.api.resize)(self.handle, window_size.into()) };
        assert_eq!(result, S_OK);
    }
}

impl From<WindowSize> for COORD {
    fn from(window_size: WindowSize) -> Self {
        let lines = window_size.num_lines;
        let columns = window_size.num_cols;
        COORD { X: columns as i16, Y: lines as i16 }
    }
}
