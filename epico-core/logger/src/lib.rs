//! `epico-logger` — structured runtime logger used by the agent, dispatcher,
//! and load generator.
//!
//! Every log call writes to two places:
//!
//! - **Stderr** — human-readable, minimal, aligned. ANSI colour only when
//!   stderr is a TTY. Warnings and errors are coloured; info/debug are not,
//!   to reduce noise.
//!   Format: `HH:MM:SS  [level]  component   message   key=val  key=val`
//!
//! - **JSONL file** — one JSON object per line, line-buffered.
//!   Path: `<log_dir>/<component>_<YYYYMMDD_HHMMSS>.jsonl`
//!
//! # Logging
//!
//! Prefer the field macros — any `Display` value works, no manual
//! `.to_string()`:
//!
//! ```ignore
//! use epico_logger::{info, warn};
//!
//! info!(log, "scale up", qd = qd, current = current, new = current + 1);
//! warn!(log, "worker gone", rid = String::from_utf8_lossy(&id));
//! info!(log, "ready");                       // no fields
//! info!(log, "bound", "socket-addr" = addr); // non-ident keys as literals
//! ```
//!
//! The slice methods ([`Logger::info`] etc.) remain for dynamically built
//! field lists.
//!
//! The minimum level is `info` by default; set `EPICO_LOG=debug` (or
//! `warn`/`error`) to change it at process start.
//!
//! Call [`Logger::finalize`] at the end of a run to write a companion
//! `_summary.json` that the HTML report generator reads.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{json, Value};

// ── ANSI colour ──────────────────────────────────────────────────────────────
//
// Palette notes:
// - `info` has NO colour on the level tag. Flooding the terminal with green
//   makes real signal (warnings, errors) harder to see.
// - `debug` uses 256-colour dim grey (240) so debug lines visually recede.
// - `warn` and `error` use plain 33/31 (no `0;` prefix) so they render at
//   the terminal's normal weight instead of the washed-out "normal intensity"
//   variant some emulators pick for `\x1b[0;3Xm`.
// - Metadata (timestamp, component, field pairs) all share the same dim grey
//   so the message itself is the only thing at full contrast.

const RESET: &str   = "\x1b[0m";
const DIM:   &str   = "\x1b[38;5;244m"; // soft grey for metadata
const MUTE:  &str   = "\x1b[38;5;240m"; // softer grey for fields / debug
const YEL:   &str   = "\x1b[33m";
const RED:   &str   = "\x1b[31m";
const VAL:   &str   = "\x1b[38;5;250m"; // field values: brighter than keys

/// Stable, muted 256-colour palette for component names. Muted tones so the
/// gutter stays calm; the message keeps full contrast.
const COMP_COLOURS: &[&str] = &[
    "\x1b[38;5;73m",  // teal
    "\x1b[38;5;110m", // steel blue
    "\x1b[38;5;139m", // mauve
    "\x1b[38;5;108m", // sage
    "\x1b[38;5;180m", // sand
    "\x1b[38;5;146m", // lavender grey
];

/// Deterministic colour per component name (FNV-1a over the bytes).
fn component_colour(name: &str) -> &'static str {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    COMP_COLOURS[(h % COMP_COLOURS.len() as u64) as usize]
}

/// TTY + NO_COLOR detection, computed once per process. The old version ran
/// an env lookup plus an isatty syscall for every painted fragment — several
/// times per log line.
fn use_colour() -> bool {
    static USE_COLOUR: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *USE_COLOUR.get_or_init(|| {
        #[cfg(unix)]
        {
            extern "C" { fn isatty(fd: i32) -> i32; }
            // Respect NO_COLOR convention (https://no-color.org).
            if std::env::var_os("NO_COLOR").is_some() { return false; }
            unsafe { isatty(2) != 0 }
        }
        #[cfg(not(unix))]
        { false }
    })
}

fn paint(code: &str, text: &str) -> String {
    if use_colour() { format!("{}{}{}", code, text, RESET) }
    else            { text.to_owned() }
}

// ── Log level ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Level { Debug, Info, Warn, Error }

impl Level {
    /// 5-char lowercase tag, padded. Lowercase feels quieter than shouty caps.
    fn tag(self) -> &'static str {
        match self {
            Level::Debug => "debug",
            Level::Info  => "info ",
            Level::Warn  => "warn ",
            Level::Error => "error",
        }
    }
    /// Colour for the tag itself. `None` means render plain (for info).
    fn tag_colour(self) -> Option<&'static str> {
        match self {
            Level::Debug => Some(MUTE),
            Level::Info  => None,
            Level::Warn  => Some(YEL),
            Level::Error => Some(RED),
        }
    }
    /// Colour for the message body. Debug/info stay default; warn/error pop.
    fn msg_colour(self) -> Option<&'static str> {
        match self {
            Level::Warn  => Some(YEL),
            Level::Error => Some(RED),
            _            => None,
        }
    }
}

// ── Inner shared state ───────────────────────────────────────────────────────

struct Inner {
    writer: BufWriter<File>,
}

// ── Logger ───────────────────────────────────────────────────────────────────

/// Structured logger. Cheap to clone — all clones share the same underlying
/// file writer. Use [`Logger::with_component`] to get a handle that prefixes
/// messages with a different component name.
#[derive(Clone)]
pub struct Logger {
    component:        String,
    inner:            Arc<Mutex<Inner>>,
    pub jsonl_path:   PathBuf,
    pub summary_path: PathBuf,
    /// Minimum level rendered to STDERR. From `EPICO_LOG` (default: info).
    /// The human view is a filtered projection; the JSONL file is the record.
    pub stderr_level: Level,
    /// Minimum level written to the JSONL file. Default: debug — the file
    /// records everything, so `debug!` telemetry (e.g. worker boot phase
    /// breakdowns) is always available to analysis scripts without re-running
    /// with a different log level.
    pub file_level:   Level,
    /// Width to pad component to on stderr. Default 18. Longer names overflow
    /// gracefully rather than getting truncated.
    pub comp_width:   usize,
}

impl Logger {
    /// Open a new logger for `component`, writing into `log_dir`.
    /// File name: `<component>_<YYYYMMDD_HHMMSS>.jsonl`
    pub fn new(component: &str, log_dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let dir = log_dir.as_ref();
        std::fs::create_dir_all(dir)?;

        let ts   = wall_now();
        let slug = format_ts_slug(ts);
        let safe = component.replace('/', "-").replace(' ', "_");
        let jsonl_path   = dir.join(format!("{}_{}.jsonl",        safe, slug));
        let summary_path = dir.join(format!("{}_{}_summary.json", safe, slug));

        let file = OpenOptions::new()
            .create(true).write(true).truncate(true)
            .open(&jsonl_path)?;

        let logger = Logger {
            component:    component.to_owned(),
            inner:        Arc::new(Mutex::new(Inner { writer: BufWriter::new(file) })),
            jsonl_path:   jsonl_path.clone(),
            summary_path: summary_path.clone(),
            stderr_level: level_from_env(),
            file_level:   Level::Debug,
            comp_width:   18,
        };

        logger.info("logger opened", &[
            ("jsonl",   jsonl_path.to_string_lossy().as_ref()),
            ("summary", summary_path.to_string_lossy().as_ref()),
        ]);
        Ok(logger)
    }

    /// Return a new handle with a different component label, sharing the same file.
    pub fn with_component(&self, component: &str) -> Logger {
        Logger {
            component:    component.to_owned(),
            inner:        self.inner.clone(),
            jsonl_path:   self.jsonl_path.clone(),
            summary_path: self.summary_path.clone(),
            stderr_level: self.stderr_level,
            file_level:   self.file_level,
            comp_width:   self.comp_width,
        }
    }

    // ── Public API ───────────────────────────────────────────────────────────

    pub fn debug(&self, msg: &str, fields: &[(&str, &str)]) { self.emit(Level::Debug, msg, fields); }
    pub fn info (&self, msg: &str, fields: &[(&str, &str)]) { self.emit(Level::Info,  msg, fields); }
    pub fn warn (&self, msg: &str, fields: &[(&str, &str)]) { self.emit(Level::Warn,  msg, fields); }
    pub fn error(&self, msg: &str, fields: &[(&str, &str)]) { self.emit(Level::Error, msg, fields); }

    /// Write a `_summary.json` file alongside the JSONL log.
    /// `summary` is any JSON-serialisable value — typically a `serde_json::Value::Object`.
    /// The logger injects `_jsonl_path` into the object before writing.
    pub fn finalize(&self, summary: &Value) -> std::io::Result<()> {
        let mut obj = match summary.as_object().cloned() {
            Some(m) => m,
            None    => serde_json::Map::new(),
        };
        obj.insert("_jsonl_path".into(), json!(self.jsonl_path.to_string_lossy()));
        obj.insert("_component".into(),  json!(&self.component));

        let text = serde_json::to_string_pretty(&Value::Object(obj))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        std::fs::write(&self.summary_path, text.as_bytes())?;

        self.info("summary written", &[
            ("path", self.summary_path.to_string_lossy().as_ref()),
        ]);

        if let Ok(mut inner) = self.inner.lock() {
            inner.writer.flush()?;
        }
        Ok(())
    }

    // ── Core emit ────────────────────────────────────────────────────────────

    fn emit(&self, level: Level, msg: &str, fields: &[(&str, &str)]) {
        let to_stderr = level >= self.stderr_level;
        let to_file   = level >= self.file_level;
        if !to_stderr && !to_file { return; }

        let ts       = wall_now();
        let wall_str = format_wall_time(ts);

        // ── Stderr ──
        //
        // Column grid (two spaces between columns):
        //
        //   HH:MM:SS  [level]  component         message                 k=v  k=v
        //             └─ 7 ─┘  └── padded to comp_width ┘
        //
        // The whole left gutter (time + tag + component) is dim except for the
        // level tag when warn/error/debug. The message is the only high-contrast
        // element (unless warn/error, then it matches the tag colour).
        if to_stderr {
            let time_col = paint(DIM, &wall_str);

            let tag_raw = level.tag();
            let tag_bracketed = format!("[{}]", tag_raw);
            let tag_col = match level.tag_colour() {
                Some(col) => paint(col, &tag_bracketed),
                None      => tag_bracketed,
            };

            // Pad component in raw chars, then colour, so ANSI codes don't
            // break alignment. Overflow: if component is longer than
            // comp_width we print it as-is and add a single space.
            let comp_padded = if self.component.len() >= self.comp_width {
                format!("{} ", self.component)
            } else {
                format!("{:<width$}", self.component, width = self.comp_width)
            };
            // Stable per-component colour so interleaved threads are
            // visually separable at a glance (autoscaler/relay vs
            // worker/forward etc.). Dim greys stay for time and fields.
            let comp_col = paint(component_colour(&self.component), &comp_padded);

            let msg_col = match level.msg_colour() {
                Some(col) => paint(col, msg),
                None      => msg.to_owned(),
            };

            // `key=` recedes (mute), the value carries the information
            // (brighter grey) — scanning a line reads values, not keys.
            let kv = if fields.is_empty() {
                String::new()
            } else {
                let pairs: Vec<String> = fields.iter()
                    .map(|(k, v)| format!("{}{}", paint(MUTE, &format!("{}=", k)), paint(VAL, v)))
                    .collect();
                format!("  {}", pairs.join("  "))
            };

            eprintln!("{}  {}  {}  {}{}", time_col, tag_col, comp_col, msg_col, kv);
        }

        // ── JSONL ──
        if !to_file { return; }
        let mut obj = serde_json::Map::new();
        obj.insert("ts".into(),        json!(round4(ts)));
        obj.insert("level".into(),     json!(level));
        obj.insert("component".into(), json!(&self.component));
        obj.insert("msg".into(),       json!(msg));
        for (k, v) in fields {
            obj.insert(k.to_string(), json!(v));
        }

        if let Ok(line) = serde_json::to_string(&Value::Object(obj)) {
            if let Ok(mut inner) = self.inner.lock() {
                let _ = writeln!(inner.writer, "{}", line);
                let _ = inner.writer.flush();
            }
        }
    }
}

// ── Field macros ─────────────────────────────────────────────────────────────

/// Internal: turn a field key token (bare ident or string literal) into &str.
#[doc(hidden)]
#[macro_export]
macro_rules! __field_key {
    ($k:ident)   => { stringify!($k) };
    ($k:literal) => { $k };
}

/// Internal: shared expansion for the level macros. Values are formatted via
/// `Display`; the temporary Strings live to the end of the statement, so the
/// borrowed slice is valid for the call.
#[doc(hidden)]
#[macro_export]
macro_rules! __log_at {
    ($method:ident, $log:expr, $msg:expr $(, $k:tt = $v:expr)* $(,)?) => {
        $log.$method($msg, &[ $( ($crate::__field_key!($k), ::std::format!("{}", $v).as_str()) ),* ])
    };
}

/// `info!(log, "message", key = value, ...)` — values are any `Display` type.
#[macro_export]
macro_rules! info {
    ($($t:tt)*) => { $crate::__log_at!(info, $($t)*) };
}

/// `warn!(log, "message", key = value, ...)`
#[macro_export]
macro_rules! warn {
    ($($t:tt)*) => { $crate::__log_at!(warn, $($t)*) };
}

/// `error!(log, "message", key = value, ...)`
#[macro_export]
macro_rules! error {
    ($($t:tt)*) => { $crate::__log_at!(error, $($t)*) };
}

/// `debug!(log, "message", key = value, ...)` — emitted only when
/// `EPICO_LOG=debug`.
#[macro_export]
macro_rules! debug {
    ($($t:tt)*) => { $crate::__log_at!(debug, $($t)*) };
}

// ── Level from env ───────────────────────────────────────────────────────────

fn level_from_env() -> Level {
    match std::env::var("EPICO_LOG").ok().as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("debug") => Level::Debug,
        Some("warn")  => Level::Warn,
        Some("error") => Level::Error,
        _             => Level::Info,
    }
}

// ── Time helpers ─────────────────────────────────────────────────────────────

fn wall_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn round4(v: f64) -> f64 { (v * 10_000.0).round() / 10_000.0 }

fn format_wall_time(ts: f64) -> String {
    let secs = ts as u64;
    format!("{:02}:{:02}:{:02}", (secs / 3600) % 24, (secs / 60) % 60, secs % 60)
}

fn format_ts_slug(ts: f64) -> String {
    let secs = ts as u64;
    let hh   = (secs / 3600) % 24;
    let mm   = (secs / 60) % 60;
    let ss   = secs % 60;

    // Proper proleptic Gregorian calendar from Unix epoch.
    // Algorithm: https://howardhinnant.github.io/date_algorithms.html
    let z   = (secs / 86400) as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y   = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp  = (5 * doy + 2) / 153;
    let d   = doy - (153 * mp + 2) / 5 + 1;
    let m   = if mp < 10 { mp + 3 } else { mp - 9 };
    let y   = if m <= 2 { y + 1 } else { y };

    format!("{:04}{:02}{:02}_{:02}{:02}{:02}", y, m, d, hh, mm, ss)
}