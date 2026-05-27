//! `epico-logger` — structured runtime logger used by the agent, dispatcher,
//! and load generator.
//!
//! Every call to [`Logger::info`] etc. writes to two places:
//!
//! - **Stderr** — human-readable, minimal, aligned. ANSI colour only when
//!   stderr is a TTY. Warnings and errors are coloured; info/debug are not,
//!   to reduce noise.
//!   Format: `HH:MM:SS  [level]  component   message   key=val  key=val`
//!
//! - **JSONL file** — one JSON object per line, line-buffered.
//!   Path: `<log_dir>/<component>_<YYYYMMDD_HHMMSS>.jsonl`
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
const CYAN:  &str   = "\x1b[36m";

fn use_colour() -> bool {
    #[cfg(unix)]
    {
        extern "C" { fn isatty(fd: i32) -> i32; }
        // Respect NO_COLOR convention (https://no-color.org).
        if std::env::var_os("NO_COLOR").is_some() { return false; }
        unsafe { isatty(2) != 0 }
    }
    #[cfg(not(unix))]
    { false }
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
    pub min_level:    Level,
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
            min_level:    Level::Info,
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
            min_level:    self.min_level,
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
        if level < self.min_level { return; }

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
        {
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
            let comp_col = paint(DIM, &comp_padded);

            let msg_col = match level.msg_colour() {
                Some(col) => paint(col, msg),
                None      => msg.to_owned(),
            };

            let kv = if fields.is_empty() {
                String::new()
            } else {
                let pairs: Vec<String> = fields.iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect();
                format!("  {}", paint(MUTE, &pairs.join("  ")))
            };

            eprintln!("{}  {}  {}  {}{}", time_col, tag_col, comp_col, msg_col, kv);
        }

        // ── JSONL ──
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

// Suppress unused-warning for CYAN if no consumer uses it; keep it in palette
// so callers extending the logger have a cool-toned accent available.
#[allow(dead_code)]
const _KEEP_CYAN: &str = CYAN;

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