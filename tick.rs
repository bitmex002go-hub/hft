#!/usr/bin/env rust-script
// tick.rs
// E2E single-file Rust loader for Binance USD-M Futures offline aggTrades.
//
// Default behavior:
//   ./tick
// means:
//   all USD-M futures assets, yesterday UTC, output input-bn.txt
//
// Output schema is exactly 8 fields, exactly this order:
//   s,a,p,q,f,l,T,m
//
// No Cargo.toml is required. This file uses Rust std only, and calls system tools:
//   curl, unzip
// Optional checksum verification uses:
//   sha256sum or shasum
//
// Compile:
//   rustc tick.rs -O -o tick
//
// Default, all assets, yesterday UTC:
//   ./tick
//
// Explicit all assets and date:
//   ./tick --all-assets --start 2026-07-01 --end 2026-07-01 --out input-bn.txt --resume
//
// Single symbol:
//   ./tick --symbol BTCUSDT --start 2026-07-01 --end 2026-07-01 --out input-bn.txt
//
// Test first 10 discovered symbols only:
//   ./tick --all-assets --max-symbols 10

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

const BASE_URL: &str = "https://data.binance.vision/data/futures/um/daily/aggTrades";
const DEFAULT_OUT: &str = "input-bn.txt";

#[derive(Debug, Clone)]
struct Args {
    symbol: String,
    symbols: Option<String>,
    all_assets: bool,
    max_symbols: usize,
    start: String,
    end: String,
    out: PathBuf,
    append: bool,
    resume: bool,
    verify: bool,
    keep_zip: bool,
    tmp_dir: PathBuf,
    max_rows: usize,
}

#[derive(Debug, Clone)]
struct AggTrade {
    s: String,
    a: i64,
    p: String,
    q: String,
    f: i64,
    l: i64,
    t: i64,
    m: bool,
}

fn print_usage() {
    eprintln!(
        "Usage:\n  tick [defaults: --all-assets --start yesterday-UTC --end yesterday-UTC --out input-bn.txt]\n  tick --symbol BTCUSDT --start YYYY-MM-DD --end YYYY-MM-DD\n  tick --symbols BTCUSDT,ETHUSDT --start YYYY-MM-DD --end YYYY-MM-DD\n  tick --all-assets --start YYYY-MM-DD --end YYYY-MM-DD\n\nOptions:\n  --symbol SYMBOL             Single symbol. Passing this disables default all-assets unless --all-assets is also passed.\n  --symbols A,B,C             Comma-separated symbols. Passing this disables default all-assets unless --all-assets is also passed.\n  --all-assets                Discover and download every symbol directory under Binance UM futures aggTrades.\n  --max-symbols N             0 = unlimited; useful for testing --all-assets.\n  --start YYYY-MM-DD          Start date. Default = yesterday UTC.\n  --end YYYY-MM-DD            End date. Default = start date, or yesterday UTC when start is omitted.\n  --out PATH                  Output JSONL file, default input-bn.txt.\n  --append                    Append to output.\n  --resume                    Append and skip existing (s,a) keys.\n  --verify                    Verify .CHECKSUM if available.\n  --max-rows N                0 = unlimited.\n  --tmp-dir PATH              Temp directory, default OS temp.\n  --keep-zip                  Keep downloaded zip files.\n\nOutput JSONL row, exactly 8 fields:\n  {\"s\":\"BTCUSDT\",\"a\":5933014,\"p\":\"0.001\",\"q\":\"100\",\"f\":100,\"l\":105,\"T\":123456785,\"m\":true}\n"
    );
}

fn parse_args() -> Result<Args, String> {
    let mut kv: HashMap<String, String> = HashMap::new();
    let mut flags: HashSet<String> = HashSet::new();

    let mut it = env::args().skip(1).peekable();
    while let Some(arg) = it.next() {
        if arg == "--help" || arg == "-h" {
            print_usage();
            std::process::exit(0);
        }
        if !arg.starts_with("--") {
            return Err(format!("unexpected positional argument: {arg}"));
        }
        match arg.as_str() {
            "--append" | "--resume" | "--verify" | "--keep-zip" | "--all-assets" => {
                flags.insert(arg.trim_start_matches("--").to_string());
            }
            "--symbol" | "--symbols" | "--start" | "--end" | "--out" | "--max-rows" | "--tmp-dir" | "--max-symbols" => {
                let Some(v) = it.next() else {
                    return Err(format!("missing value after {arg}"));
                };
                kv.insert(arg.trim_start_matches("--").to_string(), v);
            }
            _ => return Err(format!("unknown option: {arg}")),
        }
    }

    let explicit_symbol = kv.contains_key("symbol") || kv.contains_key("symbols");
    let default_day = yesterday_utc_date();
    let start_raw = kv.remove("start");
    let end_raw = kv.remove("end");
    let (start, end) = match (start_raw, end_raw) {
        (Some(s), Some(e)) => (s, e),
        (Some(s), None) => (s.clone(), s),
        (None, Some(e)) => (e.clone(), e),
        (None, None) => (default_day.clone(), default_day),
    };

    let max_rows = kv
        .remove("max-rows")
        .unwrap_or_else(|| "0".to_string())
        .parse::<usize>()
        .map_err(|e| format!("bad --max-rows: {e}"))?;

    let max_symbols = kv
        .remove("max-symbols")
        .unwrap_or_else(|| "0".to_string())
        .parse::<usize>()
        .map_err(|e| format!("bad --max-symbols: {e}"))?;

    let tmp_dir = kv
        .remove("tmp-dir")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir);

    Ok(Args {
        symbol: kv.remove("symbol").unwrap_or_else(|| "BTCUSDT".to_string()).to_ascii_uppercase(),
        symbols: kv.remove("symbols"),
        all_assets: flags.contains("all-assets") || !explicit_symbol,
        max_symbols,
        start,
        end,
        out: PathBuf::from(kv.remove("out").unwrap_or_else(|| DEFAULT_OUT.to_string())),
        append: flags.contains("append"),
        resume: flags.contains("resume"),
        verify: flags.contains("verify"),
        keep_zip: flags.contains("keep-zip"),
        tmp_dir,
        max_rows,
    })
}

fn parse_bool(x: &str) -> Result<bool, String> {
    match x.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "t" | "yes" => Ok(true),
        "false" | "0" | "f" | "no" => Ok(false),
        other => Err(format!("invalid boolean: {other:?}")),
    }
}

fn parse_date(s: &str) -> Result<(i64, i64, i64), String> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return Err(format!("bad date: {s}; expected YYYY-MM-DD"));
    }
    let y = parts[0].parse::<i64>().map_err(|e| format!("bad year in {s}: {e}"))?;
    let m = parts[1].parse::<i64>().map_err(|e| format!("bad month in {s}: {e}"))?;
    let d = parts[2].parse::<i64>().map_err(|e| format!("bad day in {s}: {e}"))?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(format!("bad date range: {s}"));
    }
    Ok((y, m, d))
}

// Howard Hinnant civil-date algorithms.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = y - if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let y = y + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

fn format_date(y: i64, m: i64, d: i64) -> String {
    format!("{y:04}-{m:02}-{d:02}")
}

fn yesterday_utc_date() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let today_days = secs / 86_400;
    let (y, m, d) = civil_from_days(today_days - 1);
    format_date(y, m, d)
}

fn date_range(start: &str, end: &str) -> Result<Vec<String>, String> {
    let (sy, sm, sd) = parse_date(start)?;
    let (ey, em, ed) = parse_date(end)?;
    let mut a = days_from_civil(sy, sm, sd);
    let b = days_from_civil(ey, em, ed);
    if b < a {
        return Err("--end must be >= --start".to_string());
    }
    let mut out = Vec::new();
    while a <= b {
        let (y, m, d) = civil_from_days(a);
        out.push(format_date(y, m, d));
        a += 1;
    }
    Ok(out)
}

fn make_url(symbol: &str, day: &str) -> String {
    format!("{BASE_URL}/{symbol}/{symbol}-aggTrades-{day}.zip")
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn download_to_file(url: &str, dest: &Path) -> Result<bool, String> {
    let status = Command::new("curl")
        .args(["-fL", "--retry", "4", "--connect-timeout", "30", "--max-time", "300", "-o"])
        .arg(dest)
        .arg(url)
        .status()
        .map_err(|e| format!("failed to launch curl; install curl first: {e}"))?;
    Ok(status.success())
}

fn curl_text(url: &str) -> Result<Option<String>, String> {
    let output = Command::new("curl")
        .args(["-fL", "--retry", "2", "--connect-timeout", "20", "--max-time", "60", url])
        .output()
        .map_err(|e| format!("failed to launch curl; install curl first: {e}"))?;
    if !output.status.success() {
        return Ok(None);
    }
    Ok(Some(String::from_utf8_lossy(&output.stdout).to_string()))
}

fn command_output_first_word(cmd: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(cmd).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .map(|s| s.to_ascii_lowercase())
}

fn sha256_file(path: &Path) -> Result<Option<String>, String> {
    let p = path.to_string_lossy().to_string();
    if let Some(x) = command_output_first_word("sha256sum", &[&p]) {
        return Ok(Some(x));
    }
    if let Some(x) = command_output_first_word("shasum", &["-a", "256", &p]) {
        return Ok(Some(x));
    }
    Ok(None)
}

fn verify_checksum(zip_url: &str, zip_path: &Path) -> Result<bool, String> {
    let checksum_url = format!("{zip_url}.CHECKSUM");
    let Some(text) = curl_text(&checksum_url)? else {
        eprintln!("[WARN] checksum missing: {checksum_url}");
        return Ok(true);
    };
    let Some(expected) = text.split_whitespace().next() else {
        eprintln!("[WARN] checksum empty: {checksum_url}");
        return Ok(true);
    };
    let Some(actual) = sha256_file(zip_path)? else {
        eprintln!("[WARN] sha256sum/shasum not found; skip checksum verification");
        return Ok(true);
    };
    let expected = expected.trim().to_ascii_lowercase();
    if expected != actual {
        eprintln!("[FAIL] checksum mismatch: {zip_url}");
        eprintln!("       expected={expected}");
        eprintln!("       actual  ={actual}");
        return Ok(false);
    }
    Ok(true)
}

fn is_valid_symbol_dir(s: &str) -> bool {
    let n = s.len();
    if n < 3 || n > 40 {
        return false;
    }
    s.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}

fn add_symbol_candidate(set: &mut HashSet<String>, raw: &str) {
    let mut s = raw.trim().trim_matches('/').to_ascii_uppercase();
    if let Some(pos) = s.find('/') {
        s = s[..pos].to_string();
    }
    if is_valid_symbol_dir(&s) {
        set.insert(s);
    }
}

fn extract_href_targets(html: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = html;
    loop {
        let Some(i) = rest.find("href=") else { break; };
        rest = &rest[i + 5..];
        let bytes = rest.as_bytes();
        if bytes.is_empty() {
            break;
        }
        let quote = bytes[0] as char;
        if quote != '"' && quote != '\'' {
            continue;
        }
        let after = &rest[1..];
        let Some(j) = after.find(quote) else { break; };
        out.push(after[..j].to_string());
        rest = &after[j + 1..];
    }
    out
}

fn discover_all_symbols(max_symbols: usize) -> Result<Vec<String>, String> {
    let url = format!("{BASE_URL}/");
    let Some(html) = curl_text(&url)? else {
        return Err(format!("cannot fetch symbol index: {url}"));
    };

    let mut set: HashSet<String> = HashSet::new();

    for href in extract_href_targets(&html) {
        let href = href.trim();
        if href.ends_with('/') {
            let last = href.trim_end_matches('/').rsplit('/').next().unwrap_or(href);
            add_symbol_candidate(&mut set, last);
        }
        let marker = "daily/aggTrades/";
        if let Some(i) = href.find(marker) {
            let tail = &href[i + marker.len()..];
            add_symbol_candidate(&mut set, tail);
        }
        let marker2 = "daily%2FaggTrades%2F";
        if let Some(i) = href.find(marker2) {
            let tail = &href[i + marker2.len()..];
            let tail = tail.split('%').next().unwrap_or(tail);
            add_symbol_candidate(&mut set, tail);
        }
    }

    let marker = "/data/futures/um/daily/aggTrades/";
    let mut rest = html.as_str();
    while let Some(i) = rest.find(marker) {
        rest = &rest[i + marker.len()..];
        let token: String = rest.chars().take_while(|c| c.is_ascii_alphanumeric()).collect();
        add_symbol_candidate(&mut set, &token);
    }

    let mut symbols: Vec<String> = set.into_iter().collect();
    symbols.sort();
    if max_symbols > 0 && symbols.len() > max_symbols {
        symbols.truncate(max_symbols);
    }
    if symbols.is_empty() {
        return Err("all-assets found zero symbols; Binance directory format may have changed".to_string());
    }
    Ok(symbols)
}

fn select_symbols(args: &Args) -> Result<Vec<String>, String> {
    if args.all_assets {
        let symbols = discover_all_symbols(args.max_symbols)?;
        println!("[DISCOVER] all-assets symbols={}", symbols.len());
        return Ok(symbols);
    }
    if let Some(list) = &args.symbols {
        let mut out: Vec<String> = list
            .split(',')
            .map(|s| s.trim().to_ascii_uppercase())
            .filter(|s| !s.is_empty())
            .collect();
        out.sort();
        out.dedup();
        return Ok(out);
    }
    Ok(vec![args.symbol.trim().to_ascii_uppercase()])
}

fn is_header(cols: &[&str]) -> bool {
    if cols.is_empty() {
        return true;
    }
    let head = cols[0].trim().to_ascii_lowercase();
    head.starts_with("aggregate")
        || head == "agg_trade_id"
        || head == "aggtradeid"
        || head == "id"
        || head.parse::<i64>().is_err()
}

fn parse_aggtrade_row(symbol: &str, line: &str) -> Result<Option<AggTrade>, String> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(None);
    }
    let cols: Vec<&str> = line.split(',').collect();
    if is_header(&cols) {
        return Ok(None);
    }
    if cols.len() < 7 {
        return Err(format!("bad row length={} row={line:?}", cols.len()));
    }

    let a = cols[0].trim().parse::<i64>().map_err(|e| format!("bad a={:?}: {e}", cols[0]))?;
    let p = cols[1].trim().to_string();
    let q = cols[2].trim().to_string();
    let f = cols[3].trim().parse::<i64>().map_err(|e| format!("bad f={:?}: {e}", cols[3]))?;
    let l = cols[4].trim().parse::<i64>().map_err(|e| format!("bad l={:?}: {e}", cols[4]))?;
    let t = cols[5].trim().parse::<i64>().map_err(|e| format!("bad T={:?}: {e}", cols[5]))?;
    let m = parse_bool(cols[6])?;

    Ok(Some(AggTrade { s: symbol.to_string(), a, p, q, f, l, t, m }))
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn to_json_line(x: &AggTrade) -> String {
    format!(
        "{{\"s\":\"{}\",\"a\":{},\"p\":\"{}\",\"q\":\"{}\",\"f\":{},\"l\":{},\"T\":{},\"m\":{}}}",
        json_escape(&x.s),
        x.a,
        json_escape(&x.p),
        json_escape(&x.q),
        x.f,
        x.l,
        x.t,
        if x.m { "true" } else { "false" }
    )
}

fn extract_json_string(line: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\":\"");
    let start = line.find(&needle)? + needle.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn extract_json_i64(line: &str, key: &str) -> Option<i64> {
    let needle = format!("\"{key}\":");
    let start = line.find(&needle)? + needle.len();
    let rest = &line[start..];
    let mut end = 0usize;
    for (i, ch) in rest.char_indices() {
        if !(ch == '-' || ch.is_ascii_digit()) {
            break;
        }
        end = i + ch.len_utf8();
    }
    if end == 0 {
        return None;
    }
    rest[..end].parse::<i64>().ok()
}

fn load_seen_keys(out_path: &Path) -> HashSet<(String, i64)> {
    let mut seen = HashSet::new();
    let Ok(file) = File::open(out_path) else { return seen; };
    let reader = BufReader::new(file);
    for line in reader.lines().flatten() {
        let Some(s) = extract_json_string(&line, "s") else { continue; };
        let Some(a) = extract_json_i64(&line, "a") else { continue; };
        seen.insert((s, a));
    }
    seen
}

fn unzip_csv_stream(zip_path: &Path) -> Result<std::process::Child, String> {
    let child = Command::new("unzip")
        .arg("-p")
        .arg(zip_path)
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to launch unzip; install unzip first: {e}"))?;
    Ok(child)
}

fn process_zip(
    symbol: &str,
    zip_path: &Path,
    out: &mut File,
    seen: &mut HashSet<(String, i64)>,
    total: &mut usize,
    max_rows: usize,
) -> Result<(usize, bool), String> {
    let mut child = unzip_csv_stream(zip_path)?;
    let stdout = child.stdout.take().ok_or_else(|| "unzip stdout missing".to_string())?;
    let reader = BufReader::new(stdout);

    let mut day_count = 0usize;
    let mut hit_limit = false;

    for line in reader.lines() {
        let line = line.map_err(|e| format!("read unzip line failed: {e}"))?;
        let Some(item) = parse_aggtrade_row(symbol, &line)? else { continue; };
        let key = (item.s.clone(), item.a);
        if seen.contains(&key) {
            continue;
        }

        writeln!(out, "{}", to_json_line(&item)).map_err(|e| format!("write output failed: {e}"))?;
        seen.insert(key);
        *total += 1;
        day_count += 1;

        if max_rows > 0 && *total >= max_rows {
            hit_limit = true;
            break;
        }
    }

    let status = child.wait().map_err(|e| format!("unzip wait failed: {e}"))?;
    if !status.success() && !hit_limit {
        return Err(format!("unzip failed for {:?}: status={status}", zip_path));
    }
    Ok((day_count, hit_limit))
}

fn run() -> Result<(), String> {
    let args = parse_args()?;
    let symbols = select_symbols(&args)?;
    let days = date_range(&args.start, &args.end)?;

    println!("[CONFIG] mode={} start={} end={} out={:?}", if args.all_assets { "all-assets" } else { "selected" }, args.start, args.end, args.out);

    fs::create_dir_all(&args.tmp_dir).map_err(|e| format!("create tmp dir failed {:?}: {e}", args.tmp_dir))?;

    let append_mode = args.append || args.resume;
    let mut seen = if args.resume { load_seen_keys(&args.out) } else { HashSet::new() };

    let mut out_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append_mode)
        .truncate(!append_mode)
        .open(&args.out)
        .map_err(|e| format!("open output failed {:?}: {e}", args.out))?;

    let mut total = 0usize;
    let mut assets_ok = 0usize;
    let mut assets_empty = 0usize;

    for symbol in symbols {
        let mut symbol_rows = 0usize;
        for day in &days {
            let url = make_url(&symbol, day);
            let zip_path = args.tmp_dir.join(format!("tick-{symbol}-{day}-{}.zip", now_ms()));

            let ok = download_to_file(&url, &zip_path)?;
            if !ok {
                println!("[SKIP] not found/download failed: {symbol} {day}");
                let _ = fs::remove_file(&zip_path);
                continue;
            }

            if args.verify && !verify_checksum(&url, &zip_path)? {
                eprintln!("[SKIP] checksum failed: {symbol} {day}");
                let _ = fs::remove_file(&zip_path);
                continue;
            }

            let (day_count, hit_limit) = process_zip(&symbol, &zip_path, &mut out_file, &mut seen, &mut total, args.max_rows)?;
            symbol_rows += day_count;
            println!("[OK] {symbol} {day} rows={day_count}");

            if !args.keep_zip {
                let _ = fs::remove_file(&zip_path);
            }
            if hit_limit {
                println!("[DONE] max rows reached: {total}");
                return Ok(());
            }
        }
        if symbol_rows > 0 { assets_ok += 1; } else { assets_empty += 1; }
        println!("[ASSET] {symbol} total_rows={symbol_rows}");
    }

    println!("[DONE] wrote rows={total} file={:?} assets_ok={assets_ok} assets_empty={assets_empty}", args.out);
    Ok(())
}

fn main() {
    if let Err(e) = run() {
        eprintln!("[ERROR] {e}");
        print_usage();
        std::process::exit(1);
    }
}
