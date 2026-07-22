use std::{
    collections::HashMap,
    fs,
    io::{BufRead as _, Cursor, Write as _},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
};

use chrono::NaiveDate;
use sha2::{Digest, Sha256};
use zip::{ZipWriter, write::SimpleFileOptions};

use crate::{BinanceArchiveLimits, CachePolicy, VerifiedBinanceArchiveCache};

pub(crate) const ROW: &str = "12345,68750.25,0.001,12345,12345,1710000000123,true,true\n";

pub(crate) fn cache(
    root: &TempRoot,
    max_bytes: u64,
    base_url: &str,
) -> VerifiedBinanceArchiveCache {
    VerifiedBinanceArchiveCache::new_for_test(
        root.path().to_path_buf(),
        CachePolicy::Bounded { max_bytes },
        BinanceArchiveLimits {
            transfer_bytes: 1_000_000,
            zip_bytes: 1_000_000,
            csv_bytes: 1_000_000,
        },
        base_url,
    )
}

pub(crate) fn date(day: u32) -> Result<NaiveDate, Box<dyn std::error::Error>> {
    NaiveDate::from_ymd_opt(2026, 1, day).ok_or_else(|| "invalid test date".into())
}

pub(crate) fn archive(csv: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer.start_file("BTCUSDT-aggTrades.csv", SimpleFileOptions::default())?;
    writer.write_all(csv.as_bytes())?;
    Ok(writer.finish()?.into_inner())
}

pub(crate) fn archive_path(root: &Path, date: NaiveDate) -> PathBuf {
    root.join(format!("BTCUSDT-aggTrades-{}.zip", date.format("%Y-%m-%d")))
}

pub(crate) fn write_cached(
    root: &Path,
    date: NaiveDate,
    archive: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(root)?;
    fs::write(archive_path(root, date), archive)?;
    fs::write(
        archive_path(root, date).with_extension("zip.sha256"),
        checksum(archive),
    )?;
    Ok(())
}

pub(crate) fn checksum(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) fn responses(
    date: NaiveDate,
    checksum: &str,
    archive: Vec<u8>,
) -> HashMap<String, Vec<u8>> {
    let file = format!("BTCUSDT-aggTrades-{}.zip", date.format("%Y-%m-%d"));
    HashMap::from([
        (
            format!("/BTCUSDT/{file}.CHECKSUM"),
            format!("{checksum}  {file}\n").into_bytes(),
        ),
        (format!("/BTCUSDT/{file}"), archive),
    ])
}

pub(crate) fn zip_count(root: &Path) -> Result<usize, std::io::Error> {
    fs::read_dir(root)?.try_fold(0, |count, entry| {
        let path = entry?.path();
        Ok(count + usize::from(path.extension().is_some_and(|extension| extension == "zip")))
    })
}

pub(crate) struct TempRoot(PathBuf);

impl TempRoot {
    pub(crate) fn new() -> Result<Self, std::io::Error> {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "pmkit-binance-cache-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path)?;
        Ok(Self(path))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub(crate) struct TestServer {
    pub(crate) url: String,
    calls: Arc<AtomicUsize>,
    thread: Option<JoinHandle<Result<(), std::io::Error>>>,
}

impl TestServer {
    pub(crate) fn new(
        responses: HashMap<String, Vec<u8>>,
        requests: usize,
    ) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let url = format!("http://{}", listener.local_addr()?);
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let thread = thread::spawn(move || {
            for _ in 0..requests {
                let (mut stream, _) = listener.accept()?;
                let path = request_path(&stream)?;
                let body = responses.get(&path).cloned().unwrap_or_default();
                let status = if body.is_empty() {
                    "404 Not Found"
                } else {
                    "200 OK"
                };
                stream.write_all(
                    format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )?;
                stream.write_all(&body)?;
                server_calls.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        });
        Ok(Self {
            url,
            calls,
            thread: Some(thread),
        })
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }

    pub(crate) fn join(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.thread
            .take()
            .ok_or("test server already joined")?
            .join()
            .map_err(|_| "test server panicked")??;
        Ok(())
    }
}

fn request_path(stream: &TcpStream) -> Result<String, std::io::Error> {
    let mut reader = std::io::BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let path = line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_owned();
    while {
        line.clear();
        reader.read_line(&mut line)? > 0 && line != "\r\n"
    } {}
    Ok(path)
}
