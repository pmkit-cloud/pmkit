//! End-to-end coverage for the `pmkit-cloud-publish` binary.

use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    process::Command,
    thread,
};

use pmkit_core::{PortfolioId, RunId};
use pmkit_store::{OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, TapeStore, TursoTapeStore};
use serde_json::json;

#[tokio::test]
async fn cli_publishes_uploads_and_finalizes_against_local_cloud()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a sealed day with one materializable PM envelope and a local Cloud fixture.
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("pmkit.db");
    let manifest = directory.path().join("sealed-day.json");
    fs::write(
        &manifest,
        r#"{"version":2,"day":"1970-01-01","day_seal":"sealed"}"#,
    )?;
    let scope = OwnerScope::new(PortfolioId::new("research")?, RunId::new("bt")?);
    let store = TursoTapeStore::open_local(&database).await?;
    store
        .store_envelope(&PmEnvelope {
            schema_version: PM_ENVELOPE_VERSION,
            scope,
            venue_id: "polymarket".into(),
            config_hash: "config-sha256".into(),
            source_id: "market-channel".into(),
            connection_id: "connection-7".into(),
            source_timestamp_ms: 1_000,
            canonical_source_rank: 0,
            connection_epoch: 0,
            frame_sequence: 0,
            receipt_timestamp_ms: 1_001,
            ingest_sequence: 1,
            raw_frame: br#"{\"event_type\":\"price_change\",\"price\":\"0.42\"}"#.to_vec(),
            normalized: json!({"canonical_market_id": "token-1", "payload": {"price": "0.42"}}),
        })
        .await?;
    drop(store);

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = thread::spawn(move || -> Result<Vec<Vec<u8>>, std::io::Error> {
        let mut requests = Vec::new();
        for (status, body) in [
            (
                "201 Created",
                r#"{"release_id":"rel-1","status":"staging"}"#,
            ),
            ("200 OK", "{}"),
            ("200 OK", r#"{"release_id":"rel-1","status":"published"}"#),
        ] {
            let (mut stream, _) = listener.accept()?;
            let request = read_request(&mut stream)?;
            let response = format!(
                "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes())?;
            requests.push(request);
        }
        Ok(requests)
    });

    // When: the published binary runs against the local Cloud fixture.
    let output = Command::new(env!("CARGO_BIN_EXE_pmkit-cloud-publish"))
        .args([
            "--database",
            database.to_str().ok_or("database path")?,
            "--manifest",
            manifest.to_str().ok_or("manifest path")?,
            "--portfolio",
            "research",
            "--run",
            "bt",
            "--endpoint",
            &endpoint,
        ])
        .env("PMKIT_STORAGE_TOKEN", "storage-token")
        .output()?;
    let requests = server.join().map_err(|_| "fixture thread panicked")??;

    // Then: publication, artifact upload, and finalization reached the HTTP boundary.
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(requests.len(), 3);
    assert!(
        String::from_utf8_lossy(&requests[0])
            .starts_with("POST /internal/processor/bundles HTTP/1.1")
    );
    assert!(String::from_utf8_lossy(&requests[1]).starts_with("PUT /internal/processor/bundles/"));
    assert!(String::from_utf8_lossy(&requests[2]).contains("/finalize HTTP/1.1"));
    Ok(())
}

fn read_request(stream: &mut std::net::TcpStream) -> Result<Vec<u8>, std::io::Error> {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            return Ok(request);
        }
        request.extend_from_slice(&buffer[..read]);
        let Some(header_end) = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|position| position + 4)
        else {
            continue;
        };
        let content_length = String::from_utf8_lossy(&request[..header_end])
            .lines()
            .find_map(|line| line.split_once(':'))
            .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if request.len() >= header_end + content_length {
            return Ok(request);
        }
    }
}
