use std::{
    fmt::Write as _,
    io::{BufRead as _, Write as _},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

pub(super) struct Response {
    status: u16,
    body: Vec<u8>,
    headers: Vec<(String, String)>,
}

impl Response {
    pub(super) fn json(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.as_bytes().to_vec(),
            headers: vec![],
        }
    }

    pub(super) fn bytes(status: u16, body: Vec<u8>, encoded: &str, logical: &str) -> Self {
        Self {
            status,
            body,
            headers: vec![
                ("x-pmkit-encoded-sha256".into(), encoded.into()),
                ("x-pmkit-segment-sha256".into(), logical.into()),
            ],
        }
    }
}

pub(super) struct TestServer {
    pub(super) url: String,
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    thread: Option<thread::JoinHandle<std::io::Result<()>>>,
}

impl TestServer {
    pub(super) fn new(responses: Vec<Response>) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let url = format!("http://{}", listener.local_addr()?);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let thread = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept()?;
                captured
                    .lock()
                    .map_err(|_| std::io::Error::other("request lock"))?
                    .push(read_request(&stream)?);
                write_response(&mut stream, response)?;
                count.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        });
        Ok(Self {
            url,
            calls,
            requests,
            thread: Some(thread),
        })
    }

    pub(super) fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }

    pub(super) fn requests(&self) -> Vec<CapturedRequest> {
        self.requests
            .lock()
            .map_or_else(|_| Vec::new(), |requests| requests.clone())
    }

    pub(super) fn join(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.thread
            .take()
            .ok_or("server already joined")?
            .join()
            .map_err(|_| "server panicked")??;
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct CapturedRequest {
    pub(super) path: String,
    pub(super) authorization: Option<String>,
}

fn read_request(stream: &TcpStream) -> std::io::Result<CapturedRequest> {
    let mut reader = std::io::BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let path = line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_owned();
    let mut authorization = None;
    while {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if let Some(value) = line.strip_prefix("authorization: ") {
            authorization = Some(value.trim().to_owned());
        }
        read > 0 && line != "\r\n"
    } {}
    Ok(CapturedRequest {
        path,
        authorization,
    })
}

fn write_response(stream: &mut TcpStream, response: Response) -> std::io::Result<()> {
    let status = match response.status {
        200 => "200 OK",
        401 => "401 Unauthorized",
        403 => "403 Forbidden",
        409 => "409 Conflict",
        429 => "429 Too Many Requests",
        503 => "503 Service Unavailable",
        _ => "500 Internal Server Error",
    };
    let mut extra = String::new();
    for (name, value) in response.headers {
        write!(extra, "{name}: {value}\r\n").map_err(std::io::Error::other)?;
    }
    stream.write_all(
        format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n",
            response.body.len()
        )
        .as_bytes(),
    )?;
    stream.write_all(&response.body)
}
