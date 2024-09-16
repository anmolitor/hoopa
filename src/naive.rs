use std::{io::Write, net::TcpListener};

pub fn main() {
    let listener = TcpListener::bind(("127.0.0.1", 3456)).unwrap();
    loop {
        let (mut stream, addr) = listener.accept().unwrap();
        stream.write_all(RESPONSE_200_HTTP11.as_bytes()).unwrap();
    }
}

const RESPONSE_200_HTTP11: &str = "HTTP/1.1 200 OK\r\n\r\nHello World";
