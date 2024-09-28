use futures::StreamExt;

use crate::picol::{block_on, spawn};

use crate::picol;

pub fn main() {
    block_on(async {
        let tcp_listener = picol::net::tcp_listener::TcpListener::bind("0.0.0.0:3456").unwrap();
        let mut accept_stream = tcp_listener.accept_multi();

        while let Some(result) = accept_stream.next().await {
            let tcp_stream = match result {
                Ok(tcp_stream) => tcp_stream,
                Err(error) => {
                    continue;
                },
            };
            println!("tcp_stream accepted");
            spawn(async move {
                loop {
                    let buf = vec![0u8; 1024];
                    let (read, buf) = tcp_stream.read(buf).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    println!("read {}", std::str::from_utf8(&buf).unwrap());
                    tcp_stream.write(buf).await.unwrap();
                }
            })
            .detach();
        }
    })
}
