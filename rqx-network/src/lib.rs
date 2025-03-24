use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{future, pin_mut, stream::TryStreamExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::Message;

type Tx = UnboundedSender<Message>;
type PeerMap = Arc<Mutex<HashMap<SocketAddr, Tx>>>;
fn handle_tcp(mut stream: TcpStream) {
    let mut buffer = [0; 512];

    // 循环读取客户端数据并返回
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                // 客户端关闭连接
                println!("Client disconnected");
                break;
            }
            Ok(n) => {
                // 打印收到的数据
                println!("Received: {}", String::from_utf8_lossy(&buffer[..n]));

                // 回应客户端
                if let Err(e) = stream.write_all(&buffer[..n]) {
                    eprintln!("Failed to send data to client: {}", e);
                    break;
                }
            }
            Err(e) => {
                eprintln!("Failed to read from stream: {}", e);
                break;
            }
        }
    }
}


async fn handle_websocket(peer_map: PeerMap, raw_stream: TcpStream, addr: SocketAddr) {
    println!("Incoming TCP connection from: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the websocket handshake occurred");
    println!("WebSocket connection established: {}", addr);

    // Insert the write part of this peer to the peer map.
    let (tx, rx) = unbounded();
    peer_map.lock().unwrap().insert(addr, tx);

    let (outgoing, incoming) = ws_stream.split();

    let broadcast_incoming = incoming.try_for_each(|msg| {
        println!("Received a message from {}: {}", addr, msg.to_text().unwrap());
        let peers = peer_map.lock().unwrap();

        // We want to broadcast the message to everyone except ourselves.
        let broadcast_recipients = peers
            .iter()
            .filter(|(peer_addr, _)| peer_addr != &&addr)
            .map(|(_, ws_sink)| ws_sink);

        for recp in broadcast_recipients {
            recp.unbounded_send(msg.clone()).unwrap();
        }

        future::ok(())
    });

    let receive_from_others = rx.map(Ok).forward(outgoing);

    pin_mut!(broadcast_incoming, receive_from_others);
    future::select(broadcast_incoming, receive_from_others).await;

    println!("{} disconnected", &addr);
    peer_map.lock().unwrap().remove(&addr);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{env, thread};

    #[test]
    fn it_works() {
        // 绑定到127.0.0.1:8080地址
        let listener = TcpListener::bind("127.0.0.1:8080").expect("Could not bind");

        println!("Server is running on 127.0.0.1:34567");

        // 循环接收客户端连接
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    // 为每个连接启动一个新线程
                    thread::spawn(move || {
                        handle_tcp(stream);
                    });
                }
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    #[tokio::test]
    async fn it_works2() -> Result<(), Box<dyn std::error::Error>> {
        let addr = env::args()
            .nth(1)
            .unwrap_or_else(|| "127.0.0.1:8080".to_string());

        let state = PeerMap::new(Mutex::new(HashMap::new()));

        // Create the event loop and TCP listener we'll accept connections on.
        let try_socket = TcpListener::bind(&addr).await;
        let listener = try_socket.expect("Failed to bind");
        println!("Listening on: {}", addr);

        // Let's spawn the handling of each connection in a separate task.
        while let Ok((stream, addr)) = listener.accept().await {
            tokio::spawn(handle_websocket(state.clone(), stream, addr));
        }

        Ok(())
    }
}
