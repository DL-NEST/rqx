use futures_util::{sink::SinkExt, stream::StreamExt};
use std::net::SocketAddr;
use tokio_tungstenite::{tungstenite::protocol::Message, WebSocketStream};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 绑定 WebSocket 服务器地址
    let addr: SocketAddr = "127.0.0.1:34567".parse()?;
    let listener = TcpListener::bind(&addr).await?;
    println!("WebSocket server running on {}", addr);

    // 监听传入的 TCP 连接
    loop {
        let (stream, _) = listener.accept().await?;
        println!("New connection");

        // 为每个连接启动一个异步任务
        tokio::spawn(handle_connection(stream));
    }
}

async fn handle_connection(stream: tokio::net::TcpStream) {
    // 升级 TCP 连接为 WebSocket 连接
    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("Error during WebSocket handshake: {}", e);
            return;
        }
    };

    println!("WebSocket connection established");

    // 在 WebSocket 上接收和发送消息
    let (mut write, mut read) = ws_stream.split();

    while let Some(message) = read.next().await {
        match message {
            Ok(Message::Text(msg)) => {
                println!("Received: {}", msg);

                // 回应客户端相同的消息
                if write.send(Message::Text(msg)).await.is_err() {
                    eprintln!("Failed to send message to client");
                    break;
                }
            }
            Ok(Message::Binary(msg)) => {
                println!("Received binary data: {:?}", msg);

                // 回应客户端相同的二进制数据
                if write.send(Message::Binary(msg)).await.is_err() {
                    eprintln!("Failed to send binary data to client");
                    break;
                }
            }
            Err(e) => {
                eprintln!("Error while processing WebSocket message: {}", e);
                break;
            }
            _ => {}
        }
    }

    println!("Connection closed");
}
