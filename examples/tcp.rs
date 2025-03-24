use std::error::Error;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 创建一个 TCP 监听器，监听 127.0.0.1:8080 地址
    let listener = TcpListener::bind("127.0.0.1:34567").await?;
    println!("Server listening on 127.0.0.1:34567");

    loop {
        // 接受一个新的连接
        let (mut socket, _) = listener.accept().await?;

        // 处理连接
        tokio::spawn(async move {
            let (mut reader, mut writer) = socket.split();

            let mut buffer = [0; 1024];
            loop {
                // 读取客户端发送的数据
                let n = match reader.read(&mut buffer).await {
                    Ok(0) => break, // 客户端关闭了连接
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("Failed to read from socket: {}", e);
                        break;
                    }
                };

                // 将读取的数据转换为字符串并打印出来
                if let Ok(str_data) = String::from_utf8(buffer[..n].to_vec()) {
                    println!("Received: {}", str_data); // 打印读取到的数据
                } else {
                    eprintln!("Failed to convert data to string");
                }

                // 将接收到的数据返回给客户端
                if let Err(e) = writer.write_all(&buffer[..n]).await {
                    eprintln!("Failed to write to socket: {}", e);
                    break;
                }
            }

            println!("Connection closed");
        });
    }
}
