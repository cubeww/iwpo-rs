use std::collections::HashMap;
use std::io::{self, Cursor, Seek};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, Mutex};
trait BufferExt {
    async fn read_u32_v(&mut self) -> Result<u32, io::Error>;
    async fn read_string(&mut self) -> Result<String, io::Error>;
    async fn write_u32_v(&mut self, x: u32) -> Result<(), io::Error>;
    async fn write_string(&mut self, s: &String) -> Result<(), io::Error>;
}

impl<T> BufferExt for T
where
    T: AsyncReadExt + AsyncWriteExt + Seek + Unpin,
{
    async fn read_u32_v(&mut self) -> Result<u32, io::Error> {
        let mut a = self.read_u8().await? as u32;
        self.seek(io::SeekFrom::Current(-1))?;
        if a & 1 > 0 {
            self.read_u8().await?;
            return Ok(a >> 1);
        } else if a & 2 > 0 {
            a = self.read_u16_le().await? as u32;
            return Ok((a >> 2) + 0x80);
        } else if a & 4 > 0 {
            self.read_u8().await?;
            a |= (self.read_u16_le().await? as u32) << 8;
            return Ok((a >> 3) + 0x4080);
        } else {
            a = self.read_u32_le().await?;
            return Ok((a >> 3) + 0x204080);
        }
    }
    async fn read_string(&mut self) -> Result<String, io::Error> {
        let mut result = String::new();
        loop {
            let c = self.read_u8().await?;
            if c == b'\0' {
                return Ok(result);
            }
            result.push(c as char);
        }
    }

    async fn write_u32_v(&mut self, x: u32) -> Result<(), io::Error> {
        if x < 0x80 {
            let a = (x << 1) | 1;
            self.write_u8(a as u8).await?;
        } else if x < 0x4080 {
            let a = ((x - 0x80) << 2) | 2;
            self.write_u16_le(a as u16).await?;
        } else if x < 0x204080 {
            let a = ((x - 0x4080) << 3) | 4;
            self.write_u8(a as u8).await?;
            self.write_u16_le((a >> 8) as u16).await?;
        } else {
            let a = (x - 0x204080) << 3;
            self.write_u32_le(a).await?;
        }
        Ok(())
    }
    async fn write_string(&mut self, s: &String) -> Result<(), io::Error> {
        for i in s.as_bytes() {
            self.write_u8(*i).await?;
        }
        self.write_u8(b'\0').await?;
        Ok(())
    }
}

type Tx = mpsc::UnboundedSender<Vec<u8>>;
type Rx = mpsc::UnboundedReceiver<Vec<u8>>;

struct TcpServer {
    peers: HashMap<SocketAddr, Tx>,
}

impl TcpServer {
    fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }
    async fn broadcast(&mut self, sender: SocketAddr, message: &Vec<u8>) {
        for p in self.peers.iter_mut() {
            if *p.0 != sender {
                p.1.send(message.clone()).unwrap();
            }
        }
    }
}

struct Peer {
    stream: TcpStream,
    rx: Rx,
    id: String,
    name: String,
    game: String,
    game_name: String,
    has_password: bool,
}

impl Peer {
    async fn new(server: Arc<Mutex<TcpServer>>, stream: TcpStream) -> io::Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        server.lock().await.peers.insert(stream.peer_addr()?, tx);
        Ok(Self {
            rx,
            stream,
            id: "".into(),
            name: "".into(),
            game: "".into(),
            game_name: "".into(),
            has_password: false,
        })
    }
}

struct UdpServer {
    peers: HashMap<SocketAddr, UdpPeer>,
}

impl UdpServer {
    fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }
}

struct UdpPeer {
    addr: SocketAddr,
    id: String,
    game: String,
    room: u16,
    previous_room: u16,
    killed: bool,
}

impl UdpPeer {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            id: "".into(),
            game: "".into(),
            room: u16::MAX,
            previous_room: u16::MAX,
            killed: false,
        }
    }
}

#[tokio::main]
async fn main() {
    let tcp_server = Arc::new(Mutex::new(TcpServer::new()));
    let tcp_listener = TcpListener::bind("127.0.0.1:8002").await.unwrap();

    let mut udp_server = UdpServer::new();
    let mut udp_socket = UdpSocket::bind("127.0.0.1:8003").await.unwrap();

    _ = tokio::join!(
        tokio::spawn(async move {
            // Tcp server
            loop {
                let (stream, _) = tcp_listener.accept().await.unwrap();
                let tcp_server = Arc::clone(&tcp_server);
                tokio::spawn(async move {
                    _ = process_tcp(tcp_server, stream).await;
                });
            }
        }),
        tokio::spawn(async move {
            // Udp server
            loop {
                process_udp(&mut udp_server, &mut udp_socket).await;
            }
        })
    );
}

async fn process_tcp(server: Arc<Mutex<TcpServer>>, stream: TcpStream) -> io::Result<()> {
    let mut buf = [0u8; 1024];
    let mut peer = Peer::new(Arc::clone(&server), stream).await?;
    loop {
        tokio::select! {
            Some(msg) = peer.rx.recv() => {
                let mut writer = Cursor::new(vec![]);
                writer.write_u32_v(msg.len() as u32).await?;
                writer.write_all(&msg).await?;
                peer.stream.write(writer.get_ref()).await?;
            }
            result = peer.stream.read(&mut buf) => match result {
                Ok(n) => {
                    if n == 0 {
                        break;
                    }
                    let mut buf = Vec::<u8>::from(buf);
                    buf.resize(n, 0);
                    let reader = Cursor::new(buf);
                    if let Err(_) = parse_tcp_message(Arc::clone(&server), &mut peer, reader).await {
                        // Parse error!
                        break;
                    }
                },
                _ => break,
            }
        }
    }

    server
        .lock()
        .await
        .peers
        .remove(&peer.stream.peer_addr().unwrap());

    Ok(())
}

async fn parse_tcp_message(
    server: Arc<Mutex<TcpServer>>,
    peer: &mut Peer,
    mut reader: Cursor<Vec<u8>>,
) -> io::Result<()> {
    reader.read_u32_v().await?;
    match reader.read_u8().await? {
        0 => {
            // Created
            let mut writer = Cursor::new(vec![]);
            writer.write_u8(0).await?;
            writer.write_string(&peer.id).await?;
            writer.write_string(&peer.name).await?;
            server
                .lock()
                .await
                .broadcast(peer.stream.peer_addr().unwrap(), writer.get_ref())
                .await;
        }
        1 => {
            // Destroyed
            let mut writer = Cursor::new(vec![]);
            writer.write_u8(1).await?;
            writer.write_string(&peer.id).await?;
            server
                .lock()
                .await
                .broadcast(peer.stream.peer_addr().unwrap(), writer.get_ref())
                .await;
        }
        2 => {
            // Heart beat
            // Do nothing
        }
        3 => {
            // Name
            peer.name = reader.read_string().await?;
            peer.game = reader.read_string().await?;
            peer.game_name = reader.read_string().await?;
            let version = reader.read_string().await?;
            peer.has_password = reader.read_u8().await? == 1;
            // TODO: version check
        }
        4 => {
            // Chat message
            let message = reader.read_string().await?;
            let mut writer = Cursor::new(vec![]);
            writer.write_u8(4).await?;
            writer.write_string(&peer.id).await?;
            writer.write_string(&message).await?;
            server
                .lock()
                .await
                .broadcast(peer.stream.peer_addr().unwrap(), writer.get_ref())
                .await;
        }
        5 => {
            // Save
            let gravity = reader.read_u8().await?;
            let x = reader.read_i32_le().await?;
            let y = reader.read_f64_le().await?;
            let room = reader.read_u16_le().await?;

            let mut writer = Cursor::new(vec![]);
            writer.write_u8(5).await?;
            writer.write_u8(gravity).await?;
            writer.write_string(&peer.name).await?;
            writer.write_i32_le(x).await?;
            writer.write_f64_le(y).await?;
            writer.write_u16_le(room).await?;
            server
                .lock()
                .await
                .broadcast(peer.stream.peer_addr().unwrap(), writer.get_ref())
                .await;
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unsupported tcp message",
            ));
        }
    }
    Ok(())
}

async fn process_udp(server: &mut UdpServer, udp_socket: &mut UdpSocket) -> io::Result<()> {
    let mut buf = [0u8; 1024];
    let (len, addr) = udp_socket.recv_from(&mut buf).await.unwrap();
    let mut buf = Vec::from(buf);
    buf.resize(len, 0);
    let mut reader = Cursor::new(buf);
    match reader.read_u8().await? {
        0 => {
            // Initialize connection
        }
        1 => {
            // Receive position
            let mut peer = server.peers.remove(&addr).unwrap_or(UdpPeer::new(addr));
            peer.id = reader.read_string().await?;
            peer.game = reader.read_string().await?;
            peer.room = reader.read_u16_le().await?;
            for (_, p) in server.peers.iter() {
                if p.id == "" {
                    continue;
                }
                if p.id == peer.id {
                    continue;
                }
                if p.game != peer.game {
                    continue;
                }
                if p.room != peer.previous_room {
                    continue;
                }
                if p.killed {
                    continue;
                }
                udp_socket.send_to(reader.get_ref(), p.addr).await?;
            }
            peer.previous_room = peer.room;
            server.peers.insert(addr, peer);
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unsupported udp message",
            ));
        }
    }

    Ok(())
}
