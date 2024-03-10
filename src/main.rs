use std::collections::HashMap;
use std::io::{self, Cursor, Seek};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, Mutex};
use tokio::time;
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

struct State {
    tcp_tx: HashMap<SocketAddr, Tx>,
    udp_peers: HashMap<SocketAddr, UdpPeer>,
}

impl State {
    fn new() -> Self {
        Self {
            tcp_tx: HashMap::new(),
            udp_peers: HashMap::new(),
        }
    }

    async fn tcp_broadcast(&mut self, sender: SocketAddr, message: &Vec<u8>) {
        for p in self.tcp_tx.iter_mut() {
            if *p.0 != sender {
                p.1.send(message.clone()).unwrap();
            }
        }
    }

    async fn tcp_send(&mut self, send_to: SocketAddr, message: &Vec<u8>) {
        self.tcp_tx[&send_to].send(message.clone()).unwrap();
    }
}

struct TcpPeer {
    stream: TcpStream,
    addr: SocketAddr,
    rx: Rx,
    id: String,
    name: String,
    game: String,
    game_name: String,
    has_password: bool,
}

impl TcpPeer {
    async fn new(state: Arc<Mutex<State>>, stream: TcpStream) -> io::Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let addr = stream.peer_addr()?;
        state.lock().await.tcp_tx.insert(addr, tx);
        Ok(Self {
            rx,
            stream,
            addr,
            id: addr.to_string(),
            name: "".into(),
            game: "".into(),
            game_name: "".into(),
            has_password: false,
        })
    }
}

struct UdpPeer {
    addr: SocketAddr,
    id: String,
    game: String,
    room: u16,
    previous_room: u16,
    killed: bool,
    last_connection: SystemTime,
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
            last_connection: SystemTime::now(),
        }
    }
}

#[tokio::main]
async fn main() {
    // Shared state
    let state = Arc::new(Mutex::new(State::new()));

    // Tcp server
    let tcp_listener = TcpListener::bind("127.0.0.1:8002").await.unwrap();
    println!("Server sockets running at port 8002");
    let state_tcp = Arc::clone(&state);
    let tcp_server = tokio::spawn(async move {
        loop {
            let (stream, _) = tcp_listener.accept().await.unwrap();
            let state_tcp = Arc::clone(&state_tcp);
            tokio::spawn(async move {
                _ = process_tcp(state_tcp, stream).await;
            });
        }
    });

    // Udp server
    let mut udp_socket = UdpSocket::bind("127.0.0.1:8003").await.unwrap();
    println!("Server sockets UDP running at port 8003");
    let state_udp = Arc::clone(&state);
    let udp_server = tokio::spawn(async move {
        loop {
            let state_udp = Arc::clone(&state_udp);
            _ = process_udp(state_udp, &mut udp_socket).await;
        }
    });

    // Udp connection checker
    let state_udp_checker = Arc::clone(&state);
    let udp_connection_checker = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(120));
        loop {
            let state_udp_checker = Arc::clone(&state_udp_checker);
            check_udp_connections(state_udp_checker).await;
            interval.tick().await;
        }
    });

    _ = tokio::join!(tcp_server, udp_server, udp_connection_checker);
}

async fn process_tcp(state: Arc<Mutex<State>>, stream: TcpStream) -> io::Result<()> {
    let mut buf = [0u8; 1024];
    let mut peer = TcpPeer::new(Arc::clone(&state), stream).await?;

    // Send self-id
    let mut writer = Cursor::new(vec![]);
    writer.write_u8(6).await?;
    writer.write_string(&peer.id).await?;
    state
        .lock()
        .await
        .tcp_send(peer.addr, writer.get_ref())
        .await;

    loop {
        tokio::select! {
            Some(msg) = peer.rx.recv() => {
                // Receive broadcast message
                let mut writer = Cursor::new(vec![]);
                writer.write_u32_v(msg.len() as u32).await?;
                writer.write_all(&msg).await?;
                peer.stream.write(writer.get_ref()).await?;
            }
            result = peer.stream.read(&mut buf) => match result {
                Ok(n) => {
                    // Receive tcp message
                    if n == 0 {
                        // Closed
                        break;
                    }
                    let mut buf = Vec::<u8>::from(buf);
                    buf.resize(n, 0);
                    let reader = Cursor::new(buf);
                    if let Err(_) = parse_tcp_message(Arc::clone(&state), &mut peer, reader).await {
                        // Parse error
                        break;
                    }
                },
                _ => break,
            }
        }
    }

    state
        .lock()
        .await
        .tcp_tx
        .remove(&peer.stream.peer_addr().unwrap());

    Ok(())
}

async fn parse_tcp_message(
    state: Arc<Mutex<State>>,
    peer: &mut TcpPeer,
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
            state
                .lock()
                .await
                .tcp_broadcast(peer.stream.peer_addr().unwrap(), writer.get_ref())
                .await;
        }
        1 => {
            // Destroyed
            let mut writer = Cursor::new(vec![]);
            writer.write_u8(1).await?;
            writer.write_string(&peer.id).await?;
            state
                .lock()
                .await
                .tcp_broadcast(peer.stream.peer_addr().unwrap(), writer.get_ref())
                .await;
        }
        2 => {
            // Heart beat
        }
        3 => {
            // Name
            peer.name = reader.read_string().await?;
            peer.game = reader.read_string().await?;
            peer.game_name = reader.read_string().await?;
            let _version = reader.read_string().await?;
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
            state
                .lock()
                .await
                .tcp_broadcast(peer.stream.peer_addr().unwrap(), writer.get_ref())
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
            state
                .lock()
                .await
                .tcp_broadcast(peer.stream.peer_addr().unwrap(), writer.get_ref())
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

async fn process_udp(state: Arc<Mutex<State>>, udp_socket: &mut UdpSocket) -> io::Result<()> {
    let mut buf = [0u8; 1024];
    let (len, addr) = udp_socket.recv_from(&mut buf).await.unwrap();

    // Take exists peer, or make new one
    let mut peer = state
        .lock()
        .await
        .udp_peers
        .remove(&addr)
        .unwrap_or(UdpPeer::new(addr));
    peer.last_connection = SystemTime::now();

    // Parse udp message
    let mut buf = Vec::from(buf);
    buf.resize(len, 0);
    let mut reader = Cursor::new(buf);
    match reader.read_u8().await? {
        0 => {
            // Initialize connection
        }
        1 => {
            // Receive position
            peer.id = reader.read_string().await?;
            peer.game = reader.read_string().await?;
            peer.room = reader.read_u16_le().await?;

            // Broadcast to peers
            for (_, p) in state.lock().await.udp_peers.iter() {
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
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unsupported udp message",
            ));
        }
    }

    // Insert the peer back
    state.lock().await.udp_peers.insert(addr, peer);

    Ok(())
}

async fn check_udp_connections(state: Arc<Mutex<State>>) {
    let cur_time = SystemTime::now();
    for (addr, peer) in state.lock().await.udp_peers.iter() {
        if cur_time.duration_since(peer.last_connection).unwrap() > Duration::from_secs(120)
            || peer.killed
        {
            state.lock().await.udp_peers.remove(addr);
        }
    }
}
