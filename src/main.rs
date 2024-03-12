use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::io::{self, Cursor, Seek};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{error, info};
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

#[derive(Debug, Clone)]
enum HttpCommand {
    UpdatePeer(TcpPeer),
    RemovePeer(TcpPeer),
}

struct HttpState {
    // (game_uid, addr)
    peers: HashMap<(String, SocketAddr), TcpPeer>,
}

impl HttpState {
    fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
enum TcpCommand {
    Broadcast {
        sender_addr: SocketAddr,
        game: String,
        message: Vec<u8>,
    },
    Send {
        to_addr: SocketAddr,
        message: Vec<u8>,
    },
}

#[derive(Debug, Clone)]
struct TcpPeer {
    addr: SocketAddr,
    id: String, // id == addr
    player_name: String,
    game_uid: String, // game_uid = md5<32 bytes>(unique_key) + password
    game_name: String,
    has_password: bool,
}

impl TcpPeer {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            id: addr.to_string(),
            player_name: "".into(),
            game_uid: "".into(),
            game_name: "".into(),
            has_password: false,
        }
    }
}

#[derive(Debug, Clone)]
struct UdpPeer {
    addr: SocketAddr,
    id: String, // id == tcp_peer.addr
    game_uid: String,
    room: u16,
    previous_room: u16,
    last_connection: SystemTime,
}

impl UdpPeer {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            id: "".into(),
            game_uid: "".into(),
            room: u16::MAX,
            previous_room: u16::MAX,
            last_connection: SystemTime::now(),
        }
    }
}

#[derive(Debug, Clone)]
enum UdpCommand {
    CheckConnections,
    Broadcast {
        sender_addr: SocketAddr,
        game: String,
        room: u16,
        previous_room: u16,
        message: Vec<u8>,
    },
    Close {
        tcp_peer_id: String,
    },
}

const HTTP_SERVER_PORT: u16 = 8001;
const TCP_SERVER_PORT: u16 = 8002;
const UDP_SERVER_PORT: u16 = 8003;

#[tokio::main]
async fn main() {
    // Initialize logger
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // Create channel
    let (http_tx, mut http_rx) = mpsc::channel::<HttpCommand>(16);
    let (tcp_tx, _) = broadcast::channel::<TcpCommand>(16);
    let (udp_tx, udp_rx) = mpsc::channel::<UdpCommand>(16);

    // Http server
    let http_listener = TcpListener::bind(format!("127.0.0.1:{}", HTTP_SERVER_PORT))
        .await
        .unwrap();
    info!("http server running at port {}", HTTP_SERVER_PORT);
    let http_server = tokio::spawn(async move {
        let state = Arc::new(Mutex::new(HttpState::new()));
        loop {
            tokio::select! {
                Some(cmd) = http_rx.recv() => {
                    match cmd {
                        HttpCommand::UpdatePeer(peer) => {
                            state
                                .lock()
                                .unwrap()
                                .peers
                                .insert((peer.game_uid.clone(), peer.addr), peer);
                        },
                        HttpCommand::RemovePeer(peer) => {
                            _ = state
                                .lock()
                                .unwrap()
                                .peers
                                .remove(&(peer.game_uid, peer.addr));
                        },
                    }
                }
                Ok((stream, _)) = http_listener.accept() => {
                    let state = Arc::clone(&state);
                    tokio::spawn(async move {
                        _ = process_http(stream, state).await;
                    });
                }
            }
        }
    });

    // Tcp server
    let tcp_listener = TcpListener::bind(format!("127.0.0.1:{}", TCP_SERVER_PORT))
        .await
        .unwrap();
    info!("tcp server running at port {}", TCP_SERVER_PORT);
    let http_tx_tcp = http_tx.clone();
    let udp_tx_tcp = udp_tx.clone();
    let tcp_server = tokio::spawn(async move {
        loop {
            let (stream, _) = tcp_listener.accept().await.unwrap();
            let tcp_tx = tcp_tx.clone();
            let tcp_rx = tcp_tx.subscribe();
            let http_tx_tcp = http_tx_tcp.clone();
            let udp_tx_tcp = udp_tx_tcp.clone();
            let peer = TcpPeer::new(stream.peer_addr().unwrap());
            tokio::spawn(async move {
                _ = process_tcp(stream, peer, tcp_tx, tcp_rx, udp_tx_tcp, http_tx_tcp).await;
            });
        }
    });

    // Udp server
    let udp_socket = UdpSocket::bind(format!("127.0.0.1:{}", UDP_SERVER_PORT))
        .await
        .unwrap();
    info!("udp server running at port {}", UDP_SERVER_PORT);
    let udp_tx_serv = udp_tx.clone();
    let udp_server = tokio::spawn(async move {
        _ = process_udp(udp_socket, udp_tx_serv, udp_rx).await;
    });

    // Udp connection checker
    let udp_tx_checker = udp_tx.clone();
    let udp_connection_checker = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(120));
        loop {
            udp_tx_checker
                .send(UdpCommand::CheckConnections)
                .await
                .unwrap();
            interval.tick().await;
        }
    });

    _ = tokio::join!(http_server, tcp_server, udp_server, udp_connection_checker);
}

async fn process_http(mut stream: TcpStream, state: Arc<Mutex<HttpState>>) -> io::Result<()> {
    let mut buf = [0; 1024];
    _ = stream.read(&mut buf).await?;

    let status = "HTTP/1.1 200 OK";

    #[derive(Serialize)]
    struct Content {
        games: Vec<Game>,
    }

    #[derive(Serialize)]
    struct Game {
        name: String,
        password: String,
        players: Vec<Player>,
    }

    #[derive(Serialize)]
    struct Player {
        name: String,
        ip: SocketAddr,
    }

    let mut games_map = HashMap::<String, Game>::new();
    for ((game_uid, addr), peer) in state.lock().unwrap().peers.iter() {
        let game = games_map.entry(game_uid.clone()).or_insert(Game {
            name: peer.game_name.clone(),
            password: if peer.game_uid.len() > 32 {
                peer.game_uid[32..].into()
            } else {
                "".into()
            },
            players: vec![],
        });
        game.players.push(Player {
            name: peer.player_name.clone(),
            ip: *addr,
        });
    }

    let content = json!({"games": games_map.values().collect::<Vec::<&Game>>()});
    let content_str = serde_json::to_string_pretty(&content).unwrap();
    let length = content_str.len();
    let response = format!("{status}\r\nContent-Length: {length}\r\n\r\n{content_str}");
    stream.write_all(response.as_bytes()).await?;

    Ok(())
}

async fn process_tcp(
    mut stream: TcpStream,
    mut peer: TcpPeer,
    tcp_tx: broadcast::Sender<TcpCommand>,
    mut tcp_rx: broadcast::Receiver<TcpCommand>,
    udp_tx: mpsc::Sender<UdpCommand>,
    http_tx: mpsc::Sender<HttpCommand>,
) -> io::Result<()> {
    let mut buf = [0u8; 1024];

    // Send self-id
    let mut writer = Cursor::new(vec![]);
    writer.write_u8(6).await?;
    writer.write_string(&peer.id).await?;
    tcp_tx
        .send(TcpCommand::Send {
            to_addr: peer.addr,
            message: writer.get_ref().clone(),
        })
        .unwrap();

    loop {
        tokio::select! {
            Ok(cmd) = tcp_rx.recv() => {
                match cmd {
                    TcpCommand::Broadcast {sender_addr, game, message} => {
                        if peer.addr != sender_addr && peer.game_uid == game {
                            let mut writer = Cursor::new(vec![]);
                            writer.write_u32_v(message.len() as u32).await?;
                            writer.write_all(&message).await?;
                            stream.write(writer.get_ref()).await?;
                        }
                    }
                    TcpCommand::Send {to_addr, message} => {
                        if peer.addr == to_addr {
                            let mut writer = Cursor::new(vec![]);
                            writer.write_u32_v(message.len() as u32).await?;
                            writer.write_all(&message).await?;
                            stream.write(writer.get_ref()).await?;
                        }
                    }
                }
            }
            result = stream.read(&mut buf) => match result {
                Ok(n) => {
                    // Receive tcp message
                    if n == 0 {
                        // Closed
                        break;
                    }
                    let mut buf = Vec::<u8>::from(buf);
                    buf.resize(n, 0);
                    let reader = Cursor::new(buf);
                    if let Err(_) = parse_tcp_message(&mut peer, reader, &tcp_tx).await {
                        error!("parse tcp message error on: {}", peer.addr);
                        break;
                    }

                    http_tx.send(HttpCommand::UpdatePeer(peer.clone()))
                        .await
                        .unwrap();
                },
                _ => break,
            }
        }
    }

    // Tcp connection closed.

    // Also close udp connection
    udp_tx
        .send(UdpCommand::Close {
            tcp_peer_id: peer.id.clone(),
        })
        .await
        .unwrap();

    // Remove related peer in http state
    http_tx
        .send(HttpCommand::RemovePeer(peer.clone()))
        .await
        .unwrap();

    // Broadcast destroy message
    let mut writer = Cursor::new(vec![]);
    writer.write_u8(1).await?;
    writer.write_string(&peer.id).await?;
    tcp_tx
        .send(TcpCommand::Broadcast {
            sender_addr: peer.addr,
            game: peer.game_uid,
            message: writer.get_ref().clone(),
        })
        .unwrap();

    Ok(())
}

async fn parse_tcp_message(
    peer: &mut TcpPeer,
    mut reader: Cursor<Vec<u8>>,
    tcp_tx: &broadcast::Sender<TcpCommand>,
) -> io::Result<()> {
    reader.read_u32_v().await?;
    match reader.read_u8().await? {
        0 => {
            // Created
            let mut writer = Cursor::new(vec![]);
            writer.write_u8(0).await?;
            writer.write_string(&peer.id).await?;
            writer.write_string(&peer.player_name).await?;
            tcp_tx
                .send(TcpCommand::Broadcast {
                    sender_addr: peer.addr,
                    game: peer.game_uid.clone(),
                    message: writer.get_ref().clone(),
                })
                .unwrap();
        }
        1 => {
            // Destroyed
            let mut writer = Cursor::new(vec![]);
            writer.write_u8(1).await?;
            writer.write_string(&peer.id).await?;
            tcp_tx
                .send(TcpCommand::Broadcast {
                    sender_addr: peer.addr,
                    game: peer.game_uid.clone(),
                    message: writer.get_ref().clone(),
                })
                .unwrap();
        }
        2 => {
            // Heart beat
        }
        3 => {
            // Name
            peer.player_name = reader.read_string().await?;
            peer.game_uid = reader.read_string().await?;
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
            tcp_tx
                .send(TcpCommand::Broadcast {
                    sender_addr: peer.addr,
                    game: peer.game_uid.clone(),
                    message: writer.get_ref().clone(),
                })
                .unwrap();
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
            writer.write_string(&peer.player_name).await?;
            writer.write_i32_le(x).await?;
            writer.write_f64_le(y).await?;
            writer.write_u16_le(room).await?;
            tcp_tx
                .send(TcpCommand::Broadcast {
                    sender_addr: peer.addr,
                    game: peer.game_uid.clone(),
                    message: writer.get_ref().clone(),
                })
                .unwrap();
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

async fn process_udp(
    udp_socket: UdpSocket,
    udp_tx: mpsc::Sender<UdpCommand>,
    mut udp_rx: mpsc::Receiver<UdpCommand>,
) -> io::Result<()> {
    let mut buf = [0u8; 1024];
    let mut peers = HashMap::<SocketAddr, UdpPeer>::new();

    loop {
        tokio::select! {
            Some(cmd) = udp_rx.recv() => {
                match cmd {
                    UdpCommand::CheckConnections => {
                        let cur_time = SystemTime::now();
                        peers.retain(|_, peer|
                            cur_time.duration_since(peer.last_connection).unwrap() <= Duration::from_secs(120)
                        );
                    },
                    UdpCommand::Broadcast {sender_addr, game, room, previous_room, message} => {
                        // Broadcast to peers
                        for (_, p) in peers.iter() {
                            if p.addr != sender_addr
                                && p.game_uid == game
                                && (p.room == room || p.previous_room == previous_room) {
                                udp_socket.send_to(&message, p.addr).await?;
                            }
                        }
                    },
                    UdpCommand::Close {tcp_peer_id} => {
                        peers.retain(|_, peer| peer.id != tcp_peer_id);
                    }
                }
            }
            Ok((len, addr)) = udp_socket.recv_from(&mut buf) => {
                let mut peer = peers.entry(addr).or_insert(UdpPeer::new(addr));
                peer.last_connection = SystemTime::now();

                // Parse udp message
                let mut buf = Vec::from(buf);
                buf.resize(len, 0);
                let reader = Cursor::new(buf);
                if let Err(_) = parse_udp_message(&mut peer, reader, &udp_tx).await {
                    error!("parse udp message error on: {}", peer.addr);
                    continue;
                }
            }
        }
    }
}

async fn parse_udp_message(
    peer: &mut UdpPeer,
    mut reader: Cursor<Vec<u8>>,
    tx: &mpsc::Sender<UdpCommand>,
) -> io::Result<()> {
    match reader.read_u8().await? {
        0 => {
            // Initialize connection
        }
        1 => {
            // Receive position
            peer.id = reader.read_string().await?;
            peer.game_uid = reader.read_string().await?;
            peer.room = reader.read_u16_le().await?;

            // Broadcast to peers
            tx.send(UdpCommand::Broadcast {
                sender_addr: peer.addr,
                game: peer.game_uid.clone(),
                room: peer.room,
                previous_room: peer.previous_room,
                message: reader.get_ref().clone(),
            })
            .await
            .unwrap();
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
