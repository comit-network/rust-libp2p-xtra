use anyhow::Context as _;
use anyhow::Result;
use asynchronous_codec::Bytes;
use futures::stream::BoxStream;
use futures::{AsyncRead, AsyncWrite, SinkExt, StreamExt, TryStreamExt};
use libp2p_core::identity::Keypair;
use libp2p_core::transport::MemoryTransport;
use libp2p_core::{Multiaddr, Negotiated, PeerId, Transport};
use libp2p_stream::{Control, Node};
use std::collections::HashMap;
use std::time::Duration;
use tokio_tasks::Tasks;
use xtra::message_channel::StrongMessageChannel;
use xtra::{Actor, Context};
use xtra_productivity::xtra_productivity;

#[tokio::test]
async fn actor_system() {
    let alice_id = libp2p_stream::libp2p::identity::Keypair::generate_ed25519();
    let bob_id = libp2p_stream::libp2p::identity::Keypair::generate_ed25519();
    let mut tasks = Tasks::default();

    let (hello_world_handler, future) = HelloWorld::default().create(None).run();
    tasks.add(future);

    let (alice, alice_fut) = Network::new(
        MemoryTransport::default(),
        alice_id.clone(),
        [("/hello-world/1.0.0", hello_world_handler.clone_channel())],
    )
    .create(None)
    .run();
    tasks.add(alice_fut);

    alice
        .send(ListenOn {
            address: "/memory/10000".parse().unwrap(),
        })
        .await
        .unwrap();

    let (bob, bob_fut) = Network::new(MemoryTransport::default(), bob_id, [])
        .create(None)
        .run();
    tasks.add(bob_fut);

    bob.send(Connect {
        address: format!("/memory/10000/p2p/{}", alice_id.public().to_peer_id())
            .parse()
            .unwrap(),
    })
    .await
    .unwrap();

    let bob_to_alice = bob
        .send(OpenSubstreamToPeer {
            peer: alice_id.public().to_peer_id(),
            protocol: "/hello-world/1.0.0",
        })
        .await
        .unwrap()
        .unwrap();

    let string = hello_world_dialer(bob_to_alice, "Bob").await.unwrap();

    assert_eq!(string, "Hello Bob!")
}

#[derive(Default)]
struct HelloWorld {
    tasks: Tasks,
}

#[xtra_productivity(message_impl = false)]
impl HelloWorld {
    async fn handle(&mut self, msg: NewInboundSubstream) {
        println!("New hello world stream from {}", msg.peer);

        self.tasks
            .add_fallible(hello_world_listener(msg.stream), move |e| async move {
                eprintln!("Hello world protocol with peer {} failed: {}", msg.peer, e);
            });
    }
}

async fn hello_world_dialer(
    stream: Negotiated<yamux::Stream>,
    name: &'static str,
) -> Result<String> {
    let mut stream = asynchronous_codec::Framed::new(stream, asynchronous_codec::LengthCodec);

    stream.send(Bytes::from(name)).await?;
    let bytes = stream.next().await.context("Expected message")??;
    let message = String::from_utf8(bytes.to_vec())?;

    Ok(message)
}

async fn hello_world_listener(stream: Negotiated<yamux::Stream>) -> Result<()> {
    let mut stream =
        asynchronous_codec::Framed::new(stream, asynchronous_codec::LengthCodec).fuse();

    let bytes = stream.select_next_some().await?;
    let name = String::from_utf8(bytes.to_vec())?;

    stream.send(Bytes::from(format!("Hello {name}!"))).await?;

    Ok(())
}

// BOILERPLATE BELOW THIS LINE

impl xtra::Actor for HelloWorld {}

struct Network {
    node: Node,
    tasks: Tasks,
    controls: HashMap<PeerId, Control>,
    inbound_substream_channels:
        HashMap<&'static str, Box<dyn StrongMessageChannel<NewInboundSubstream>>>,
}

struct OpenSubstreamToPeer {
    pub peer: PeerId,
    pub protocol: &'static str,
}

struct Connect {
    pub address: Multiaddr,
}

struct ListenOn {
    pub address: Multiaddr,
}

struct GetConnectionStats;

impl Network {
    fn new<T, const N: usize>(
        transport: T,
        identity: Keypair,
        inbound_substream_handlers: [(
            &'static str,
            Box<dyn StrongMessageChannel<NewInboundSubstream>>,
        ); N],
    ) -> Self
    where
        T: Transport + Clone + Send + Sync + 'static,
        T::Output: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        T::Error: Send + Sync,
        T::Listener: Send + 'static,
        T::Dial: Send + 'static,
        T::ListenerUpgrade: Send + 'static,
    {
        Self {
            node: Node::new(
                transport,
                identity,
                inbound_substream_handlers
                    .iter()
                    .map(|(proto, _)| *proto)
                    .collect(),
                Duration::from_secs(20),
            ),
            tasks: Tasks::default(),
            inbound_substream_channels: inbound_substream_handlers.into_iter().collect(),
            controls: HashMap::default(),
        }
    }
}

#[xtra_productivity]
impl Network {
    async fn handle(&mut self, msg: NewConnection, ctx: &mut Context<Self>) {
        let this = ctx.address().expect("we are alive");

        let NewConnection {
            peer,
            control,
            mut incoming_substreams,
        } = msg;

        self.tasks.add_fallible(
            {
                let inbound_substream_channels = self
                    .inbound_substream_channels
                    .iter()
                    .map(|(proto, channel)| {
                        (
                            proto.to_owned(),
                            StrongMessageChannel::clone_channel(channel.as_ref()),
                        )
                    })
                    .collect::<HashMap<_, _>>();

                async move {
                    loop {
                        let (stream, protocol) = incoming_substreams
                            .try_next()
                            .await? // TODO: This fails if we can't negotiate a protocol, must not abort the loop!!
                            .context("Substream listener closed")?;

                        let channel = inbound_substream_channels
                            .get(&protocol)
                            .expect("Cannot negotiate a protocol that we don't support");

                        let _ = channel.send(NewInboundSubstream { peer, stream }).await;
                    }
                }
            },
            move |error| async move {
                let _ = this.send(ListenerFailed { error }).await;
            },
        );
        self.controls.insert(peer, control);
    }

    async fn handle(&mut self, msg: ListenerFailed) {
        eprintln!("ListenerFailed: {:#}", msg.error)
    }

    async fn handle(&mut self, msg: ConnectionFailed) {
        eprintln!("ConnectionFailed: {:#}", msg.error)
    }

    async fn handle(&mut self, msg: Connect, ctx: &mut Context<Self>) {
        let this = ctx.address().expect("we are alive");

        let (peer, control, mut incoming_substreams) = match self.node.connect(msg.address).await {
            Ok(connection) => connection,
            Err(error) => {
                let _ = this.do_send_async(ConnectionFailed { error }).await;

                return;
            }
        };

        self.controls.insert(peer, control);
        self.tasks.add_fallible(
            {
                let inbound_substream_channels = self
                    .inbound_substream_channels
                    .iter()
                    .map(|(proto, channel)| {
                        (
                            proto.to_owned(),
                            StrongMessageChannel::clone_channel(channel.as_ref()),
                        )
                    })
                    .collect::<HashMap<_, _>>();

                async move {
                    loop {
                        let (stream, protocol) = incoming_substreams
                            .try_next()
                            .await?
                            .context("Substream listener closed")?;

                        let channel = inbound_substream_channels
                            .get(&protocol)
                            .expect("Cannot negotiate a protocol that we don't support");

                        let _ = channel.send(NewInboundSubstream { peer, stream }).await;
                    }
                }
            },
            move |error| async move {
                let _ = this.send(ConnectionFailed { error }).await;
            },
        );
    }

    async fn handle(&mut self, msg: ListenOn, ctx: &mut Context<Self>) {
        let this = ctx.address().expect("we are alive");

        let mut stream = match self.node.listen_on(msg.address) {
            Ok(stream) => stream,
            Err(error) => {
                let _ = this.do_send_async(ListenerFailed { error }).await;

                return;
            }
        };

        self.tasks.add_fallible(
            {
                let this = this.clone();

                async move {
                    loop {
                        let (peer, control, incoming_substreams) =
                            stream.try_next().await?.context("Listener closed")?;

                        this.send(NewConnection {
                            peer,
                            control,
                            incoming_substreams,
                        })
                        .await?;
                    }
                }
            },
            |error| async move {
                let _ = this.send(ListenerFailed { error }).await;
            },
        );
    }

    async fn handle(&mut self, msg: OpenSubstreamToPeer) -> Result<Negotiated<yamux::Stream>> {
        let peer = msg.peer;
        let protocol = msg.protocol;

        let stream = self
            .controls
            .get_mut(&peer)
            .with_context(|| format!("No connection to {peer}"))?
            .open_substream(protocol)
            .await?;

        Ok(stream)
    }
}

impl xtra::Actor for Network {}

struct ListenerFailed {
    error: anyhow::Error,
}

struct ConnectionFailed {
    error: anyhow::Error,
}

struct NewConnection {
    peer: PeerId,
    control: Control,
    incoming_substreams: BoxStream<'static, Result<(Negotiated<yamux::Stream>, &'static str)>>,
}

struct NewInboundSubstream {
    peer: PeerId,
    stream: Negotiated<yamux::Stream>,
}

impl xtra::Message for NewInboundSubstream {
    type Result = ();
}
