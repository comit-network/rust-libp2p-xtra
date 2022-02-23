use anyhow::Context as _;
use anyhow::Result;
use async_trait::async_trait;
use asynchronous_codec::Bytes;
use futures::stream::BoxStream;
use futures::{AsyncRead, AsyncWrite, SinkExt, StreamExt, TryStreamExt};
use libp2p_core::identity::Keypair;
use libp2p_core::transport::MemoryTransport;
use libp2p_core::{Negotiated, PeerId, Transport};
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
    let mut tasks = Tasks::default();

    let (address, future) = HelloWorld::default().create(None).run();
    tasks.add(future);

    let listener = Listener::new(
        MemoryTransport::default(),
        alice_id,
        &[("/hello-world/1.0.0", &address)],
    );
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

impl xtra::Actor for HelloWorld {}

struct Listener {
    node: Node,
    tasks: Tasks,
    inbound_substream_channels:
        HashMap<&'static str, Box<dyn StrongMessageChannel<NewInboundSubstream>>>,
}

impl Listener {
    fn new<T>(
        transport: T,
        identity: Keypair,
        handlers: &[(
            &'static str,
            &impl StrongMessageChannel<NewInboundSubstream>,
        )],
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
                handlers.iter().map(|(proto, _)| *proto).collect(),
                Duration::from_secs(20),
            )
            .unwrap(),
            tasks: Default::default(),
            inbound_substream_channels: handlers
                .iter()
                .map(|(proto, channel)| (*proto, StrongMessageChannel::clone_channel(*channel)))
                .collect(),
        }
    }
}

#[xtra_productivity]
impl Listener {
    async fn handle(&mut self, msg: NewConnection, ctx: &mut Context<Self>) {
        let this = ctx.address().expect("we are alive");

        let NewConnection {
            peer,
            control,
            mut incoming_substreams,
        } = msg;

        self.tasks.add_fallible(
            {
                let this = this.clone();
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
                let _ = this.send(ListenerFailed { error }).await;
            },
        );
    }

    async fn handle(&mut self, msg: ListenerFailed) {}
}

#[async_trait]
impl xtra::Actor for Listener {
    async fn started(&mut self, ctx: &mut Context<Self>) {
        let this = ctx.address().expect("we just started");
        let mut stream = self
            .node
            .listen_on("/memory/10000".parse().unwrap())
            .unwrap();

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
}

struct ListenerFailed {
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
