use anyhow::Context as _;
use anyhow::Result;
use asynchronous_codec::Bytes;
use futures::{SinkExt, StreamExt};
use libp2p_stream::libp2p::identity::Keypair;
use libp2p_stream::libp2p::transport::MemoryTransport;
use libp2p_stream_xtra::{Connect, ListenOn, NewInboundSubstream, Node, OpenSubstream};
use tokio_tasks::Tasks;
use xtra::message_channel::StrongMessageChannel;
use xtra::spawn::TokioGlobalSpawnExt;
use xtra::Actor;
use xtra_productivity::xtra_productivity;

#[tokio::test]
async fn hello_world() {
    let alice_id = Keypair::generate_ed25519();
    let bob_id = Keypair::generate_ed25519();

    let hello_world_handler = HelloWorld::default().create(None).spawn_global();

    let alice = Node::new(
        MemoryTransport::default(),
        alice_id.clone(),
        [("/hello-world/1.0.0", hello_world_handler.clone_channel())],
    )
    .create(None)
    .spawn_global();

    alice
        .send(ListenOn {
            address: "/memory/10000".parse().unwrap(),
        })
        .await
        .unwrap();

    let bob = Node::new(MemoryTransport::default(), bob_id, [])
        .create(None)
        .spawn_global();

    bob.send(Connect {
        address: format!("/memory/10000/p2p/{}", alice_id.public().to_peer_id())
            .parse()
            .unwrap(),
    })
    .await
    .unwrap()
    .unwrap();

    let bob_to_alice = bob
        .send(OpenSubstream {
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
        tracing::info!("New hello world stream from {}", msg.peer);

        self.tasks
            .add_fallible(hello_world_listener(msg.stream), move |e| async move {
                tracing::warn!("Hello world protocol with peer {} failed: {}", msg.peer, e);
            });
    }
}

impl xtra::Actor for HelloWorld {}

async fn hello_world_dialer(
    stream: libp2p_stream::Substream,
    name: &'static str,
) -> Result<String> {
    let mut stream = asynchronous_codec::Framed::new(stream, asynchronous_codec::LengthCodec);

    stream.send(Bytes::from(name)).await?;
    let bytes = stream.next().await.context("Expected message")??;
    let message = String::from_utf8(bytes.to_vec())?;

    Ok(message)
}

async fn hello_world_listener(stream: libp2p_stream::Substream) -> Result<()> {
    let mut stream =
        asynchronous_codec::Framed::new(stream, asynchronous_codec::LengthCodec).fuse();

    let bytes = stream.select_next_some().await?;
    let name = String::from_utf8(bytes.to_vec())?;

    stream.send(Bytes::from(format!("Hello {name}!"))).await?;

    Ok(())
}