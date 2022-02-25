use anyhow::Context;
use anyhow::Result;
use asynchronous_codec::Bytes;
use futures::{SinkExt, Stream, StreamExt};
use libp2p_core::transport::MemoryTransport;
use libp2p_core::Negotiated;
use libp2p_stream::Node;
use std::time::Duration;

#[tokio::test]
async fn hello_world() {
    env_logger::init();

    let alice_id = libp2p_stream::libp2p::identity::Keypair::generate_ed25519();
    let bob_id = libp2p_stream::libp2p::identity::Keypair::generate_ed25519();

    let alice = Node::new(
        MemoryTransport::default(),
        alice_id.clone(),
        vec!["/hello-world/1.0.0"],
        Duration::from_secs(20),
        Duration::from_secs(10),
    );
    let bob = Node::new(
        MemoryTransport::default(),
        bob_id.clone(),
        vec!["/hello-world/1.0.0"],
        Duration::from_secs(20),
        Duration::from_secs(10),
    );

    let mut alice_inc = alice
        .listen_on("/memory/10000".parse().unwrap())
        .unwrap()
        .fuse();

    let (alice_conn, bob_conn) = tokio::join!(
        alice_inc.select_next_some(),
        bob.connect(
            format!("/memory/10000/p2p/{}", alice_id.public().to_peer_id())
                .parse()
                .unwrap()
        )
    );
    let (_, mut alice_control, alice_streams) = alice_conn.unwrap();
    let (_, _bob_control, bob_streams) = bob_conn.unwrap();

    tokio::spawn(substream_handler(alice_streams));
    tokio::spawn(substream_handler(bob_streams));

    let alice_hello_world = alice_control
        .open_substream("/hello-world/1.0.0")
        .await
        .unwrap();

    let alice_out = hello_world_dialer(alice_hello_world, "Alice")
        .await
        .unwrap();

    assert_eq!(&alice_out, "Hello Alice!")
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

async fn substream_handler(
    mut new_substreams: impl Stream<Item = Result<(Negotiated<yamux::Stream>, &'static str)>> + Unpin,
) {
    loop {
        let (stream, protocol) = new_substreams.next().await.unwrap().unwrap();

        match protocol {
            "/hello-world/1.0.0" => {
                tokio::spawn(async move {
                    hello_world_listener(stream).await.unwrap();
                });
            }
            _ => panic!("Unsupported protocol!"),
        }
    }
}
