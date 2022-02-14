use anyhow::Context;
use anyhow::Result;
use asynchronous_codec::Bytes;
use futures::{SinkExt, StreamExt};
use libp2p_core::transport::MemoryTransport;
use libp2p_stream::{Node, Substream};

#[tokio::test]
async fn hello_world() {
    env_logger::init();

    let alice = libp2p_stream::libp2p::identity::Keypair::generate_ed25519();
    let bob = libp2p_stream::libp2p::identity::Keypair::generate_ed25519();

    let alice = Node::new(
        MemoryTransport::default(),
        alice.clone(),
        vec!["/hello-world/1.0.0"],
    )
    .unwrap();
    let bob = Node::new(
        MemoryTransport::default(),
        bob.clone(),
        vec!["/hello-world/1.0.0"],
    )
    .unwrap();

    let mut alice_inc = alice
        .listen_on("/memory/10000".parse().unwrap())
        .unwrap()
        .fuse();

    let (alice_conn, bob_conn) = tokio::join!(
        alice_inc.select_next_some(),
        bob.connect("/memory/10000".parse().unwrap())
    );
    let alice_conn = alice_conn.unwrap();
    let bob_conn = bob_conn.unwrap();

    let (alice_hello_world, bob_hello_world) = tokio::join!(
        alice_conn.new_outbound_substream("/hello-world/1.0.0"),
        bob_conn.next_inbound_substream()
    );
    let alice_hello_world = alice_hello_world.unwrap();
    let (bob_hello_world, protocol) = bob_hello_world.unwrap();

    assert_eq!(protocol, "/hello-world/1.0.0");

    let alice_proto = hello_world_dialer(alice_hello_world, "Alice");
    let bob_proto = hello_world_listener(bob_hello_world);

    let (alice_out, bob_out) = tokio::join!(alice_proto, bob_proto);
    let alice_out = alice_out.unwrap();
    bob_out.unwrap();

    assert_eq!(&alice_out, "Hello Alice!")
}

async fn hello_world_dialer(stream: Substream, name: &'static str) -> Result<String> {
    let mut stream = asynchronous_codec::Framed::new(stream, asynchronous_codec::LengthCodec);

    stream.send(Bytes::from(name)).await?;
    let bytes = stream.next().await.context("Expected message")??;
    let message = String::from_utf8(bytes.to_vec())?;

    Ok(message)
}

async fn hello_world_listener(stream: Substream) -> Result<()> {
    let mut stream =
        asynchronous_codec::Framed::new(stream, asynchronous_codec::LengthCodec).fuse();

    let bytes = stream.select_next_some().await?;
    let name = String::from_utf8(bytes.to_vec())?;

    stream.send(Bytes::from(format!("Hello {name}!"))).await?;

    Ok(())
}
