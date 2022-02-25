use anyhow::bail;
use anyhow::Context as _;
use anyhow::Result;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use futures::{AsyncRead, AsyncWrite};
use libp2p_stream::libp2p::identity::Keypair;
use libp2p_stream::libp2p::{Multiaddr, PeerId, Transport};
use libp2p_stream::multiaddress_ext::MultiaddrExt;
use libp2p_stream::Control;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use thiserror::Error;
use tokio_tasks::Tasks;
use xtra::message_channel::StrongMessageChannel;
use xtra::Context;
use xtra_productivity::xtra_productivity;

// TODO:
// 1. Group tasks by connection so we can drop them
// 2. Provide a disconnect API
// 3. Write tests for `GetConnectionStats`
// 4. Think about load testing
// 5. Make timeouts configurable
// 6. Audit for deadlocks (always use async sending in message channels?)
// 7. Clean up inbound substream channels if disconnected? => No because it might be supervised and get reconnected again.
pub struct Node {
    node: libp2p_stream::Node,
    tasks: Tasks,
    controls: HashMap<PeerId, Control>,
    inbound_substream_channels:
        HashMap<&'static str, Box<dyn StrongMessageChannel<NewInboundSubstream>>>,
    listen_addresses: HashSet<Multiaddr>,
}

pub struct OpenSubstream {
    pub peer: PeerId,
    pub protocol: &'static str,
}

pub struct Connect {
    pub address: Multiaddr,
}

pub struct ListenOn {
    pub address: Multiaddr,
}

pub struct GetConnectionStats;

pub struct ConnectionStats {
    pub connected_peers: HashSet<PeerId>,
    pub listen_addresses: HashSet<Multiaddr>,
}

pub struct NewInboundSubstream {
    pub peer: PeerId,
    pub stream: libp2p_stream::Substream,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("No connection to {0}")]
    NoConnection(PeerId),
    #[error("Failed to open substream")]
    FailedToOpen(#[from] libp2p_stream::Error),
}

impl Node {
    pub fn new<T, const N: usize>(
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
            node: libp2p_stream::Node::new(
                transport,
                identity,
                inbound_substream_handlers
                    .iter()
                    .map(|(proto, _)| *proto)
                    .collect(),
                Duration::from_secs(20),
                Duration::from_secs(20),
            ),
            tasks: Tasks::default(),
            inbound_substream_channels: inbound_substream_handlers.into_iter().collect(),
            controls: HashMap::default(),
            listen_addresses: HashSet::default(),
        }
    }
}

#[xtra_productivity]
impl Node {
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
                        let (stream, protocol) = match incoming_substreams.try_next().await {
                            Ok(Some((stream, protocol))) => (stream, protocol),
                            Ok(None) => bail!("Substream listener closed"),
                            Err(libp2p_stream::Error::NegotiationTimeoutReached) => {
                                tracing::debug!("Hit timeout while negotiating substream");
                                continue;
                            }
                            Err(libp2p_stream::Error::NegotiationFailed(e)) => {
                                tracing::debug!("Failed to negotiate substream: {}", e);
                                continue;
                            }
                            Err(e) => bail!(e),
                        };

                        let channel = inbound_substream_channels
                            .get(&protocol)
                            .expect("Cannot negotiate a protocol that we don't support");

                        let _ = channel.send(NewInboundSubstream { peer, stream }).await;
                    }
                }
            },
            move |error| async move {
                let _ = this.send(ConnectionFailed { peer, error }).await;
            },
        );
        self.controls.insert(peer, control);
    }

    async fn handle(&mut self, msg: ListenerFailed) {
        tracing::debug!("Listener failed: {:#}", msg.error);

        self.listen_addresses.remove(&msg.address);
    }

    async fn handle(&mut self, msg: FailedToConnect) {
        tracing::debug!("Failed to connect: {:#}", msg.error);

        let control = match self.controls.remove(&msg.peer) {
            None => return,
            Some(control) => control,
        };

        self.tasks.add(control.close_connection());
    }

    async fn handle(&mut self, msg: ConnectionFailed) {
        tracing::debug!("Connection failed: {:#}", msg.error);

        let control = match self.controls.remove(&msg.peer) {
            None => return,
            Some(control) => control,
        };

        self.tasks.add(control.close_connection());
    }

    async fn handle(&mut self, _: GetConnectionStats) -> ConnectionStats {
        ConnectionStats {
            connected_peers: self.controls.keys().copied().collect(),
            listen_addresses: self.listen_addresses.clone(),
        }
    }

    async fn handle(&mut self, msg: Connect, ctx: &mut Context<Self>) -> Result<()> {
        let this = ctx.address().expect("we are alive");

        let peer = msg
            .address
            .clone()
            .extract_peer_id()
            .context("Failed to extract PeerId from address")?;

        self.tasks.add_fallible(
            {
                let node = self.node.clone();
                let this = this.clone();

                async move {
                    let (peer, control, incoming_substreams) = node.connect(msg.address).await?;

                    let _ = this
                        .send(NewConnection {
                            peer,
                            control,
                            incoming_substreams,
                        })
                        .await;

                    anyhow::Ok(())
                }
            },
            move |error| async move {
                let _ = this.send(FailedToConnect { peer, error }).await;
            },
        );

        Ok(())
    }

    async fn handle(&mut self, msg: ListenOn, ctx: &mut Context<Self>) {
        let this = ctx.address().expect("we are alive");
        let listen_address = msg.address.clone();

        self.listen_addresses.insert(listen_address.clone()); // FIXME: This address could be a "catch-all" like "0.0.0.0" which actually results in listening on multiple interfaces.
        self.tasks.add_fallible(
            {
                let node = self.node.clone();
                let this = this.clone();

                async move {
                    let mut stream = node.listen_on(msg.address)?;

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
                let _ = this
                    .send(ListenerFailed {
                        address: listen_address,
                        error,
                    })
                    .await;
            },
        );
    }

    async fn handle(&mut self, msg: OpenSubstream) -> Result<libp2p_stream::Substream, Error> {
        let peer = msg.peer;
        let protocol = msg.protocol;

        let stream = self
            .controls
            .get_mut(&peer)
            .ok_or_else(|| Error::NoConnection(peer))?
            .open_substream(protocol)
            .await?;

        Ok(stream)
    }
}

impl xtra::Actor for Node {}

struct ListenerFailed {
    address: Multiaddr,
    error: anyhow::Error,
}

struct FailedToConnect {
    peer: PeerId,
    error: anyhow::Error,
}

struct ConnectionFailed {
    peer: PeerId,
    error: anyhow::Error,
}

struct NewConnection {
    peer: PeerId,
    control: Control,
    incoming_substreams:
        BoxStream<'static, Result<(libp2p_stream::Substream, &'static str), libp2p_stream::Error>>,
}

impl xtra::Message for NewInboundSubstream {
    type Result = ();
}
