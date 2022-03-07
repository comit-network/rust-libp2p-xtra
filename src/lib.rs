pub use libp2p_core as libp2p;
pub use multistream_select::NegotiationError;

mod libp2p_stream;
mod multiaddress_ext;
mod verify_peer_id;

use anyhow::bail;
use anyhow::Context as _;
use anyhow::Result;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use futures::{AsyncRead, AsyncWrite};
use libp2p_core::identity::Keypair;
use libp2p_core::{Multiaddr, Negotiated, PeerId, Transport};
use libp2p_stream::Control;
use multiaddress_ext::MultiaddrExt as _;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use thiserror::Error;
use tokio_tasks::Tasks;
use xtra::message_channel::StrongMessageChannel;
use xtra::Context;
use xtra_productivity::xtra_productivity;

pub type Substream = Negotiated<yamux::Stream>;

/// An actor for managing multiplexed connections over a given transport.
///
/// The actor does not inflict any policy on connection and/or protocol management.
/// New connections can be established by sending a [`Connect`] messages. Existing connections can be disconnected by sending [`Disconnect`]. Listening for incoming connections is done by sending a [`ListenOn`] message.
/// To list the current state, send the [`GetConnectionStats`] message.
///
/// The combination of the above should make it possible to implement a fairly large number of policies. For example, to maintain a connection to a specific node, you can regularly check if the connection is still established by sending [`GetConnectionStats`] and react accordingly (f.e. sending [`Connect`] in case the connection has disappeared).
///
/// Once a connection with a peer is established, both sides can open substreams on top of the connection. Any incoming substream will - assuming the protocol is supported by the node - trigger a [`NewInboundSubstream`] message to the actor provided in the constructor.
/// Opening a new substream can be achieved by sending the [`OpenSubstream`] message.
pub struct Node {
    node: libp2p_stream::Node,
    tasks: Tasks,
    controls: HashMap<PeerId, (Control, Tasks)>,
    inbound_substream_channels:
        HashMap<&'static str, Box<dyn StrongMessageChannel<NewInboundSubstream>>>,
    listen_addresses: HashSet<Multiaddr>,
    inflight_connections: HashSet<PeerId>,
}

/// Open a substream to the provided peer.
///
/// Fails if we are not connected to the peer or the peer does not support the requested protocol.
pub struct OpenSubstream {
    pub peer: PeerId,
    pub protocol: &'static str,
}

/// Connect to the given [`Multiaddr`].
///
/// The address must contain a `/p2p` suffix.
/// Will fail if we are already connected to the peer.
pub struct Connect(pub Multiaddr);

/// Disconnect from the given peer.
pub struct Disconnect(pub PeerId);

/// Listen on the provided [`Multiaddr`].
///
/// For this to work, the [`Node`] needs to be constructed with a compatible transport.
/// In other words, you cannot listen on a `/memory` address if you haven't configured a `/memory` transport.
pub struct ListenOn(pub Multiaddr);

/// Retrieve [`ConnectionStats`] from the [`Node`].
pub struct GetConnectionStats;

pub struct ConnectionStats {
    pub connected_peers: HashSet<PeerId>,
    pub listen_addresses: HashSet<Multiaddr>,
}

/// Notifies an actor of a new, inbound substream from the given peer.
pub struct NewInboundSubstream {
    pub peer: PeerId,
    pub stream: libp2p_stream::Substream,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("No connection to {0}")]
    NoConnection(PeerId),
    #[error("Timeout in protocol negotiation")]
    NegotiationTimeoutReached,
    #[error("Failed to negotiate protocol")]
    NegotiationFailed(#[from] NegotiationError), // TODO(public-api): Consider breaking this up.
    #[error("Bad connection")]
    BadConnection(#[from] yamux::ConnectionError), // TODO(public-api): Consider removing this.
    #[error("Address {0} does not end with a peer ID")]
    NoPeerIdInAddress(Multiaddr),
    #[error("Either currently connecting or already connected to peer {0}")]
    AlreadyConnected(PeerId),
}

impl Node {
    /// Construct a new [`Node`] from the provided transport.
    ///
    /// A [`Node`]s identity ([`PeerId`]) will be computed from the given [`Keypair`].
    ///
    /// The `connection_timeout` is applied to:
    /// 1. Connection upgrades (i.e. noise handshake, yamux upgrade, etc)
    /// 2. Protocol negotiations
    ///
    /// The provided substream handlers are actors that will be given the fully-negotiated substreams whenever a peer opens a new substream for the provided protocol.
    pub fn new<T, const N: usize>(
        transport: T,
        identity: Keypair,
        connection_timeout: Duration,
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
                connection_timeout,
            ),
            tasks: Tasks::default(),
            inbound_substream_channels: inbound_substream_handlers.into_iter().collect(),
            controls: HashMap::default(),
            listen_addresses: HashSet::default(),
            inflight_connections: HashSet::default(),
        }
    }

    fn drop_connection(&mut self, peer: &PeerId) {
        let (control, tasks) = match self.controls.remove(&peer) {
            None => return,
            Some(control) => control,
        };

        // TODO: Evaluate whether dropping and closing has to be in a particular order.
        self.tasks.add(async move {
            control.close_connection().await;
            drop(tasks);
        });
    }
}

#[xtra_productivity]
impl Node {
    async fn handle(&mut self, msg: NewConnection, ctx: &mut Context<Self>) {
        self.inflight_connections.remove(&msg.peer);
        let this = ctx.address().expect("we are alive");

        let NewConnection {
            peer,
            control,
            mut incoming_substreams,
            worker,
        } = msg;

        let mut tasks = Tasks::default();
        tasks.add(worker);
        tasks.add_fallible(
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
                            Ok(Some(Ok((stream, protocol)))) => (stream, protocol),
                            Ok(Some(Err(libp2p_stream::Error::NegotiationTimeoutReached))) => {
                                tracing::debug!("Hit timeout while negotiating substream");
                                continue;
                            }
                            Ok(Some(Err(libp2p_stream::Error::NegotiationFailed(e)))) => {
                                tracing::debug!("Failed to negotiate substream: {}", e);
                                continue;
                            }
                            Ok(None) => bail!("Substream listener closed"),
                            Err(e) => bail!(e),
                        };

                        let channel = inbound_substream_channels
                            .get(&protocol)
                            .expect("Cannot negotiate a protocol that we don't support");

                        let _ = channel.do_send(NewInboundSubstream { peer, stream });
                    }
                }
            },
            move |error| async move {
                let _ = this.send(ConnectionFailed { peer, error }).await;
            },
        );
        self.controls.insert(peer, (control, tasks));
    }

    async fn handle(&mut self, msg: ListenerFailed) {
        tracing::debug!("Listener failed: {:#}", msg.error);

        self.listen_addresses.remove(&msg.address);
    }

    async fn handle(&mut self, msg: FailedToConnect) {
        tracing::debug!("Failed to connect: {:#}", msg.error);
        let peer = msg.peer;

        self.inflight_connections.remove(&peer);
        self.drop_connection(&peer);
    }

    async fn handle(&mut self, msg: ConnectionFailed) {
        tracing::debug!("Connection failed: {:#}", msg.error);
        let peer = msg.peer;

        self.drop_connection(&peer);
    }

    async fn handle(&mut self, _: GetConnectionStats) -> ConnectionStats {
        ConnectionStats {
            connected_peers: self.controls.keys().copied().collect(),
            listen_addresses: self.listen_addresses.clone(),
        }
    }

    async fn handle(&mut self, msg: Connect, ctx: &mut Context<Self>) -> Result<(), Error> {
        let this = ctx.address().expect("we are alive");

        let peer = msg
            .0
            .clone()
            .extract_peer_id()
            .ok_or_else(|| Error::NoPeerIdInAddress(msg.0.clone()))?;

        if self.inflight_connections.contains(&peer) || self.controls.contains_key(&peer){
            return Err(Error::AlreadyConnected(peer));
        }

        self.inflight_connections.insert(peer);
        self.tasks.add_fallible(
            {
                let node = self.node.clone();
                let this = this.clone();

                async move {
                    let (peer, control, incoming_substreams, worker) = node.connect(msg.0).await?;

                    let _ = this
                        .do_send_async(NewConnection {
                            peer,
                            control,
                            incoming_substreams,
                            worker,
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

    async fn handle(&mut self, msg: Disconnect) {
        self.drop_connection(&msg.0);
    }

    async fn handle(&mut self, msg: ListenOn, ctx: &mut Context<Self>) {
        let this = ctx.address().expect("we are alive");
        let listen_address = msg.0.clone();

        self.listen_addresses.insert(listen_address.clone()); // FIXME: This address could be a "catch-all" like "0.0.0.0" which actually results in listening on multiple interfaces.
        self.tasks.add_fallible(
            {
                let node = self.node.clone();
                let this = this.clone();

                async move {
                    let mut stream = node.listen_on(msg.0)?;

                    loop {
                        let (peer, control, incoming_substreams, worker) =
                            stream.try_next().await?.context("Listener closed")?;

                        this.do_send_async(NewConnection {
                            peer,
                            control,
                            incoming_substreams,
                            worker,
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

    async fn handle(&mut self, msg: OpenSubstream) -> Result<Substream, Error> {
        let peer = msg.peer;
        let protocol = msg.protocol;

        let (control, _) = self
            .controls
            .get_mut(&peer)
            .ok_or_else(|| Error::NoConnection(peer))?;

        let stream = control
            .open_substream(protocol)
            .await?
            .map_err(|e| match e {
                libp2p_stream::Error::NegotiationFailed(e) => Error::NegotiationFailed(e),
                libp2p_stream::Error::NegotiationTimeoutReached => Error::NegotiationTimeoutReached,
            })?;

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
    incoming_substreams: BoxStream<
        'static,
        Result<
            Result<(libp2p_stream::Substream, &'static str), libp2p_stream::Error>,
            yamux::ConnectionError,
        >,
    >,
    worker: BoxFuture<'static, ()>,
}

impl xtra::Message for NewInboundSubstream {
    type Result = ();
}
