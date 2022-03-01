use crate::verify_peer_id::VerifyPeerId;
use anyhow::Result;
use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{AsyncRead, AsyncWrite, FutureExt, SinkExt, StreamExt, TryStreamExt};
use libp2p_core::identity::Keypair;
use libp2p_core::transport::timeout::TransportTimeout;
use libp2p_core::transport::{Boxed, ListenerEvent};
use libp2p_core::upgrade::Version;
use libp2p_core::Multiaddr;
use libp2p_core::PeerId;
use libp2p_core::Transport;
use libp2p_core::{upgrade, Endpoint, Negotiated};
use libp2p_noise as noise;
use multistream_select::NegotiationError;
use std::io;
use std::time::Duration;
use thiserror::Error;
use void::Void;
use yamux::Mode;

pub type Substream = Negotiated<yamux::Stream>;

pub type Connection = (
    PeerId,
    Control,
    BoxStream<'static, Result<Result<(Substream, &'static str), Error>, yamux::ConnectionError>>,
    BoxFuture<'static, ()>,
);

// TODO: Inline this abstraction.
#[derive(Clone)]
pub struct Node {
    inner: Boxed<Connection>,
}

impl Node {
    pub fn new<T>(
        transport: T,
        identity: Keypair,
        supported_inbound_protocols: Vec<&'static str>,
        connection_timeout: Duration,
    ) -> Self
    where
        T: Transport + Clone + Send + Sync + 'static,
        T::Output: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        T::Error: Send + Sync,
        T::Listener: Send + 'static,
        T::Dial: Send + 'static,
        T::ListenerUpgrade: Send + 'static,
    {
        let identity = noise::Keypair::<noise::X25519Spec>::new()
            .into_authentic(&identity)
            .expect("ed25519 signing does not fail");

        let authenticated = transport.and_then(|conn, endpoint| {
            upgrade::apply(
                conn,
                noise::NoiseConfig::xx(identity).into_authenticated(),
                endpoint,
                Version::V1,
            )
        });

        let peer_id_verified = VerifyPeerId::new(authenticated);

        let multiplexed = peer_id_verified.and_then(|(peer_id, conn), endpoint| {
            upgrade::apply(
                conn,
                upgrade::from_fn::<_, _, _, _, _, Void>(
                    b"/yamux/1.0.0",
                    move |conn, endpoint| async move {
                        Ok(match endpoint {
                            Endpoint::Dialer => (
                                peer_id,
                                yamux::Connection::new(
                                    conn,
                                    yamux::Config::default(),
                                    Mode::Client,
                                ),
                            ),
                            Endpoint::Listener => (
                                peer_id,
                                yamux::Connection::new(
                                    conn,
                                    yamux::Config::default(),
                                    Mode::Server,
                                ),
                            ),
                        })
                    },
                ),
                endpoint,
                Version::V1,
            )
        });

        let protocols_negotiated = multiplexed.map(move |(peer, mut connection), _| {
            let control = Control {
                inner: connection.control(),
                connection_timeout,
            };

            let (mut sender, receiver) = mpsc::unbounded();

            let worker = async move {
                while let Ok(Some(stream)) = connection.next_stream().await {
                    let _ = sender.send(stream).await; // ignore error for now.
                }
            }
            .boxed();

            let incoming = receiver
                .then(move |stream| {
                    let supported_protocols = supported_inbound_protocols.clone();

                    async move {
                        let result = tokio::time::timeout(
                            connection_timeout,
                            multistream_select::listener_select_proto(stream, &supported_protocols),
                        )
                        .await;

                        match result {
                            Ok(Ok((protocol, stream))) => Ok(Ok((stream, *protocol))),
                            Ok(Err(e)) => Ok(Err(Error::NegotiationFailed(e))),
                            Err(_timeout) => Ok(Err(Error::NegotiationTimeoutReached)),
                        }
                    }
                })
                .boxed();

            (peer, control, incoming, worker)
        });

        let timeout_applied = TransportTimeout::new(protocols_negotiated, connection_timeout);

        Self {
            inner: timeout_applied.boxed(),
        }
    }

    // TODO: After inlining, create concept of `ListenerId` to properly track listeners?
    pub fn listen_on(
        &self,
        address: Multiaddr,
    ) -> Result<BoxStream<'static, io::Result<Connection>>> {
        let stream = self
            .inner
            .clone()
            .listen_on(address)?
            .map_ok(|e| match e {
                ListenerEvent::NewAddress(_) => Ok(None), // TODO: Should we map these as well? How do we otherwise track our listeners?
                ListenerEvent::Upgrade { upgrade, .. } => Ok(Some(upgrade)),
                ListenerEvent::AddressExpired(_) => Ok(None),
                ListenerEvent::Error(e) => Err(e),
            })
            .try_filter_map(|o| async move { o })
            .and_then(|upgrade| upgrade)
            .boxed();

        Ok(stream)
    }

    pub async fn connect(&self, address: Multiaddr) -> Result<Connection> {
        // TODO: Either assume `Multiaddr` ends with a `PeerId` or pass it in separately.

        let connection = self.inner.clone().dial(address)?.await?;

        Ok(connection)
    }
}

pub struct Control {
    inner: yamux::Control,
    connection_timeout: Duration,
}

impl Control {
    pub async fn open_substream(
        &mut self,
        protocol: &'static str, // TODO: Pass a list in here so we can negotiate different versions?
    ) -> Result<Result<Negotiated<yamux::Stream>, Error>, yamux::ConnectionError> {
        let stream = self.inner.open_stream().await?;

        let result = tokio::time::timeout(self.connection_timeout, async {
            let (_, stream) =
                multistream_select::dialer_select_proto(stream, vec![protocol], Version::V1)
                    .await?;

            Ok(stream)
        })
        .await;

        let stream = match result {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => return Ok(Err(Error::NegotiationFailed(e))),
            Err(_timeout) => return Ok(Err(Error::NegotiationTimeoutReached)),
        };

        Ok(Ok(stream))
    }

    pub async fn close_connection(mut self) {
        let _ = self.inner.close().await;
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Timeout in protocol negotiation")]
    NegotiationTimeoutReached,
    #[error("Failed to negotiate protocol")]
    NegotiationFailed(#[from] NegotiationError),
}
