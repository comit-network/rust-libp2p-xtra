mod verify_peer_id;

pub use libp2p_core as libp2p;
use std::io;
use std::time::Duration;

use anyhow::Result;
use futures::stream::BoxStream;
use futures::{AsyncRead, AsyncWrite, StreamExt, TryStreamExt};
use libp2p_core::transport::timeout::TransportTimeout;
use libp2p_core::transport::{Boxed, ListenerEvent};
use libp2p_core::upgrade::Version;
use libp2p_core::{upgrade, Endpoint, Negotiated};
use libp2p_noise as noise;
use void::Void;
use yamux::Mode;

use crate::libp2p::identity::Keypair;
use crate::libp2p::Multiaddr;
use crate::libp2p::PeerId;
use crate::libp2p::Transport;
use crate::verify_peer_id::VerifyPeerId;

pub type Connection = (
    PeerId,
    Control,
    BoxStream<'static, Result<(Negotiated<yamux::Stream>, &'static str)>>,
);

#[derive(Clone)]
pub struct Node {
    inner: Boxed<Connection>,
}

impl Node {
    pub fn new<T>(
        transport: T,
        identity: Keypair,
        supported_inbound_protocols: Vec<&'static str>,
        upgrade_timeout: Duration,
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

        let protocols_negotiated = multiplexed.map(move |(peer, connection), _| {
            let control = Control {
                inner: connection.control(),
            };

            let incoming = yamux::into_stream(connection)
                .err_into::<anyhow::Error>()
                .and_then(move |stream| {
                    let supported_protocols = supported_inbound_protocols.clone();

                    async move {
                        let (protocol, stream) =
                            multistream_select::listener_select_proto(stream, &supported_protocols)
                                .await?;

                        anyhow::Ok((stream, *protocol))
                    }
                })
                .boxed();

            (peer, control, incoming)
        });

        let timeout_applied = TransportTimeout::new(protocols_negotiated, upgrade_timeout);

        Self {
            inner: timeout_applied.boxed(),
        }
    }

    pub fn listen_on(
        &self,
        address: Multiaddr,
    ) -> Result<BoxStream<'static, io::Result<Connection>>> {
        let stream = self
            .inner
            .clone()
            .listen_on(address)?
            .map_ok(|e| match e {
                ListenerEvent::NewAddress(_) => Ok(None),
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
}

impl Control {
    // TODO: This must have a timeout
    pub async fn open_substream(
        &mut self,
        protocol: &'static str, // TODO: Pass a list in here so we can negotiate different versions?
    ) -> Result<Negotiated<yamux::Stream>> {
        let stream = self.inner.open_stream().await?;

        let (negotiated_protocol, stream) =
            multistream_select::dialer_select_proto(stream, vec![protocol], Version::V1).await?;

        anyhow::ensure!(negotiated_protocol == protocol);

        Ok(stream)
    }
}
