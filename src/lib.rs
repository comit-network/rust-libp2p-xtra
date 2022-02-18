pub use libp2p_core as libp2p;
use std::io;

use anyhow::{Context, Result};
use futures::stream::BoxStream;
use futures::{AsyncRead, AsyncWrite, FutureExt, StreamExt, TryStreamExt};
use futures::future::BoxFuture;
use libp2p_core::transport::{Boxed, ListenerEvent};
use libp2p_core::upgrade::Version;
use libp2p_core::{upgrade, Endpoint, Negotiated};
use libp2p_noise as noise;
use libp2p_noise::NoiseOutput;
use void::Void;
use yamux::Mode;

use crate::libp2p::identity::Keypair;
use crate::libp2p::Multiaddr;
use crate::libp2p::PeerId;
use crate::libp2p::Transport;

type Connection = (PeerId, Box<dyn Fn(&'static str) -> BoxFuture<'static, Result<Negotiated<yamux::Stream>>>>, BoxStream<'static, Result<(Negotiated<yamux::Stream>, &'static str)>>);

pub struct Node {
    inner: Boxed<Connection>,
}

impl Node
{
    pub fn new<T>(
        transport: T,
        identity: Keypair,
        supported_protocols: Vec<&'static str>,
    ) -> Result<Self>
    where
        T: Transport + Clone + Send + Sync + 'static,
        T::Output: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        T::Error: Send + Sync,
        T::Listener: Send + 'static,
        T::Dial: Send + 'static,
        T::ListenerUpgrade: Send + 'static,
    {
        let identity = noise::Keypair::<noise::X25519Spec>::new().into_authentic(&identity)?;

        let stream = transport
            .and_then(|conn, endpoint| {
                upgrade::apply(
                    conn,
                    noise::NoiseConfig::xx(identity).into_authenticated(),
                    endpoint,
                    Version::V1,
                )
            })
            .and_then(|(peer_id, conn), endpoint| {
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
            })
            .map(move |(peer, connection), _| {
                let control = connection.control();

                let new_stream = Box::new({
                    let supported_protocols = supported_protocols.clone();

                    move |protocol| {
                        let mut control = control.clone();
                        let supported_protocols = supported_protocols.clone();

                        async move {
                            let stream = control.open_stream().await?;

                            let (negotiated_protocol, stream) =
                                multistream_select::dialer_select_proto(stream, &supported_protocols, Version::V1).await?;

                            anyhow::ensure!(*negotiated_protocol == protocol);

                            anyhow::Ok(stream)
                        }.boxed()
                    }
                }) as Box<dyn Fn(&'static str) -> BoxFuture<'static, Result<Negotiated<yamux::Stream>>>>;

                let incoming = yamux::into_stream(connection).err_into::<anyhow::Error>().and_then(move |stream| {
                    let supported_protocols = supported_protocols.clone();

                    async move {
                        let (protocol, stream) =
                            multistream_select::listener_select_proto(stream, &supported_protocols).await?;

                        anyhow::Ok((stream, *protocol))
                    }
                }).boxed();

                (peer, new_stream, incoming)
            });

        Ok(Self {
            inner: libp2p::transport::boxed::boxed(stream),
        })
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
        let connection = self.inner.clone().dial(address)?.await?;

        Ok(connection)
    }
}

