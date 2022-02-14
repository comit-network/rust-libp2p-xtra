pub use libp2p_core as libp2p;
use std::io;
use std::sync::Arc;

use anyhow::Result;
use futures::stream::BoxStream;
use futures::{AsyncRead, AsyncWrite, StreamExt, TryStreamExt};
use libp2p_core::muxing::StreamMuxerEvent;
use libp2p_core::transport::{Boxed, ListenerEvent};
use libp2p_core::upgrade::Version;
use libp2p_core::{Negotiated, StreamMuxer};
use libp2p_noise as noise;
use libp2p_noise::NoiseOutput;
use libp2p_yamux::{Incoming, Yamux, YamuxConfig};

use crate::libp2p::identity::Keypair;
use crate::libp2p::Multiaddr;
use crate::libp2p::PeerId;
use crate::libp2p::Transport;

pub struct Node<S> {
    inner: Boxed<Connection<S>>,
}

impl<S> Node<S>
where
    S: 'static,
{
    pub fn new<T>(
        transport: T,
        identity: Keypair,
        supported_protocols: Vec<&'static str>,
    ) -> Result<Self>
    where
        T: Transport<Output = S> + Clone + Send + Sync + 'static,
        T::Output: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        T::Error: Send + Sync,
        T::Listener: Send + 'static,
        T::Dial: Send + 'static,
        T::ListenerUpgrade: Send + 'static,
    {
        let identity = noise::Keypair::<noise::X25519Spec>::new().into_authentic(&identity)?;

        let stream = transport
            .upgrade(Version::V1)
            .authenticate(noise::NoiseConfig::xx(identity).into_authenticated())
            .multiplex(YamuxConfig::default());

        Ok(Self {
            inner: libp2p::transport::boxed::boxed(stream.map(|(peer, connection), _| {
                Connection {
                    peer,
                    inner: Arc::new(connection),
                    supported_protocols,
                }
            })),
        })
    }

    pub fn listen_on(
        &self,
        address: Multiaddr,
    ) -> Result<BoxStream<'static, io::Result<Connection<S>>>> {
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

    pub async fn connect(&self, address: Multiaddr) -> Result<Connection<S>> {
        let connection = self.inner.clone().dial(address)?.await?;

        Ok(connection)
    }
}

pub type Substream = Negotiated<yamux::Stream>;

pub struct Connection<S> {
    peer: PeerId,
    inner: Arc<Yamux<Incoming<Negotiated<NoiseOutput<Negotiated<S>>>>>>,
    supported_protocols: Vec<&'static str>,
}

impl<S> Clone for Connection<S> {
    fn clone(&self) -> Self {
        Self {
            peer: self.peer,
            inner: self.inner.clone(),
            supported_protocols: self.supported_protocols.clone(),
        }
    }
}

impl<S> Connection<S> {
    pub fn peer(&self) -> PeerId {
        self.peer
    }

    pub async fn new_outbound_substream(&self, protocol: &'static str) -> Result<Substream> {
        let mut token = self.inner.open_outbound();
        let stream =
            futures::future::poll_fn(|cx| self.inner.poll_outbound(cx, &mut token)).await?;
        let (negotiated_protocol, stream) =
            multistream_select::dialer_select_proto(stream, &self.supported_protocols, Version::V1)
                .await?;

        anyhow::ensure!(*negotiated_protocol == protocol);

        Ok(stream)
    }

    pub async fn next_inbound_substream(&self) -> Result<(Substream, &'static str)> {
        let stream = match futures::future::poll_fn(|cx| self.inner.poll_event(cx)).await? {
            StreamMuxerEvent::InboundSubstream(stream) => stream,
            StreamMuxerEvent::AddressChange(_) => panic!("never emitted as per docs"),
        };

        let (protocol, stream) =
            multistream_select::listener_select_proto(stream, &self.supported_protocols).await?;

        Ok((stream, *protocol))
    }
}
