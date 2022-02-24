use crate::Multiaddr;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use libp2p_core::multiaddr::Protocol;
use libp2p_core::transport::memory::Channel;
use libp2p_core::transport::{ListenerEvent, TransportError};
use libp2p_core::{PeerId, Transport};
use std::fmt;
use std::fmt::Debug;

#[derive(Clone)]
pub struct VerifyPeerId<TInner> {
    inner: TInner,
}

impl<TInner> VerifyPeerId<TInner> {
    pub fn new(inner: TInner) -> Self {
        Self { inner }
    }
}

impl<TInner, C> Transport for VerifyPeerId<TInner>
where
    TInner: Transport<Output = (PeerId, C)> + 'static,
    TInner::Dial: Send + 'static,
    TInner::Listener: Send + 'static,
    TInner::ListenerUpgrade: Send + 'static,
    TInner::Error: 'static,
    C: 'static,
{
    type Output = TInner::Output;
    type Error = Error<TInner::Error>;
    type Listener =
        BoxStream<'static, Result<ListenerEvent<Self::ListenerUpgrade, Self::Error>, Self::Error>>;
    type ListenerUpgrade = BoxFuture<'static, Result<Self::Output, Self::Error>>;
    type Dial = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn listen_on(self, addr: Multiaddr) -> Result<Self::Listener, TransportError<Self::Error>>
    where
        Self: Sized,
    {
        let listener = self
            .inner
            .listen_on(addr)
            .map_err(|e| e.map(Error::Inner))?
            .map_err(Error::Inner)
            .map_ok(|e| {
                e.map(|u| u.map_err(Error::Inner).boxed())
                    .map_err(Error::Inner)
            })
            .boxed();

        Ok(listener)
    }

    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>>
    where
        Self: Sized,
    {
        let expected_peer_id =
            extract_peer_id(addr.clone()).ok_or(TransportError::Other(Error::NoPeerId))?;

        let dial = self.inner.dial(addr).map_err(|e| e.map(Error::Inner))?;

        Ok(dial_and_verify_peer_id::<TInner, C>(dial, expected_peer_id).boxed())
    }

    fn dial_as_listener(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>>
    where
        Self: Sized,
    {
        let expected_peer_id =
            extract_peer_id(addr.clone()).ok_or(TransportError::Other(Error::NoPeerId))?;
        let dial = self
            .inner
            .dial_as_listener(addr)
            .map_err(|e| e.map(Error::Inner))?;

        Ok(dial_and_verify_peer_id::<TInner, C>(dial, expected_peer_id).boxed())
    }

    fn address_translation(&self, listen: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr> {
        self.inner.address_translation(listen, observed)
    }
}

async fn dial_and_verify_peer_id<T, C>(
    dial: T::Dial,
    expected_peer_id: PeerId,
) -> Result<(PeerId, C), Error<T::Error>>
where
    T: Transport<Output = (PeerId, C)>,
{
    let (actual_peer_id, conn) = dial.await.map_err(Error::Inner)?;

    if expected_peer_id != actual_peer_id {
        return Err(Error::PeerIdMismatch {
            actual: actual_peer_id,
            expected: expected_peer_id,
        });
    }

    Ok((actual_peer_id, conn))
}

#[derive(Debug)]
pub enum Error<T> {
    PeerIdMismatch { expected: PeerId, actual: PeerId },
    NoPeerId,
    Inner(T),
}

impl<T: fmt::Display> fmt::Display for Error<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::PeerIdMismatch { actual, expected } => {
                write!(f, "Peer ID mismatch, expected {expected} but got {actual}")
            }
            Error::Inner(_) => Ok(()),
            Error::NoPeerId => write!(f, "The given address does not contain a peer ID"),
        }
    }
}

impl<T> std::error::Error for Error<T>
where
    T: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::PeerIdMismatch { .. } => None,
            Error::NoPeerId => None,
            Error::Inner(inner) => Some(inner),
        }
    }
}

fn extract_peer_id(mut addr: Multiaddr) -> Option<PeerId> {
    let peer_id = match addr.pop()? {
        Protocol::P2p(hash) => PeerId::from_multihash(hash).ok()?,
        _ => return None,
    };

    Some(peer_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p_core::transport::MemoryTransport;
    use libp2p_core::ConnectedPoint;

    #[test]
    fn rejects_address_without_peer_id() {
        let transport = VerifyPeerId::new(MemoryTransport::default().map(simulate_auth_upgrade));

        let result = transport.dial("/memory/10000".parse().unwrap());

        assert!(matches!(
            result,
            Err(TransportError::Other(Error::NoPeerId))
        ))
    }

    #[tokio::test]
    async fn fails_dial_with_unexpected_peer_id() {
        let _alice = MemoryTransport::default()
            .map(simulate_auth_upgrade)
            .listen_on("/memory/10000".parse().unwrap())
            .unwrap();
        let bob = VerifyPeerId::new(MemoryTransport::default().map(simulate_auth_upgrade));

        let result = bob
            .dial(
                "/memory/10000/p2p/12D3KooWSLdEVWR1rjrnimdX3KwTvRT8uxNs8q7keREr6MsuizJ7" // Hardcoded PeerId will unlikely be equal to random one that is simulated in auth upgrade.
                    .parse()
                    .unwrap(),
            )
            .unwrap()
            .await;

        assert!(matches!(result, Err(Error::PeerIdMismatch { .. })))
    }

    // Mapping function for simulating an authentication upgrade in a transport.
    fn simulate_auth_upgrade(
        conn: Channel<Vec<u8>>,
        _: ConnectedPoint,
    ) -> (PeerId, Channel<Vec<u8>>) {
        (PeerId::random(), conn)
    }
}
