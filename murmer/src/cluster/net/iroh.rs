//! iroh-backed implementations of the [`Net`](super::Net) seam's stream and
//! connection traits.
//!
//! Thin newtype wrappers over iroh's QUIC `SendStream` / `RecvStream` /
//! `Connection` — every method is a one-line delegation plus an error map. This
//! is the production side of the seam: [`super::super::transport::Transport`]
//! implements `Net` and constructs these wrappers when it opens iroh streams.

use async_trait::async_trait;
use iroh::endpoint::{Connection as QuicConnection, RecvStream, SendStream, VarInt};

use super::{Connection, NodeId, RecvHalf, SendHalf};
use crate::cluster::error::ClusterError;

/// Send half over an iroh QUIC `SendStream`.
pub struct IrohSend(pub SendStream);

#[async_trait]
impl SendHalf for IrohSend {
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), ClusterError> {
        self.0
            .write_all(buf)
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))
    }

    fn finish(&mut self) -> Result<(), ClusterError> {
        self.0
            .finish()
            .map_err(|e| ClusterError::Transport(e.to_string()))
    }
}

/// Recv half over an iroh QUIC `RecvStream`.
pub struct IrohRecv(pub RecvStream);

#[async_trait]
impl RecvHalf for IrohRecv {
    async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ClusterError> {
        self.0
            .read(buf)
            .await
            .map_err(|e| ClusterError::Transport(e.to_string()))
    }
}

/// Connection over an iroh QUIC `Connection`.
pub struct IrohConnection(pub QuicConnection);

#[async_trait]
impl Connection for IrohConnection {
    fn remote_id(&self) -> NodeId {
        NodeId(self.0.remote_id().to_string())
    }

    async fn open_bi(&self) -> Result<(Box<dyn SendHalf>, Box<dyn RecvHalf>), ClusterError> {
        let (send, recv) = self
            .0
            .open_bi()
            .await
            .map_err(|e| ClusterError::Connection(e.to_string()))?;
        Ok((Box::new(IrohSend(send)), Box::new(IrohRecv(recv))))
    }

    async fn accept_bi(&self) -> Option<(Box<dyn SendHalf>, Box<dyn RecvHalf>)> {
        match self.0.accept_bi().await {
            Ok((send, recv)) => Some((Box::new(IrohSend(send)), Box::new(IrohRecv(recv)))),
            // Err => connection closed; drives the accept loop's exit.
            Err(_) => None,
        }
    }

    fn close(&self, code: u32, reason: &[u8]) {
        self.0.close(VarInt::from_u32(code), reason);
    }
}
