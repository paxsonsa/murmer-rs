use std::collections::VecDeque;
use tokio::sync::mpsc;

const DEFAULT_QUEUE_CAPACITY: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum QoSLevel {
    Supervisor = 0, // Highest priority - system commands
    Internal = 1,   // Actor self-messages
    Normal = 2,     // Regular messages
}

pub(crate) struct PrioritizedMailbox<T> {
    queues: Vec<VecDeque<T>>,
    rx: mpsc::Receiver<(QoSLevel, T)>,
}

impl<T> PrioritizedMailbox<T> {
    pub fn new(rx: mpsc::Receiver<(QoSLevel, T)>) -> Self {
        let queues = (0..3)
            .map(|_| VecDeque::with_capacity(DEFAULT_QUEUE_CAPACITY))
            .collect();
        Self { queues, rx }
    }

    pub async fn recv(&mut self) -> Option<T> {
        loop {
            // First check priority queues
            for priority in 0..self.queues.len() {
                if let Some(msg) = self.queues[priority].pop_front() {
                    return Some(msg);
                }
            }

            // If no messages in queues, wait for new message
            match self.rx.recv().await {
                Some((priority, msg)) => {
                    let priority_idx = priority as usize;
                    self.queues[priority_idx].push_back(msg);
                }
                None => return None,
            }
        }
    }
}

pub(crate) struct MailboxSender<T> {
    tx: mpsc::Sender<(QoSLevel, T)>,
}

impl<T> MailboxSender<T> {
    pub fn new(tx: mpsc::Sender<(QoSLevel, T)>) -> Self {
        Self { tx }
    }

    pub async fn send(
        &self,
        qos: QoSLevel,
        msg: T,
    ) -> Result<(), mpsc::error::SendError<(QoSLevel, T)>> {
        self.tx.send((qos, msg)).await
    }

    pub fn send_blocking(
        &self,
        qos: QoSLevel,
        msg: T,
    ) -> Result<(), mpsc::error::SendError<(QoSLevel, T)>> {
        tokio::runtime::Handle::current().block_on(self.tx.send((qos, msg)))
    }
}

impl<T> Clone for MailboxSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}
