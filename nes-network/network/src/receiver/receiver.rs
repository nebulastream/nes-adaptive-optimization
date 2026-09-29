/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

use crate::channel::Communication;
use crate::protocol::*;
use futures::future::Either;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info_span, warn};

use super::control::*;

/// Timeout for graceful tokio runtime shutdown
const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// A handle to the data queue of a registered channel.
///
/// Multiple handles can read from the same queue (see [`ReceiverChannel::new_handle`]), e.g. when a network source
/// is replaced during adaptive re-optimization while the channel itself stays registered.
pub struct ReceiverChannel {
    queue: async_channel::Receiver<TupleBuffer>,
    /// Interrupts receives on this handle only; other handles on the same queue are not affected.
    detached: CancellationToken,
}

pub enum ReceiverChannelResult {
    Ok(TupleBuffer),
    Closed,
    Error(Error),
}
impl ReceiverChannel {
    /// Closes the queue for all handles.
    pub fn close(&self) {
        self.queue.close();
    }

    /// Interrupts pending and future receives on this handle without closing the queue.
    pub fn detach(&self) {
        self.detached.cancel();
    }

    /// Creates another handle on the same queue with its own detach token.
    pub fn new_handle(&self) -> ReceiverChannel {
        ReceiverChannel {
            queue: self.queue.clone(),
            detached: CancellationToken::new(),
        }
    }

    /// Blocks until a buffer is available, the queue is closed, or this handle is detached.
    ///
    /// The receive future of `async_channel` only takes a buffer from the queue when it completes, so interrupting it
    /// by detaching never loses a buffer. `select` polls the detach token first, so a detached handle never takes
    /// another buffer.
    pub fn receive(&self) -> ReceiverChannelResult {
        let detached = std::pin::pin!(self.detached.cancelled());
        let recv = std::pin::pin!(self.queue.recv());
        match futures::executor::block_on(futures::future::select(detached, recv)) {
            Either::Left(_) => ReceiverChannelResult::Closed,
            Either::Right((Ok(buffer), _)) => ReceiverChannelResult::Ok(buffer),
            Either::Right((Err(_), _)) => ReceiverChannelResult::Closed,
        }
    }
}

pub struct NetworkService<C: Communication> {
    sender: NetworkingServiceController,
    runtime: Mutex<Option<Runtime>>,
    listener: PhantomData<C>,
}

pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;

impl<C: Communication + 'static> NetworkService<C> {
    pub fn start(
        runtime: Runtime,
        connection_addr: ThisConnectionIdentifier,
        communication: C,
    ) -> Arc<NetworkService<C>> {
        let (tx, rx) = async_channel::bounded(10);
        let service = Arc::new(NetworkService {
            sender: tx.clone(),
            runtime: Mutex::new(Some(runtime)),
            listener: Default::default(),
        });

        service
            .runtime
            .lock()
            .expect("BUG: No one should panic while holding this lock")
            .as_ref()
            .expect("BUG: The service was just started")
            .spawn(
                {
                    let listener = rx;
                    let controller = tx;
                    let connection_id = connection_addr.clone();
                    let communication = communication;
                    async move {
                        let control_socket_result = create_control_socket_handler(
                            listener,
                            controller,
                            connection_id,
                            communication,
                        )
                        .await;
                        match control_socket_result {
                            Ok(_) => {
                                warn!("Control stopped")
                            }
                            Err(e) => {
                                error!("Control stopped with error: {:?}", e);
                            }
                        }
                    }
                }
                .instrument(info_span!("receiver", this = %connection_addr)),
            );

        service
    }

    pub fn register_channel(
        self: &Arc<NetworkService<C>>,
        channel: ChannelIdentifier,
        data_queue_size: usize,
    ) -> Result<ReceiverChannel> {
        let (data_queue_sender, data_queue_receiver) = async_channel::bounded(data_queue_size);
        let (tx, rx) = oneshot::channel();
        let Ok(_) = self
            .sender
            .send_blocking(NetworkServiceControlCommand::RegisterChannel(
                channel,
                data_queue_sender,
                tx,
            ))
        else {
            return Err("Networking Service was stopped".into());
        };
        rx.blocking_recv()
            .map_err(|_| "Networking Service was stopped")?;
        Ok(ReceiverChannel {
            queue: data_queue_receiver,
            detached: CancellationToken::new(),
        })
    }

    pub fn shutdown(self: Arc<NetworkService<C>>) -> Result<()> {
        self.sender.close();
        let runtime = self
            .runtime
            .lock()
            .expect("BUG: No one should panic while holding this lock")
            .take()
            .ok_or("Networking Service was stopped")?;
        runtime.shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn buffer(sequence_number: u64) -> TupleBuffer {
        TupleBuffer {
            sequence_number,
            origin_id: 1,
            watermark: 0,
            chunk_number: 1,
            number_of_tuples: 0,
            last_chunk: true,
            data: vec![],
            child_buffers: vec![],
        }
    }

    fn channel() -> (async_channel::Sender<TupleBuffer>, ReceiverChannel) {
        let (sender, queue) = async_channel::bounded(8);
        (
            sender,
            ReceiverChannel {
                queue,
                detached: CancellationToken::new(),
            },
        )
    }

    fn sequence_number(result: ReceiverChannelResult) -> Option<u64> {
        match result {
            ReceiverChannelResult::Ok(buffer) => Some(buffer.sequence_number),
            _ => None,
        }
    }

    #[test]
    fn detached_handle_does_not_affect_other_handles() {
        let (sender, root) = channel();
        let old = Arc::new(root.new_handle());

        let receiving = {
            let old = old.clone();
            thread::spawn(move || matches!(old.receive(), ReceiverChannelResult::Closed))
        };
        // Give the old handle time to block in receive.
        thread::sleep(Duration::from_millis(50));
        old.detach();
        assert!(
            receiving.join().expect("receiving thread panicked"),
            "detached receive must return Closed"
        );

        // The queue is still open and the buffer sent after the detach goes to the new handle, not the detached one.
        sender
            .send_blocking(buffer(1))
            .expect("queue must still be open");
        assert!(matches!(old.receive(), ReceiverChannelResult::Closed));
        let new = root.new_handle();
        assert_eq!(sequence_number(new.receive()), Some(1));

        // Closing the queue ends the stream for all handles.
        root.close();
        assert!(matches!(new.receive(), ReceiverChannelResult::Closed));
    }
}
