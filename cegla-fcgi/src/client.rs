//! Client-side FastCGI implementation.

use std::{
  collections::{HashMap, VecDeque},
  future::Future,
  pin::Pin,
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
  },
  task::{Context, Poll},
  time::{Duration, Instant},
};

use cegla::{client::convert_to_http_response, CgiIncoming};
use futures_util::{stream::FuturesUnordered, task::AtomicWaker, Sink, SinkExt, StreamExt};
use http_body::Body;
use http_body_util::BodyExt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, ReadHalf, WriteHalf};
use tokio_util::{
  bytes::Bytes,
  codec::{FramedRead, FramedWrite},
};

pub use cegla::client::CgiBuilder;

use crate::protocol::{
  codec::{Decoder, Encoder},
  constants::{ProtocolStatus, RecordType, Role, FCGI_KEEP_CONN, FCGI_MAX_REQS, FCGI_MPXS_CONNS, FCGI_NULL_REQUEST_ID},
  id_alloc::IdAllocator,
  name_value_pair::NameValuePair,
  record::Record,
};

/// An [`AsyncRead`] adapter that reassembles `FCGI_STDOUT` record content
/// chunks received over an [`async_channel`] into a byte stream suitable for
/// [`convert_to_http_response`].
///
/// A `None` sentinel on the channel signals end-of-stream (the server sent an
/// empty `FCGI_STDOUT` record or an `FCGI_END_REQUEST` record).  A channel
/// that closes without it means the connection ended before the stream did,
/// and reading fails with [`std::io::ErrorKind::UnexpectedEof`].
pub struct FcgiResponseReader {
  // Incoming chunks of stdout data.  `None` means EOF.
  // Boxed + pinned so that the struct itself is Unpin.
  rx: Pin<Box<async_channel::Receiver<Option<Vec<u8>>>>>,
  // Leftover bytes from the last chunk that haven't been consumed yet.
  leftover: Option<(Vec<u8>, usize)>,
  // Whether the end-of-stream sentinel has arrived.
  ended: bool,
  // For the stdout reader: the connection to wake when the reader is
  // dropped, so it can abort a request whose response nobody reads.
  wake_on_drop: Option<Arc<Shared>>,
  // For the stdout reader: set when the connection gave up the response,
  // because more of it waited than `max_unread`.
  given_up: Option<Arc<AtomicBool>>,
}

impl Drop for FcgiResponseReader {
  fn drop(&mut self) {
    if let Some(shared) = self.wake_on_drop.take() {
      // Close first, so the connection sees the reader gone when it wakes.
      self.rx.close();
      shared.waker.wake();
    }
  }
}

// SAFETY: FcgiResponseReader only contains a pinned box (heap-allocated) and
// plain data.  The box makes the receiver address-stable, so moving
// FcgiResponseReader is safe.
impl Unpin for FcgiResponseReader {}

impl AsyncRead for FcgiResponseReader {
  fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
    loop {
      // Drain any leftover bytes first.
      if let Some((ref data, ref mut offset)) = self.leftover {
        let remaining = &data[*offset..];
        let to_copy = remaining.len().min(buf.remaining());
        buf.put_slice(&remaining[..to_copy]);
        *offset += to_copy;
        if *offset >= data.len() {
          self.leftover = None;
        }
        return Poll::Ready(Ok(()));
      }

      // Ask the channel for the next chunk.
      match self.rx.as_mut().poll_next_unpin(cx) {
        Poll::Ready(Some(Some(chunk))) => {
          if chunk.is_empty() {
            continue;
          }
          let to_copy = chunk.len().min(buf.remaining());
          buf.put_slice(&chunk[..to_copy]);
          if to_copy < chunk.len() {
            self.leftover = Some((chunk, to_copy));
          }
          return Poll::Ready(Ok(()));
        }
        // EOF sentinel.
        Poll::Ready(Some(None)) => {
          self.ended = true;
          return Poll::Ready(Ok(()));
        }
        // Closed: after the sentinel the stream has ended, without it the
        // connection ended first.
        Poll::Ready(None) => {
          if self.ended {
            return Poll::Ready(Ok(()));
          }
          if let Some(given_up) = self.given_up.as_ref() {
            if given_up.load(Ordering::Acquire) {
              return Poll::Ready(Err(std::io::Error::other(ResponseUnread)));
            }
          }
          return Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "the FastCGI connection ended before the response did",
          )));
        }
        Poll::Pending => return Poll::Pending,
      }
    }
  }
}

// ---------------------------------------------------------------------------
// PendingRequest
// ---------------------------------------------------------------------------

/// State kept for every in-flight FastCGI request.
struct PendingRequest<B> {
  // The HTTP request body (needed to stream FCGI_STDIN records).
  // Boxed + pinned so that the body is polled through a stable address.
  body: Option<Pin<Box<B>>>,
  // Channel used to feed `FCGI_STDOUT` bytes to [`FcgiResponseReader`].
  stdout_tx: async_channel::Sender<Option<Vec<u8>>>,
  // Channel used to feed `FCGI_STDERR` bytes to [`FcgiResponseReader`].
  stderr_tx: async_channel::Sender<Option<Vec<u8>>>,
  // Whether the `FCGI_END_REQUEST` record has been received.
  end_received: bool,
  // Whether the empty `FCGI_STDOUT` record has been received.
  stdout_ended: bool,
  // When `FCGI_ABORT_REQUEST` was sent, if it was.
  aborted: Option<Instant>,
  // Futures that convert the accumulated stdout into an HTTP response and
  // deliver it through the oneshot channel.
  response_futures: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
  // The channel for the response, until a response future or a rejection
  // takes it.
  res_ch: Arc<std::sync::Mutex<Option<oneshot::Sender<ResponseResult>>>>,
  // On a connection that multiplexes: STDOUT that waits for room in the
  // reader's channel, in order, so that the connection reads on for the
  // other requests; `None` for its end.
  unread: VecDeque<Option<Vec<u8>>>,
  // The bytes in `unread`.
  unread_bytes: usize,
  // Sends the first of `unread` once the reader has room.
  deliver: Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
  // Shared with the stdout reader: set when the response was given up.
  given_up: Arc<AtomicBool>,
}

impl<B> PendingRequest<B> {
  /// Passes `chunk` of STDOUT, or its end, on to the reader behind what
  /// waits already; gives the response up when more than `max_unread` bytes
  /// wait.
  fn pass_stdout(&mut self, chunk: Option<Vec<u8>>, max_unread: usize) {
    if self.stdout_tx.is_closed() {
      // No reader, or given up: nobody reads it.
      return;
    }
    let chunk = if self.unread.is_empty() && self.deliver.is_none() {
      match self.stdout_tx.try_send(chunk) {
        Ok(()) => return,
        // The reader went just now.
        Err(async_channel::TrySendError::Closed(gone)) => {
          drop(gone);
          return;
        }
        Err(async_channel::TrySendError::Full(chunk)) => chunk,
      }
    } else {
      chunk
    };
    self.unread_bytes += chunk.as_ref().map_or(0, Vec::len);
    self.unread.push_back(chunk);
    if self.unread_bytes > max_unread {
      self.give_up();
    }
  }

  /// Gives up the response: what waits is dropped, and the reader fails
  /// with [`ResponseUnread`]. Its closed channel also makes the connection
  /// abort the request.
  fn give_up(&mut self) {
    self.unread.clear();
    self.unread_bytes = 0;
    self.deliver = None;
    self.given_up.store(true, Ordering::Release);
    if self.stdout_tx.close() {
      // Closed now.
    } else {
      // The reader was gone already.
    }
  }

  /// Moves waiting STDOUT on to the reader as it makes room; whether none
  /// waits any more.
  fn deliver_unread(&mut self, cx: &mut Context<'_>) -> bool {
    loop {
      if let Some(deliver) = self.deliver.as_mut() {
        match deliver.as_mut().poll(cx) {
          Poll::Ready(()) => self.deliver = None,
          Poll::Pending => return false,
        }
      }
      let Some(chunk) = self.unread.pop_front() else {
        return true;
      };
      self.unread_bytes -= chunk.as_ref().map_or(0, Vec::len);
      let tx = self.stdout_tx.clone();
      self.deliver = Some(Box::pin(async move {
        if let Err(gone) = tx.send(chunk).await {
          // The reader went: nobody reads it.
          drop(gone);
        }
      }));
    }
  }
}

/// The response to a request and the reader for its stderr.
type ResponseResult = Result<
  (
    http::Response<CgiIncoming<cegla::client::CgiResponseInner<FcgiResponseReader>>>,
    FcgiResponseReader,
  ),
  std::io::Error,
>;

/// The FastCGI server rejected a request: its `FCGI_END_REQUEST` record had
/// the protocol status `FCGI_CANT_MPX_CONN`, `FCGI_OVERLOADED` or
/// `FCGI_UNKNOWN_ROLE`.
///
/// [`SendRequest::send_request`] returns it inside a [`std::io::Error`]; get
/// it with [`std::io::Error::get_ref`] and `downcast_ref`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestRejected(pub ProtocolStatus);

impl std::fmt::Display for RequestRejected {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "FastCGI server rejected the request: {}", self.0)
  }
}

impl std::error::Error for RequestRejected {}

/// More of a response waited for its reader than [`Builder::max_unread`] on
/// a connection that multiplexes, so the connection gave it up and aborted
/// its request: the other requests on the connection would otherwise have
/// waited for that reader.
///
/// Reading the response fails with it inside a [`std::io::Error`]; get it
/// with [`std::io::Error::get_ref`] and `downcast_ref`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseUnread;

impl std::fmt::Display for ResponseUnread {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "the FastCGI response wasn't read fast enough and was given up")
  }
}

impl std::error::Error for ResponseUnread {}

/// How much of a response may wait for its reader by default on a
/// connection that multiplexes, see [`Builder::max_unread`].
const DEFAULT_MAX_UNREAD: usize = 1024 * 1024;

/// Sleeps for the timeouts a [`Connection`] keeps, from whichever async
/// runtime drives it.
pub trait Timer: Send + Sync {
  /// A future that completes once `duration` has passed.
  fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

/// How long the server may take to end a request after
/// `FCGI_ABORT_REQUEST`, and what to sleep with.
#[derive(Clone)]
struct AbortTimeout {
  after: Duration,
  timer: Arc<dyn Timer>,
}

impl std::fmt::Debug for AbortTimeout {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("AbortTimeout")
      .field("after", &self.after)
      .finish_non_exhaustive()
  }
}

/// The server didn't end a request within [`Builder::abort_timeout`] of
/// `FCGI_ABORT_REQUEST`, so the connection ended: the request would
/// otherwise have kept its place on it for good.
///
/// The [`Connection`] ends with it inside a [`std::io::Error`] of kind
/// [`std::io::ErrorKind::TimedOut`]; get it with
/// [`std::io::Error::get_ref`] and `downcast_ref`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortUnanswered(pub Duration);

impl std::fmt::Display for AbortUnanswered {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "the FastCGI server didn't end an aborted request within {:?}",
      self.0
    )
  }
}

impl std::error::Error for AbortUnanswered {}

/// The request body failed while the connection sent it, so the FastCGI
/// server got only part of it; the inner error is the body's.
///
/// The request isn't finished as if the body were whole: the server gets
/// `FCGI_ABORT_REQUEST`, as for any request nobody waits for (see
/// [`Builder::abort_timeout`]).
///
/// [`SendRequest::send_request`] returns it inside a [`std::io::Error`]; get
/// it with [`std::io::Error::get_ref`] and `downcast_ref`.
#[derive(Debug)]
pub struct RequestBodyFailed(pub Box<dyn std::error::Error + Send + Sync>);

impl std::fmt::Display for RequestBodyFailed {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "the request body failed: {}", self.0)
  }
}

impl std::error::Error for RequestBodyFailed {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    Some(self.0.as_ref())
  }
}

/// The connection never sent the request to the FastCGI server, so sending
/// it again on another connection is safe, even if it isn't idempotent.
///
/// [`SendRequest::send_request`] returns it inside a [`std::io::Error`]; get
/// it with [`std::io::Error::get_ref`] and `downcast_ref`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotSent {
  /// The connection was closed, or stopped taking requests, first.
  Closed,
  /// The connection had no FastCGI request ID left for the request.
  NoRequestId,
}

impl std::fmt::Display for NotSent {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      NotSent::Closed => write!(f, "FastCGI connection closed before the request was sent"),
      NotSent::NoRequestId => write!(f, "no FastCGI request IDs available"),
    }
  }
}

impl std::error::Error for NotSent {}

/// A request that [`SendRequest::try_send_request`] gave back whole, because
/// the connection doesn't take requests any more: nothing of it was sent, so
/// it can go to another connection.
pub struct Unsent<B> {
  pub request: http::Request<B>,
  pub builder: CgiBuilder,
}

/// An error for a request the connection never sent, with the kind it has
/// always had for `why`.
fn not_sent(why: NotSent) -> std::io::Error {
  let kind = match why {
    NotSent::Closed => std::io::ErrorKind::ConnectionAborted,
    NotSent::NoRequestId => std::io::ErrorKind::WouldBlock,
  };
  std::io::Error::new(kind, why)
}

// ---------------------------------------------------------------------------
// Shared (state of SendRequest handles and their Connection)
// ---------------------------------------------------------------------------

/// State shared between [`SendRequest`] handles and their [`Connection`].
struct Shared {
  // Requests sent through SendRequest and not finished yet.
  in_flight: AtomicUsize,
  // The most requests the connection runs at the same time.
  max_requests: AtomicUsize,
  // The connection's task, for a dropped stdout reader to wake.
  waker: AtomicWaker,
  // Whether the server said it multiplexes connections (FCGI_MPXS_CONNS).
  // Every server gets FCGI_ABORT_REQUEST, but one that doesn't multiplex
  // may not read it until its request ended: php-fpm then reads it as the
  // start of the next request and closes a kept-alive connection. So such a
  // connection takes no request after an abort.
  multiplexes: AtomicBool,
  // Wakes SendRequest::idle when in_flight drops to 0 or the connection ends.
  settled: tokio::sync::Notify,
  // Since when no request runs, or `None` while one does. Changed together
  // with in_flight's way to and from 0, under this lock, so two threads
  // can't leave it saying idle while a request runs.
  idle_since: parking_lot::Mutex<Option<Instant>>,
  // Whether the Connection is gone.
  ended: AtomicBool,
}

impl Shared {
  fn new(max_requests: usize, multiplexes: bool) -> Self {
    Self {
      in_flight: AtomicUsize::new(0),
      max_requests: AtomicUsize::new(max_requests),
      waker: AtomicWaker::new(),
      multiplexes: AtomicBool::new(multiplexes),
      settled: tokio::sync::Notify::new(),
      idle_since: parking_lot::Mutex::new(Some(Instant::now())),
      ended: AtomicBool::new(false),
    }
  }

  /// Counts a request up; the connection isn't idle any more.
  fn start_request(&self) {
    let mut idle_since = self.idle_since.lock();
    if self.in_flight.fetch_add(1, Ordering::AcqRel) == 0 {
      *idle_since = None;
    }
  }

  /// Counts a finished request down, and wakes those waiting for the
  /// connection to be idle if it was the last one.
  fn finish_request(&self) {
    let mut idle_since = self.idle_since.lock();
    if self.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
      *idle_since = Some(Instant::now());
      drop(idle_since);
      self.settled.notify_waiters();
    }
  }
}

/// Marks the connection as ended when its [`Connection`] is dropped, so
/// [`SendRequest::idle`] doesn't wait for requests that will never finish.
struct EndGuard(Arc<Shared>);

impl Drop for EndGuard {
  fn drop(&mut self) {
    self.0.ended.store(true, Ordering::Release);
    self.0.settled.notify_waiters();
  }
}

/// What [`SendRequest::idle`] waited for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Settled {
  /// No request runs on the connection any more.
  Idle,
  /// The connection has ended; requests it still counted will never finish.
  Ended,
}

// ---------------------------------------------------------------------------
// SentRequest (internal message from SendRequest → Connection)
// ---------------------------------------------------------------------------

struct SentRequest<B> {
  req: http::Request<B>,
  builder: CgiBuilder,
  res_ch: oneshot::Sender<ResponseResult>,
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

pin_project_lite::pin_project! {
  /// Represents a connection to a FastCGI server to be spawned as a background task.
  pub struct Connection<Io, B> {
    #[pin]
    reader: FramedRead<ReadHalf<Io>, Decoder>,
    #[pin]
    writer: FramedWrite<WriteHalf<Io>, Encoder>,
    keepalive: bool,
    #[pin]
    send_request_rx: async_channel::Receiver<SentRequest<B>>,
    // Per-request state, keyed by FastCGI request ID.
    pending: HashMap<u16, PendingRequest<B>>,
    // ID allocator for FastCGI request IDs.
    id_alloc: IdAllocator,
    shared: Arc<Shared>,
    // Records waiting to be flushed to the writer.
    write_queue: VecDeque<Record>,
    // A `FCGI_STDOUT` chunk waiting for room in its request's channel.  While
    // it waits, no more records are read, so a slow reader slows down the
    // server instead of losing data.
    stdout_send: Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    // Requests that were running when the server closed the connection.
    cut_off: usize,
    // How much of a response may wait for its reader on a connection that
    // multiplexes.
    max_unread: usize,
    // How long an aborted request may take to end, if limited.
    abort_timeout: Option<AbortTimeout>,
    // Wakes the connection when the oldest unanswered abort is due.
    abort_sleep: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    ended: EndGuard,
  }
}

/// Sends a `FCGI_STDOUT` chunk to its [`FcgiResponseReader`], or returns a
/// future that sends it once the channel has room.
fn send_stdout(
  tx: &async_channel::Sender<Option<Vec<u8>>>,
  chunk: Option<Vec<u8>>,
) -> Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>> {
  match tx.try_send(chunk) {
    Err(async_channel::TrySendError::Full(chunk)) => {
      let tx = tx.clone();
      Some(Box::pin(async move {
        // An error means the reader is gone, so the chunk is not needed.
        let _ = tx.send(chunk).await;
      }))
    }
    // Sent, or the reader is gone.
    _ => None,
  }
}

/// The most bytes of records waiting to be written before the connection
/// stops taking more of the request bodies: a body goes to the server about
/// as fast as the server reads it, not into memory ahead of it.
const MAX_QUEUED: usize = 64 * 1024;

/// The bytes of the records in `queue`.
fn queued_bytes(queue: &VecDeque<Record>) -> usize {
  queue.iter().map(|record| record.content.len()).sum()
}

/// Stops `rx` from taking requests, and answers the ones still waiting in it
/// with [`NotSent::Closed`]: they are never sent.
fn refuse_waiting<B>(rx: &async_channel::Receiver<SentRequest<B>>, shared: &Shared) {
  if rx.close() {
    // Closed now: requests that were waiting are refused below.
  } else {
    // Closed before, by SendRequest::close or an earlier refusal: requests
    // still waiting are refused below all the same.
  }
  while let Ok(SentRequest { req, builder, res_ch }) = rx.try_recv() {
    drop((req, builder));
    shared.finish_request();
    answer(res_ch, Err(not_sent(NotSent::Closed)));
  }
}

/// Sends `result` to the caller of [`SendRequest::send_request`].
/// The response of a request handed to a connection, as the caller waits
/// for it.
///
/// Dropped before the response came, it wakes the connection, so the
/// connection can abort the request nobody waits for any more.
struct AwaitResponse {
  // `None` once the response came.
  rx: Option<oneshot::AsyncReceiver<ResponseResult>>,
  shared: Arc<Shared>,
}

impl Future for AwaitResponse {
  type Output = ResponseResult;

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ResponseResult> {
    let Some(rx) = self.rx.as_mut() else {
      return Poll::Ready(Err(std::io::Error::other("the response was already taken")));
    };
    match Pin::new(rx).poll(cx) {
      Poll::Ready(received) => {
        self.rx = None;
        Poll::Ready(match received {
          Ok(result) => result,
          Err(gone) => Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("unknown error obtaining response: {gone}"),
          )),
        })
      }
      Poll::Pending => Poll::Pending,
    }
  }
}

impl Drop for AwaitResponse {
  fn drop(&mut self) {
    if let Some(rx) = self.rx.take() {
      // Close first, so the connection sees no one waiting when it wakes.
      drop(rx);
      self.shared.waker.wake();
    }
  }
}

/// Whether the caller stopped waiting for the response headers of the
/// request whose response channel is `res_ch`.
fn headers_abandoned(res_ch: &std::sync::Mutex<Option<oneshot::Sender<ResponseResult>>>) -> bool {
  let res_ch = match res_ch.lock() {
    Ok(res_ch) => res_ch,
    // A response future panicked while it held the lock; the channel is
    // still whole.
    Err(poisoned) => poisoned.into_inner(),
  };
  match res_ch.as_ref() {
    Some(ch) => ch.is_closed(),
    // The response, or a rejection, was sent: the reader is watched instead.
    None => false,
  }
}

fn answer<T>(ch: oneshot::Sender<T>, result: T) {
  match ch.send(result) {
    Ok(()) => {}
    // The caller stopped waiting: there is no one to give it to.
    Err(unsent) => drop(unsent),
  }
}

// ---------------------------------------------------------------------------
// SendRequest
// ---------------------------------------------------------------------------

/// Represents a handle to send requests to a FastCGI server.
///
/// Clones send over the same connection, which ends once every handle is
/// dropped and its requests have finished.
pub struct SendRequest<B> {
  send_request_tx: async_channel::Sender<SentRequest<B>>,
  shared: Arc<Shared>,
}

// Not derived: that would need `B: Clone`.
impl<B> Clone for SendRequest<B> {
  fn clone(&self) -> Self {
    Self {
      send_request_tx: self.send_request_tx.clone(),
      shared: Arc::clone(&self.shared),
    }
  }
}

impl<B> SendRequest<B>
where
  B: Body + Send + 'static,
  B::Data: AsRef<[u8]> + Send + 'static,
{
  /// Sends an HTTP request to the FastCGI application and awaits the response.
  ///
  /// A request the connection doesn't take any more fails with
  /// [`NotSent::Closed`]; [`SendRequest::try_send_request`] gives it back
  /// instead.
  pub async fn send_request(
    &self,
    req: http::Request<B>,
    builder: CgiBuilder,
  ) -> Result<
    (
      http::Response<CgiIncoming<cegla::client::CgiResponseInner<FcgiResponseReader>>>,
      FcgiResponseReader,
    ),
    std::io::Error,
  > {
    match self.try_send_request(req, builder) {
      Ok(response) => response.await,
      Err(unsent) => {
        drop(unsent);
        Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, NotSent::Closed))
      }
    }
  }

  /// Hands an HTTP request to the connection at once and returns the future
  /// of its response; or, if the connection doesn't take requests any more,
  /// gives the request back whole, so it can be sent on another connection.
  ///
  /// The request counts in [`SendRequest::in_flight`] from this call on.
  pub fn try_send_request(
    &self,
    req: http::Request<B>,
    builder: CgiBuilder,
  ) -> Result<impl Future<Output = ResponseResult> + Send + 'static, Box<Unsent<B>>> {
    let (res_ch, res_rx) = oneshot::channel();
    // The connection counts the request down when it's finished.
    self.shared.start_request();
    match self.send_request_tx.try_send(SentRequest { req, builder, res_ch }) {
      Ok(()) => Ok(AwaitResponse {
        rx: Some(std::future::IntoFuture::into_future(res_rx)),
        shared: Arc::clone(&self.shared),
      }),
      // The channel is unbounded, so it is only ever full if it is closed.
      Err(async_channel::TrySendError::Closed(sent) | async_channel::TrySendError::Full(sent)) => {
        self.shared.finish_request();
        let SentRequest { req, builder, res_ch } = sent;
        // No one waits on the other end.
        drop((res_ch, res_rx));
        // Boxed: it is the rare case, and the request is large.
        Err(Box::new(Unsent { request: req, builder }))
      }
    }
  }

  /// The requests on the connection that haven't finished: running, or
  /// sent and waiting for room.  `0` means the connection is idle.
  pub fn in_flight(&self) -> usize {
    self.shared.in_flight.load(Ordering::Relaxed)
  }

  /// Since when no request runs on the connection, or `None` while one
  /// does. A new connection is idle since its handshake.
  pub fn idle_since(&self) -> Option<Instant> {
    *self.shared.idle_since.lock()
  }

  /// Stops the connection from taking new requests; returns whether this
  /// call did, `false` if it already had.
  ///
  /// Requests already handed to the connection run to their end, and the
  /// connection ends after them with [`Closed::ByClient`]. A request sent
  /// afterwards fails with [`NotSent::Closed`]. [`SendRequest::is_closed`]
  /// is `true` from now on.
  pub fn close(&self) -> bool {
    self.send_request_tx.close()
  }

  /// Waits until no request runs on the connection any more, or the
  /// connection has ended.
  pub async fn idle(&self) -> Settled {
    loop {
      let mut settled = std::pin::pin!(self.shared.settled.notified());
      // Register before looking, so a change in between isn't missed.
      let changed = settled.as_mut().enable();
      if self.shared.ended.load(Ordering::Acquire) {
        return Settled::Ended;
      }
      if self.shared.in_flight.load(Ordering::Acquire) == 0 {
        return Settled::Idle;
      }
      if !changed {
        settled.await;
      }
    }
  }

  /// Check if the connection is closed.
  pub fn is_closed(&self) -> bool {
    self.send_request_tx.is_closed()
  }

  /// Check if the connection can run another request without waiting for
  /// one to finish.
  pub fn is_ready(&self) -> bool {
    !self.is_closed() && self.shared.in_flight.load(Ordering::Relaxed) < self.max_requests()
  }

  /// The most requests the connection runs at the same time.
  pub fn max_requests(&self) -> usize {
    self.shared.max_requests.load(Ordering::Relaxed)
  }
}

/// How a FastCGI client [`Connection`] ended without an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Closed {
  /// The server closed the connection while no request ran on it.
  ByServer,
  /// Every [`SendRequest`] for the connection was dropped while no request
  /// ran on it.
  ByClient,
}

// ---------------------------------------------------------------------------
// Connection::poll  (the core state machine)
// ---------------------------------------------------------------------------

impl<Io, B> Future for Connection<Io, B>
where
  Io: AsyncRead + AsyncWrite + Unpin + 'static,
  B: Body + Send + 'static,
  B::Data: AsRef<[u8]> + Send + 'static,
  B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
  type Output = Result<Closed, std::io::Error>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let mut this = self.project();
    this.shared.waker.register(cx.waker());

    // ------------------------------------------------------------------
    // 1. Accept new requests from SendRequest handles
    // ------------------------------------------------------------------
    loop {
      // Further requests wait until one finishes.
      if this.pending.len() >= this.shared.max_requests.load(Ordering::Relaxed) {
        break;
      }

      match this.send_request_rx.poll_next_unpin(cx) {
        Poll::Ready(Some(SentRequest { req, builder, res_ch })) => {
          // Allocate a fresh FastCGI request ID.
          let request_id = match this.id_alloc.allocate() {
            Some(id) => id,
            None => {
              // No IDs available – reject immediately.
              drop((req, builder));
              this.shared.finish_request();
              answer(res_ch, Err(not_sent(NotSent::NoRequestId)));
              continue;
            }
          };

          // ---- FCGI_BEGIN_REQUEST ----
          // Role = Responder (1), flags = FCGI_KEEP_CONN when keepalive is set.
          let flags: u8 = if *this.keepalive { FCGI_KEEP_CONN } else { 0 };
          let role = Role::Responder.as_u16();
          let begin_body = vec![(role >> 8) as u8, (role & 0xff) as u8, flags, 0, 0, 0, 0, 0];
          this
            .write_queue
            .push_back(Record::new(RecordType::BeginRequest.as_u8(), request_id, begin_body));

          // ---- FCGI_PARAMS ----
          // Build CGI environment variables from the HTTP request.
          let (parts, body) = req.into_parts();

          let uri = parts.uri.clone();
          let method = parts.method.clone();
          let version = parts.version;
          let headers = parts.headers.clone();

          // Build CGI params directly from the request parts (no body needed).
          let synthetic_req = {
            let mut builder = http::Request::builder()
              .method(method.clone())
              .uri(uri.clone())
              .version(version);
            for (name, value) in &headers {
              builder = builder.header(name, value);
            }
            builder.body(http_body_util::Empty::<Bytes>::new()).unwrap()
          };

          // CgiBuilder::build requires B::Error: Into<io::Error>.
          // http_body_util::Empty<Bytes> has Error = Infallible which doesn't
          // satisfy that bound, so we extract the env vars manually instead.
          let (parts2, _) = synthetic_req.into_parts();
          let env: cegla::CgiEnvironment = {
            // Re-build with a compatible body type by mapping the error.
            let mapped_req = {
              let mut builder = http::Request::builder()
                .method(parts2.method)
                .uri(parts2.uri)
                .version(parts2.version);
              for (name, value) in &parts2.headers {
                builder = builder.header(name, value);
              }
              builder
                .body(http_body_util::Empty::<Bytes>::new().map_err(|e| -> std::io::Error { std::io::Error::other(e) }))
                .unwrap()
            };
            let (env, _) = builder.build(mapped_req);
            env
          };

          let mut params_data = Vec::new();
          for (name, value) in env.iter() {
            let nvp = NameValuePair::from_slices(name.as_bytes(), value.as_bytes());
            params_data.extend_from_slice(&nvp.encode());
          }

          // Chunk PARAMS into ≤65535-byte records.
          for chunk in params_data.chunks(65535) {
            this
              .write_queue
              .push_back(Record::new(RecordType::Params.as_u8(), request_id, chunk.to_vec()));
          }
          // Empty PARAMS record terminates the params stream.
          this
            .write_queue
            .push_back(Record::new(RecordType::Params.as_u8(), request_id, vec![]));

          // Create the stdout and stderr pipes used by FcgiResponseReader.
          let (stdout_tx, stdout_rx) = async_channel::bounded::<Option<Vec<u8>>>(64);
          // The stderr reader reaches the caller only with the response, so
          // stderr sent before the response headers can't wait for it.
          let (stderr_tx, stderr_rx) = async_channel::unbounded::<Option<Vec<u8>>>();

          let given_up = Arc::new(AtomicBool::new(false));
          let reader = FcgiResponseReader {
            rx: Box::pin(stdout_rx),
            leftover: None,
            ended: false,
            wake_on_drop: Some(Arc::clone(this.shared)),
            given_up: Some(Arc::clone(&given_up)),
          };

          // Spawn the response-conversion future inside the Connection so it
          // is driven without a separate task.
          let res_ch = Arc::new(std::sync::Mutex::new(Some(res_ch)));
          let res_ch_cell = Arc::clone(&res_ch);
          let response_future: Pin<Box<dyn Future<Output = ()> + Send + 'static>> = Box::pin(async move {
            let result = convert_to_http_response(reader).await;
            if let Some(ch) = res_ch_cell.lock().unwrap().take() {
              let _ = ch.send(result.map(|r| {
                (
                  r,
                  FcgiResponseReader {
                    rx: Box::pin(stderr_rx),
                    leftover: None,
                    ended: false,
                    wake_on_drop: None,
                    given_up: None,
                  },
                )
              }));
            }
          });

          let response_futures: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send + 'static>>> =
            FuturesUnordered::new();
          response_futures.push(response_future);

          this.pending.insert(
            request_id,
            PendingRequest {
              body: Some(Box::pin(body)),
              stdout_tx,
              stderr_tx,
              end_received: false,
              stdout_ended: false,
              aborted: None,
              response_futures,
              res_ch,
              unread: VecDeque::new(),
              unread_bytes: 0,
              deliver: None,
              given_up,
            },
          );
        }
        Poll::Ready(None) => {
          // All SendRequest handles dropped – begin graceful shutdown.
          if this.pending.is_empty() {
            return Poll::Ready(Ok(Closed::ByClient));
          }
          break;
        }
        Poll::Pending => break,
      }
    }

    // ------------------------------------------------------------------
    // 2. Drain STDIN from pending requests that still have a body
    // ------------------------------------------------------------------
    // Whether a body has more to give than the queue takes now.
    let mut held_back = false;
    for (&request_id, pending) in this.pending.iter_mut() {
      if let Some(ref mut body) = pending.body {
        let mut body_done = false;
        let mut failed = None;
        loop {
          if queued_bytes(this.write_queue) >= MAX_QUEUED {
            // The server reads slower than the body comes: take more once
            // the queue has gone to it.
            held_back = true;
            break;
          }
          match body.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
              if let Ok(data) = frame.into_data() {
                let slice: &[u8] = data.as_ref();
                if !slice.is_empty() {
                  for chunk in slice.chunks(65535) {
                    this
                      .write_queue
                      .push_back(Record::new(RecordType::Stdin.as_u8(), request_id, chunk.to_vec()));
                  }
                }
              }
            }
            Poll::Ready(Some(Err(err))) => {
              failed = Some(err.into());
              break;
            }
            Poll::Ready(None) => {
              body_done = true;
              break;
            }
            Poll::Pending => break,
          }
        }
        if let Some(err) = failed {
          // STDIN doesn't end: the server must not take the body as whole.
          pending.body = None;
          // Nor may a response that has started look whole: its reader
          // fails. Closed, the stdout channel also makes step 5 abort the
          // request.
          if pending.stdout_tx.close() {
            // Closed now.
          } else {
            // Its reader was gone already.
          }
          let caller = match pending.res_ch.lock() {
            Ok(mut res_ch) => res_ch.take(),
            // A response future panicked while it held the lock; the
            // channel is still whole.
            Err(poisoned) => poisoned.into_inner().take(),
          };
          match caller {
            Some(ch) => answer(ch, Err(std::io::Error::other(RequestBodyFailed(err)))),
            // The response came already; its body fails instead.
            None => drop(err),
          }
          continue;
        }
        if body_done {
          // Empty STDIN record terminates the stdin stream.
          this
            .write_queue
            .push_back(Record::new(RecordType::Stdin.as_u8(), request_id, vec![]));
          pending.body = None;
        }
      }
    }
    // ------------------------------------------------------------------
    // 3. Flush the write queue to the FramedWrite
    // ------------------------------------------------------------------
    while let Some(record) = this.write_queue.front() {
      let record = record.clone();
      match this.writer.as_mut().poll_ready(cx) {
        Poll::Ready(Ok(())) => {
          this.write_queue.pop_front();
          if let Err(e) = this.writer.as_mut().start_send(record) {
            return Poll::Ready(Err(e));
          }
        }
        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
        Poll::Pending => break,
      }
    }
    // Flush any buffered data.
    if let Poll::Ready(Err(e)) = this.writer.as_mut().poll_flush(cx) {
      return Poll::Ready(Err(e));
    }
    if held_back && queued_bytes(this.write_queue) < MAX_QUEUED {
      // Room again: take more of the bodies. Otherwise the writer wakes the
      // connection once the server has read.
      cx.waker().wake_by_ref();
    }

    // ------------------------------------------------------------------
    // 4. Read incoming FastCGI records and dispatch them
    // ------------------------------------------------------------------
    let mut read_end = false;
    loop {
      // Wait until a held-back STDOUT chunk has been delivered.
      if let Some(send) = this.stdout_send.as_mut() {
        if send.as_mut().poll(cx).is_pending() {
          break;
        }
        *this.stdout_send = None;
      }

      match this.reader.poll_next_unpin(cx) {
        Poll::Ready(Some(Ok(record))) => {
          let request_id = record.request_id;
          match RecordType::from_u8(record.record_type) {
            Some(RecordType::Stdout) => {
              if let Some(pending) = this.pending.get_mut(&request_id) {
                // Empty STDOUT = EOF sentinel.
                pending.stdout_ended |= record.content.is_empty();
                let chunk = (!record.content.is_empty()).then_some(record.content);
                if this.shared.multiplexes.load(Ordering::Relaxed) {
                  // Read on for the other requests.
                  pending.pass_stdout(chunk, *this.max_unread);
                } else {
                  // The one request: the server waits for its reader.
                  *this.stdout_send = send_stdout(&pending.stdout_tx, chunk);
                }
              }
            }
            Some(RecordType::Stderr) => {
              if let Some(pending) = this.pending.get_mut(&request_id) {
                if record.content.is_empty() {
                  // Empty STDERR = EOF sentinel.
                  let _ = pending.stderr_tx.try_send(None);
                } else {
                  let _ = pending.stderr_tx.try_send(Some(record.content));
                }
              }
            }
            Some(RecordType::EndRequest) => {
              if let Some(pending) = this.pending.get_mut(&request_id) {
                // Ensure EOF is signaled in case no empty STDOUT was sent,
                // waiting for room like a STDOUT chunk: a lost sentinel
                // makes the reader fail.
                if !pending.stdout_ended {
                  pending.stdout_ended = true;
                  if this.shared.multiplexes.load(Ordering::Relaxed) {
                    pending.pass_stdout(None, *this.max_unread);
                  } else {
                    *this.stdout_send = send_stdout(&pending.stdout_tx, None);
                  }
                }
                // The stderr channel is unbounded, so this fails only when
                // its reader is gone and needs no sentinel.
                if let Err(err) = pending.stderr_tx.try_send(None) {
                  debug_assert!(err.is_closed());
                }
                pending.end_received = true;

                let status = record.content.get(4).copied().and_then(ProtocolStatus::from_u8);
                match status {
                  Some(ProtocolStatus::CantMpxConn) => {
                    // The server takes one request at a time.
                    this.shared.max_requests.store(1, Ordering::Relaxed);
                    this.shared.multiplexes.store(false, Ordering::Relaxed);
                  }
                  Some(ProtocolStatus::Overloaded | ProtocolStatus::UnknownRole) => {
                    // The server asks not to send more requests on this
                    // connection; it ends once the open requests have.
                    refuse_waiting(this.send_request_rx.as_ref().get_ref(), this.shared);
                  }
                  Some(ProtocolStatus::RequestComplete) | None => {}
                }
                if let Some(status) = status.filter(|&status| status != ProtocolStatus::RequestComplete) {
                  if let Some(ch) = pending.res_ch.lock().unwrap().take() {
                    let _ = ch.send(Err(std::io::Error::other(RequestRejected(status))));
                  }
                }
              }
            }
            _ => {
              // Ignore unknown / unexpected records.
            }
          }
        }
        Poll::Ready(Some(Err(e))) => {
          return Poll::Ready(Err(e));
        }
        Poll::Ready(None) => {
          read_end = true;
          break;
        }
        Poll::Pending => break,
      }
    }

    // ------------------------------------------------------------------
    // 5. Poll response-conversion futures and clean up finished requests
    // ------------------------------------------------------------------
    let mut finished_ids: Vec<u16> = Vec::new();
    // An abort went to a server that doesn't multiplex.
    let mut aborted_one_at_a_time = false;
    for (&request_id, pending) in this.pending.iter_mut() {
      let delivered = pending.deliver_unread(cx);
      while let Poll::Ready(Some(())) = pending.response_futures.poll_next_unpin(cx) {}

      // The response was dropped before it ended, or the caller stopped
      // waiting for its headers, or the request body failed: the server can
      // stop.
      if pending.aborted.is_none()
        && !pending.stdout_ended
        && !pending.end_received
        && (pending.stdout_tx.is_closed() || headers_abandoned(&pending.res_ch))
      {
        pending.aborted = Some(Instant::now());
        pending.body = None;
        this
          .write_queue
          .push_back(Record::new(RecordType::AbortRequest.as_u8(), request_id, vec![]));
        // Flush the write queue.
        cx.waker().wake_by_ref();
        if !this.shared.multiplexes.load(Ordering::Relaxed) {
          aborted_one_at_a_time = true;
        }
      }

      // A request is fully done when END_REQUEST arrived, its STDOUT has
      // reached the reader, and all response futures have completed.
      if pending.end_received && delivered && pending.response_futures.is_empty() {
        finished_ids.push(request_id);
      }
    }
    if aborted_one_at_a_time {
      // The server may read the abort only after this request, as the
      // start of the next one: the connection takes no other request, and
      // ends once the server has ended this one.
      refuse_waiting(this.send_request_rx.as_ref().get_ref(), this.shared);
    }
    if !finished_ids.is_empty() {
      // Waiting requests may start now.
      cx.waker().wake_by_ref();
    }
    for id in finished_ids {
      this.pending.remove(&id);
      this.id_alloc.free(id);
      this.shared.finish_request();
    }

    // ------------------------------------------------------------------
    // 6. End the connection if the server doesn't answer an abort
    // ------------------------------------------------------------------
    if let Some(abort_timeout) = this.abort_timeout.as_ref() {
      // The oldest abort the server hasn't answered.
      let mut oldest: Option<Instant> = None;
      for pending in this.pending.values() {
        if pending.end_received {
          // Answered: it finishes once its response has been delivered,
          // which can wait for room in a full stdout channel when the
          // caller left before the headers. Rare, so no test covers it.
          continue;
        }
        if let Some(at) = pending.aborted {
          oldest = Some(match oldest {
            Some(older) => older.min(at),
            None => at,
          });
        }
      }
      match oldest {
        None => *this.abort_sleep = None,
        Some(at) => {
          let waited = at.elapsed();
          if waited >= abort_timeout.after {
            // Requests waiting for this connection go elsewhere.
            refuse_waiting(this.send_request_rx.as_ref().get_ref(), this.shared);
            return Poll::Ready(Err(std::io::Error::new(
              std::io::ErrorKind::TimedOut,
              AbortUnanswered(abort_timeout.after),
            )));
          }
          let sleep = this
            .abort_sleep
            .get_or_insert_with(|| abort_timeout.timer.sleep(abort_timeout.after - waited));
          match sleep.as_mut().poll(cx) {
            Poll::Ready(()) => {
              // Due: look again.
              *this.abort_sleep = None;
              cx.waker().wake_by_ref();
            }
            Poll::Pending => {}
          }
        }
      }
    }

    if read_end {
      // The server closed the connection. Requests it didn't end are cut
      // off: no response will come for them.
      let mut cut_off = Vec::new();
      for (&request_id, pending) in this.pending.iter() {
        if !pending.end_received {
          cut_off.push(request_id);
        }
      }
      if !cut_off.is_empty() {
        *this.cut_off += cut_off.len();
        // No more requests can be sent on this connection.
        refuse_waiting(this.send_request_rx.as_ref().get_ref(), this.shared);
      }
      for request_id in cut_off {
        if let Some(pending) = this.pending.remove(&request_id) {
          if let Some(ch) = pending.res_ch.lock().unwrap().take() {
            answer(
              ch,
              Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "the FastCGI server closed the connection before the response headers",
              )),
            );
          }
          // Dropping `pending` closes its stdout and stderr channels without
          // the end-of-stream sentinel, so their readers fail.
          drop(pending);
          this.id_alloc.free(request_id);
          this.shared.finish_request();
        }
      }
      // Requests the server ended finish delivering their responses first.
      if this.pending.is_empty() {
        if *this.cut_off > 0 {
          return Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
              "the FastCGI server closed the connection with {} requests running",
              this.cut_off
            ),
          )));
        }
        return Poll::Ready(Ok(Closed::ByServer));
      }
    }

    Poll::Pending
  }
}

// ---------------------------------------------------------------------------
// Record: add Clone so we can peek-then-send in the write loop
// ---------------------------------------------------------------------------

impl Clone for Record {
  fn clone(&self) -> Self {
    Self {
      version: self.version,
      record_type: self.record_type,
      request_id: self.request_id,
      content: self.content.clone(),
      padding_length: self.padding_length,
    }
  }
}

// ---------------------------------------------------------------------------
// handshake
// ---------------------------------------------------------------------------

/// Performs the FastCGI handshake and returns a handle to send requests and a
/// connection future.
///
/// The returned [`Connection`] must be driven to completion (e.g. via
/// `tokio::spawn`) for requests submitted through [`SendRequest`] to make
/// progress.
///
/// This is [`Builder::handshake`] with only `keepalive` set.
pub async fn handshake<Io, B>(io: Io, keepalive: bool) -> Result<(SendRequest<B>, Connection<Io, B>), std::io::Error>
where
  Io: AsyncRead + AsyncWrite + Unpin + 'static,
  B: Body + Send + 'static,
  B::Data: AsRef<[u8]> + Send + 'static,
{
  Builder::new().keepalive(keepalive).handshake(io).await
}

/// What a server told about itself in `FCGI_GET_VALUES_RESULT`.
#[derive(Debug, Default)]
struct ServerValues {
  // FCGI_MPXS_CONNS
  multiplexes: Option<bool>,
  // FCGI_MAX_REQS
  max_requests: Option<usize>,
}

/// Sends `FCGI_GET_VALUES` and reads the answer.  A server that doesn't know
/// the record type answers `FCGI_UNKNOWN_TYPE`, which tells nothing.
async fn get_values<R, W>(
  reader: &mut FramedRead<R, Decoder>,
  writer: &mut FramedWrite<W, Encoder>,
) -> Result<ServerValues, std::io::Error>
where
  R: AsyncRead + Unpin,
  W: AsyncWrite + Unpin,
{
  let mut query = Vec::new();
  for name in [FCGI_MAX_REQS, FCGI_MPXS_CONNS] {
    query.extend_from_slice(&NameValuePair::from_slices(name.as_bytes(), b"").encode());
  }
  writer
    .send(Record::new(RecordType::GetValues.as_u8(), FCGI_NULL_REQUEST_ID, query))
    .await?;

  loop {
    let record = reader.next().await.ok_or_else(|| {
      std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "connection closed before FCGI_GET_VALUES_RESULT",
      )
    })??;
    if record.request_id != FCGI_NULL_REQUEST_ID {
      // Not an answer to the query.
      continue;
    }
    match RecordType::from_u8(record.record_type) {
      Some(RecordType::GetValuesResult) => {
        let mut values = ServerValues::default();
        let mut content = std::io::Cursor::new(record.content);
        while let Some(nvp) = NameValuePair::decode(&mut content)? {
          if nvp.name == FCGI_MPXS_CONNS.as_bytes() {
            values.multiplexes = Some(nvp.value == b"1");
          } else if nvp.name == FCGI_MAX_REQS.as_bytes() {
            values.max_requests = std::str::from_utf8(&nvp.value).ok().and_then(|v| v.parse().ok());
          }
        }
        return Ok(values);
      }
      Some(RecordType::UnknownType) => return Ok(ServerValues::default()),
      _ => continue,
    }
  }
}

/// Options for a FastCGI client connection.
#[derive(Debug, Clone)]
pub struct Builder {
  keepalive: bool,
  max_requests: usize,
  get_values: bool,
  max_unread: usize,
  abort_timeout: Option<AbortTimeout>,
}

impl Default for Builder {
  fn default() -> Self {
    Self::new()
  }
}

impl Builder {
  /// Creates options with keepalive off and no limit on requests.
  pub fn new() -> Self {
    Self {
      keepalive: false,
      max_requests: usize::MAX,
      get_values: false,
      max_unread: DEFAULT_MAX_UNREAD,
      abort_timeout: None,
    }
  }

  /// Sets `FCGI_KEEP_CONN`, so the server keeps the connection open after a
  /// request.
  pub fn keepalive(&mut self, enabled: bool) -> &mut Self {
    self.keepalive = enabled;
    self
  }

  /// Sets the most requests the connection runs at the same time; further
  /// requests wait until one finishes.  A server that doesn't multiplex
  /// connections needs `1`.
  pub fn max_requests(&mut self, max: usize) -> &mut Self {
    self.max_requests = max.max(1);
    self
  }

  /// Asks the server during the handshake how many requests it runs at the
  /// same time on one connection (`FCGI_MAX_REQS`, `FCGI_MPXS_CONNS`).  A
  /// server that doesn't multiplex connections gets one request at a time,
  /// and the limit is the smaller of `FCGI_MAX_REQS` and
  /// [`max_requests`](Self::max_requests).  A server that doesn't know the
  /// query leaves the limit as it is.
  ///
  /// The handshake then waits for the answer, so wrap it in a timeout.
  pub fn get_values(&mut self, enabled: bool) -> &mut Self {
    self.get_values = enabled;
    self
  }

  /// Sets how many bytes of a response may wait for its reader on a
  /// connection that multiplexes, 1 MiB by default. FastCGI can't tell the
  /// server to pause one request, so the connection reads on for the other
  /// requests and keeps what a slow reader hasn't taken yet; past this, it
  /// gives the response up, aborts its request, and the reader fails with
  /// [`ResponseUnread`]. A connection that runs one request at a time
  /// instead stops reading from the server until the reader takes more.
  pub fn max_unread(&mut self, bytes: usize) -> &mut Self {
    self.max_unread = bytes;
    self
  }

  /// Ends the connection with [`AbortUnanswered`] when the server doesn't
  /// end a request within `after` of `FCGI_ABORT_REQUEST`; otherwise such a
  /// request keeps its place on the connection, and the connection never
  /// becomes idle. `timer` gives the connection its sleeps. Off by default.
  ///
  /// Every server gets `FCGI_ABORT_REQUEST`. On a connection to a server
  /// that doesn't multiplex, the connection takes no request after an
  /// abort and ends once the server has ended the aborted one: such a
  /// server may read the abort only then, as php-fpm does, which reads it as
  /// the start of the next request.
  pub fn abort_timeout(&mut self, after: Duration, timer: Arc<dyn Timer>) -> &mut Self {
    self.abort_timeout = Some(AbortTimeout { after, timer });
    self
  }

  /// Performs the FastCGI handshake and returns a handle to send requests and
  /// a connection future.
  ///
  /// The returned [`Connection`] must be driven to completion (e.g. via
  /// `tokio::spawn`) for requests submitted through [`SendRequest`] to make
  /// progress.
  pub async fn handshake<Io, B>(&self, io: Io) -> Result<(SendRequest<B>, Connection<Io, B>), std::io::Error>
  where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    B: Body + Send + 'static,
    B::Data: AsRef<[u8]> + Send + 'static,
  {
    let (reader, writer) = tokio::io::split(io);
    let mut framed_read = FramedRead::new(reader, Decoder::default());
    let mut framed_write = FramedWrite::new(writer, Encoder);
    let values = if self.get_values {
      get_values(&mut framed_read, &mut framed_write).await?
    } else {
      ServerValues::default()
    };
    let max_requests = match values {
      ServerValues {
        multiplexes: Some(false),
        ..
      } => 1,
      ServerValues {
        max_requests: Some(max),
        ..
      } => self.max_requests.min(max.max(1)),
      ServerValues { .. } => self.max_requests,
    };
    let (send_request_tx, send_request_rx) = async_channel::unbounded();
    let shared = Arc::new(Shared::new(max_requests, values.multiplexes == Some(true)));

    Ok((
      SendRequest {
        send_request_tx,
        shared: Arc::clone(&shared),
      },
      Connection {
        reader: framed_read,
        writer: framed_write,
        keepalive: self.keepalive,
        send_request_rx,
        pending: HashMap::default(),
        id_alloc: IdAllocator::new(),
        ended: EndGuard(Arc::clone(&shared)),
        shared,
        write_queue: VecDeque::new(),
        stdout_send: None,
        cut_off: 0,
        max_unread: self.max_unread,
        abort_timeout: self.abort_timeout.clone(),
        abort_sleep: None,
      },
    ))
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
  use super::*;
  use crate::protocol::codec::{Decoder, Encoder};
  use tokio::io::{AsyncRead, AsyncWrite};
  use tokio_util::codec::FramedRead;

  /// Mock IO type for testing
  struct MockIo {
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
    read_pos: usize,
  }

  impl AsyncRead for MockIo {
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
      let remaining = &self.read_buf[self.read_pos..];
      let to_read = remaining.len().min(buf.remaining());
      buf.put_slice(&remaining[..to_read]);
      self.read_pos += to_read;
      Poll::Ready(Ok(()))
    }
  }

  impl AsyncWrite for MockIo {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
      self.write_buf.extend_from_slice(buf);
      Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
      Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
      Poll::Ready(Ok(()))
    }
  }

  /// Create a simple mock response with headers and body
  fn make_mock_response(content: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();

    // FCGI_STDOUT record with content
    let stdout_record = Record::new(RecordType::Stdout.as_u8(), 1, content.to_vec());
    result.extend_from_slice(&stdout_record.encode().unwrap());

    // Empty FCGI_STDOUT record (EOF sentinel)
    let empty_stdout = Record::new(RecordType::Stdout.as_u8(), 1, vec![]);
    result.extend_from_slice(&empty_stdout.encode().unwrap());

    // FCGI_END_REQUEST record
    let end_request_body = vec![0, 0, 0, 0, 0, 0, 0, 0]; // status=0, protocolStatus=0
    let end_request = Record::new(RecordType::EndRequest.as_u8(), 1, end_request_body);
    result.extend_from_slice(&end_request.encode().unwrap());

    result
  }

  #[tokio::test]
  async fn test_fcgi_response_reader_basic() {
    let content = b"Content-Type: text/plain\r\n\r\nHello";
    let mock_data = make_mock_response(content);

    let mock_io = MockIo {
      read_buf: mock_data,
      write_buf: Vec::new(),
      read_pos: 0,
    };

    let (reader, writer) = tokio::io::split(mock_io);
    let framed_read = FramedRead::new(reader, Decoder::default());
    let framed_write = FramedWrite::new(writer, Encoder);
    let (_send_request_tx, send_request_rx) = async_channel::unbounded::<SentRequest<http_body_util::Full<Bytes>>>();

    let shared = Arc::new(Shared::new(usize::MAX, false));
    let connection = Connection {
      reader: framed_read,
      writer: framed_write,
      keepalive: false,
      send_request_rx,
      pending: HashMap::default(),
      id_alloc: IdAllocator::new(),
      ended: EndGuard(Arc::clone(&shared)),
      shared,
      write_queue: VecDeque::new(),
      stdout_send: None,
      cut_off: 0,
      max_unread: DEFAULT_MAX_UNREAD,
      abort_timeout: None,
      abort_sleep: None,
    };

    // Test that we can create a connection
    drop(connection);
  }

  #[tokio::test]
  async fn test_handshake() {
    let mock_data = make_mock_response(b"faked");
    let mock_io = MockIo {
      read_buf: mock_data,
      write_buf: Vec::new(),
      read_pos: 0,
    };

    // Use Full<Bytes> which has io::Error as its error type
    let (send_req, connection) = handshake::<MockIo, http_body_util::Full<Bytes>>(mock_io, false)
      .await
      .unwrap();

    // Verify we got a SendRequest handle
    drop(send_req);
    drop(connection);
  }

  #[tokio::test]
  async fn test_send_request_basic() {
    let mock_data = make_mock_response(b"Content-Type: text/plain\r\n\r\nHello");
    let mock_io = MockIo {
      read_buf: mock_data,
      write_buf: Vec::new(),
      read_pos: 0,
    };

    let (send_req, connection) = handshake::<MockIo, http_body_util::Full<Bytes>>(mock_io, false)
      .await
      .unwrap();
    tokio::spawn(connection);

    // Create a simple request with Full body
    let request = http::Request::builder()
      .method("GET")
      .uri("http://example.com/test")
      .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
      .unwrap();

    let builder = CgiBuilder::new();

    // Send the request
    let result = send_req.send_request(request, builder).await;

    // We expect success since we provided a valid mock response
    assert!(result.is_ok());
  }

  #[tokio::test]
  async fn test_connection_drives_response() {
    let content = b"Content-Type: text/plain\r\n\r\nHello, FastCGI!";
    let mock_data = make_mock_response(content);
    let mock_io = MockIo {
      read_buf: mock_data,
      write_buf: Vec::new(),
      read_pos: 0,
    };

    let (send_req, connection) = handshake::<MockIo, http_body_util::Full<Bytes>>(mock_io, false)
      .await
      .unwrap();

    // Spawn the connection task
    let connection_task = tokio::spawn(connection);

    // Give the task a moment to process
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Create and send a request
    let request = http::Request::builder()
      .method("POST")
      .uri("http://example.com/api")
      .body(http_body_util::Full::<Bytes>::from(b"test body".as_ref()))
      .unwrap();

    let builder = CgiBuilder::new();
    let (response, _stdout_reader) = send_req.send_request(request, builder).await.unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(response.headers().get("Content-Type").unwrap(), "text/plain");

    // Drop the connection to stop the task
    drop(connection_task);
  }

  #[tokio::test]
  async fn test_request_id_allocation() {
    let mock_data = make_mock_response(b"faked");
    let mock_io = MockIo {
      read_buf: mock_data,
      write_buf: Vec::new(),
      read_pos: 0,
    };

    let (_send_req, connection) = handshake::<MockIo, http_body_util::Full<Bytes>>(mock_io, false)
      .await
      .unwrap();

    // The connection should have an IdAllocator that can allocate IDs
    // This is tested implicitly through the connection's operation

    drop(connection);
  }

  #[tokio::test]
  async fn test_keepalive_flag() {
    let mock_data = make_mock_response(b"faked");
    let mock_io = MockIo {
      read_buf: mock_data,
      write_buf: Vec::new(),
      read_pos: 0,
    };

    let (send_req, connection) = handshake::<MockIo, http_body_util::Full<Bytes>>(mock_io, true)
      .await
      .unwrap();

    tokio::spawn(connection);

    // Create a request
    let request = http::Request::builder()
      .method("GET")
      .uri("http://example.com/")
      .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
      .unwrap();

    let builder = CgiBuilder::new();
    let result = send_req.send_request(request, builder).await;
    assert!(result.is_ok());
  }

  #[tokio::test]
  async fn test_multiple_concurrent_requests() {
    // Create a mock that returns multiple responses
    let content1 = b"Content-Type: text/plain\r\n\r\nResponse1";
    let content2 = b"Content-Type: application/json\r\n\r\n{\"ok\":true}";

    let mut mock_data = Vec::new();

    // First response
    let stdout1 = Record::new(RecordType::Stdout.as_u8(), 1, content1.to_vec());
    mock_data.extend_from_slice(&stdout1.encode().unwrap());
    let empty1 = Record::new(RecordType::Stdout.as_u8(), 1, vec![]);
    mock_data.extend_from_slice(&empty1.encode().unwrap());
    let end1 = Record::new(RecordType::EndRequest.as_u8(), 1, vec![0, 0, 0, 0, 0, 0, 0, 0]);
    mock_data.extend_from_slice(&end1.encode().unwrap());

    // Second response (different request ID)
    let stdout2 = Record::new(RecordType::Stdout.as_u8(), 2, content2.to_vec());
    mock_data.extend_from_slice(&stdout2.encode().unwrap());
    let empty2 = Record::new(RecordType::Stdout.as_u8(), 2, vec![]);
    mock_data.extend_from_slice(&empty2.encode().unwrap());
    let end2 = Record::new(RecordType::EndRequest.as_u8(), 2, vec![0, 0, 0, 0, 0, 0, 0, 0]);
    mock_data.extend_from_slice(&end2.encode().unwrap());

    let mock_io = MockIo {
      read_buf: mock_data,
      write_buf: Vec::new(),
      read_pos: 0,
    };

    let (send_req, connection) = handshake::<MockIo, http_body_util::Full<Bytes>>(mock_io, false)
      .await
      .unwrap();
    let connection_task = tokio::spawn(connection);

    // Send two requests concurrently
    let req1 = http::Request::builder()
      .method("GET")
      .uri("http://example.com/1")
      .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
      .unwrap();

    let req2 = http::Request::builder()
      .method("GET")
      .uri("http://example.com/2")
      .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
      .unwrap();

    let f1 = send_req.send_request(req1, CgiBuilder::new());
    let f2 = send_req.send_request(req2, CgiBuilder::new());

    let (res1, res2) = tokio::join!(f1, f2);

    assert!(res1.is_ok());
    assert!(res2.is_ok());

    drop(connection_task);
  }

  #[tokio::test]
  async fn test_stderr_stream() {
    // Create a mock with both stdout and stderr
    let mut mock_data = Vec::new();

    // Stdout
    let stdout = Record::new(RecordType::Stdout.as_u8(), 1, b"OK".to_vec());
    mock_data.extend_from_slice(&stdout.encode().unwrap());

    // Stderr (error message)
    let stderr = Record::new(RecordType::Stderr.as_u8(), 1, b"Warning: something happened".to_vec());
    mock_data.extend_from_slice(&stderr.encode().unwrap());

    // EOF markers
    let empty_stdout = Record::new(RecordType::Stdout.as_u8(), 1, vec![]);
    mock_data.extend_from_slice(&empty_stdout.encode().unwrap());
    let empty_stderr = Record::new(RecordType::Stderr.as_u8(), 1, vec![]);
    mock_data.extend_from_slice(&empty_stderr.encode().unwrap());

    let end_request = Record::new(RecordType::EndRequest.as_u8(), 1, vec![0, 0, 0, 0, 0, 0, 0, 0]);
    mock_data.extend_from_slice(&end_request.encode().unwrap());

    let mock_io = MockIo {
      read_buf: mock_data,
      write_buf: Vec::new(),
      read_pos: 0,
    };

    let (send_req, connection) = handshake::<MockIo, http_body_util::Full<Bytes>>(mock_io, false)
      .await
      .unwrap();
    let connection_task = tokio::spawn(connection);

    let request = http::Request::builder()
      .method("GET")
      .uri("http://example.com/")
      .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
      .unwrap();

    let builder = CgiBuilder::new();
    let (_response, _stdout_reader) = send_req.send_request(request, builder).await.unwrap();

    drop(connection_task);
  }

  #[tokio::test]
  async fn test_empty_request_body() {
    let mock_data = make_mock_response(b"Content-Type: text/plain\r\n\r\n");
    let mock_io = MockIo {
      read_buf: mock_data,
      write_buf: Vec::new(),
      read_pos: 0,
    };

    let (send_req, connection) = handshake::<MockIo, http_body_util::Full<Bytes>>(mock_io, false)
      .await
      .unwrap();
    let connection_task = tokio::spawn(connection);

    let request = http::Request::builder()
      .method("GET")
      .uri("http://example.com/")
      .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
      .unwrap();

    let builder = CgiBuilder::new();
    let (response, _reader) = send_req.send_request(request, builder).await.unwrap();

    assert_eq!(response.status(), 200);

    drop(connection_task);
  }

  #[tokio::test]
  async fn test_request_body_streaming() {
    let content = b"Content-Type: text/plain\r\n\r\nReceived";
    let mock_data = make_mock_response(content);
    let mock_io = MockIo {
      read_buf: mock_data,
      write_buf: Vec::new(),
      read_pos: 0,
    };

    let (send_req, connection) = handshake::<MockIo, http_body_util::Full<Bytes>>(mock_io, false)
      .await
      .unwrap();
    let connection_task = tokio::spawn(connection);

    let body_data = b"This is a test body with some data";
    let request = http::Request::builder()
      .method("POST")
      .uri("http://example.com/upload")
      .body(http_body_util::Full::<Bytes>::from(body_data.as_ref()))
      .unwrap();

    let builder = CgiBuilder::new();
    let (_response, _reader) = send_req.send_request(request, builder).await.unwrap();

    drop(connection_task);
  }

  #[tokio::test]
  async fn test_connection_cleanup() {
    let mock_data = make_mock_response(b"faked");
    let mock_io = MockIo {
      read_buf: mock_data,
      write_buf: Vec::new(),
      read_pos: 0,
    };

    let (send_req, connection) = handshake::<MockIo, http_body_util::Full<Bytes>>(mock_io, false)
      .await
      .unwrap();

    // Drop the send_req to signal no more requests
    drop(send_req);

    // Connection should complete when pending is empty
    let result = connection.await;
    assert_eq!(result.expect("without an error"), Closed::ByClient);
  }

  /// Encode a response of `records` FCGI_STDOUT records of `size` bytes each
  /// after a header record.
  fn make_long_response(request_id: u16, records: usize, size: usize) -> Vec<u8> {
    let mut result = Vec::new();
    let header = Record::new(
      RecordType::Stdout.as_u8(),
      request_id,
      b"Content-Type: text/plain\r\n\r\n".to_vec(),
    );
    result.extend_from_slice(&header.encode().unwrap());
    for _ in 0..records {
      let stdout = Record::new(RecordType::Stdout.as_u8(), request_id, vec![b'x'; size]);
      result.extend_from_slice(&stdout.encode().unwrap());
    }
    let empty_stdout = Record::new(RecordType::Stdout.as_u8(), request_id, vec![]);
    result.extend_from_slice(&empty_stdout.encode().unwrap());
    let end_request = Record::new(RecordType::EndRequest.as_u8(), request_id, vec![0, 0, 0, 0, 0, 0, 0, 0]);
    result.extend_from_slice(&end_request.encode().unwrap());
    result
  }

  #[tokio::test]
  async fn test_long_response_arrives_whole() {
    // More records than the stdout channel holds, all readable before the
    // response body is read.
    let mock_io = MockIo {
      read_buf: make_long_response(1, 200, 1000),
      write_buf: Vec::new(),
      read_pos: 0,
    };

    let (send_req, connection) = handshake::<MockIo, http_body_util::Full<Bytes>>(mock_io, false)
      .await
      .unwrap();
    let connection_task = tokio::spawn(connection);

    let request = http::Request::builder()
      .method("GET")
      .uri("http://example.com/")
      .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
      .unwrap();

    let (response, _reader) = send_req.send_request(request, CgiBuilder::new()).await.unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.len(), 200 * 1000);

    drop(connection_task);
  }

  #[tokio::test]
  async fn test_dropped_response_does_not_block_connection() {
    // The first response is longer than the stdout channel holds and is
    // dropped unread; the second one behind it must still arrive.
    let mut mock_data = make_long_response(1, 200, 1000);
    mock_data.extend_from_slice(&make_long_response(2, 1, 5));
    let mock_io = MockIo {
      read_buf: mock_data,
      write_buf: Vec::new(),
      read_pos: 0,
    };

    let (send_req, connection) = handshake::<MockIo, http_body_util::Full<Bytes>>(mock_io, false)
      .await
      .unwrap();
    let connection_task = tokio::spawn(connection);

    let req1 = http::Request::builder()
      .method("GET")
      .uri("http://example.com/1")
      .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
      .unwrap();
    let req2 = http::Request::builder()
      .method("GET")
      .uri("http://example.com/2")
      .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
      .unwrap();

    let f1 = async {
      // Drop the response with its body unread.
      send_req.send_request(req1, CgiBuilder::new()).await.unwrap();
    };
    let f2 = send_req.send_request(req2, CgiBuilder::new());
    let ((), res2) = tokio::join!(f1, f2);

    let (response, _reader) = res2.unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), b"xxxxx");

    drop(connection_task);
  }

  #[tokio::test]
  async fn test_long_stderr_before_headers_arrives_whole() {
    // More STDERR records than a channel of 64 holds, all before the response
    // headers, so before the caller has the stderr reader.
    let mut mock_data = Vec::new();
    for _ in 0..200 {
      let stderr = Record::new(RecordType::Stderr.as_u8(), 1, vec![b'e'; 100]);
      mock_data.extend_from_slice(&stderr.encode().unwrap());
    }
    let empty_stderr = Record::new(RecordType::Stderr.as_u8(), 1, vec![]);
    mock_data.extend_from_slice(&empty_stderr.encode().unwrap());
    mock_data.extend_from_slice(&make_mock_response(b"Content-Type: text/plain\r\n\r\nOK"));
    let mock_io = MockIo {
      read_buf: mock_data,
      write_buf: Vec::new(),
      read_pos: 0,
    };

    let (send_req, connection) = handshake::<MockIo, http_body_util::Full<Bytes>>(mock_io, false)
      .await
      .unwrap();
    let connection_task = tokio::spawn(connection);

    let request = http::Request::builder()
      .method("GET")
      .uri("http://example.com/")
      .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
      .unwrap();

    let (response, mut stderr_reader) = send_req.send_request(request, CgiBuilder::new()).await.unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), b"OK");

    let mut stderr = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stderr_reader, &mut stderr)
      .await
      .unwrap();
    assert_eq!(stderr.len(), 200 * 100);

    drop(connection_task);
  }

  /// A FastCGI server for tests.  Once it has read all records sent so far,
  /// it answers every request whose STDIN has ended with the request ID as
  /// the body.  It answers `FCGI_GET_VALUES` with `values`, or with
  /// `FCGI_UNKNOWN_TYPE` if there are none.  With `reject`, it ends requests
  /// with that protocol status instead; `FCGI_CANT_MPX_CONN` only while
  /// another request is open.  It returns the most requests that were open at
  /// the same time.
  async fn serve_requests(
    io: tokio::io::DuplexStream,
    values: &[(&str, &str)],
    reject: Option<ProtocolStatus>,
  ) -> usize {
    use futures_util::{FutureExt, SinkExt};

    let (reader, writer) = tokio::io::split(io);
    let mut reader = FramedRead::new(reader, Decoder::default());
    let mut writer = FramedWrite::new(writer, Encoder);
    let mut open: Vec<u16> = Vec::new();
    let mut answerable: Vec<u16> = Vec::new();
    let mut max_open = 0;

    while let Some(record) = reader.next().await {
      let mut records = vec![record.unwrap()];
      while let Some(Some(record)) = reader.next().now_or_never() {
        records.push(record.unwrap());
      }
      for record in records {
        match RecordType::from_u8(record.record_type) {
          Some(RecordType::GetValues) if values.is_empty() => {
            let content = vec![RecordType::GetValues.as_u8(), 0, 0, 0, 0, 0, 0, 0];
            writer
              .send(Record::new(RecordType::UnknownType.as_u8(), 0, content))
              .await
              .unwrap();
          }
          Some(RecordType::GetValues) => {
            let mut content = Vec::new();
            for (name, value) in values {
              content.extend_from_slice(&NameValuePair::from_slices(name.as_bytes(), value.as_bytes()).encode());
            }
            writer
              .send(Record::new(RecordType::GetValuesResult.as_u8(), 0, content))
              .await
              .unwrap();
          }
          Some(RecordType::BeginRequest) => open.push(record.request_id),
          Some(RecordType::Stdin) if record.content.is_empty() => answerable.push(record.request_id),
          Some(RecordType::AbortRequest) => panic!("unexpected FCGI_ABORT_REQUEST"),
          _ => {}
        }
      }
      max_open = max_open.max(open.len());

      // The client writes STDIN in no fixed order; answer the oldest first.
      answerable.sort_unstable();
      let multiplexed = open.len() > 1;
      for (i, request_id) in answerable.drain(..).enumerate() {
        let status = match reject {
          Some(ProtocolStatus::CantMpxConn) if !multiplexed || i == 0 => None,
          reject => reject,
        };
        let records = match status {
          Some(status) => vec![Record::new(
            RecordType::EndRequest.as_u8(),
            request_id,
            vec![0, 0, 0, 0, status.as_u8(), 0, 0, 0],
          )],
          None => vec![
            Record::new(
              RecordType::Stdout.as_u8(),
              request_id,
              format!("Content-Type: text/plain\r\n\r\n{request_id}").into_bytes(),
            ),
            Record::new(RecordType::Stdout.as_u8(), request_id, vec![]),
            Record::new(RecordType::EndRequest.as_u8(), request_id, vec![0, 0, 0, 0, 0, 0, 0, 0]),
          ],
        };
        for record in records {
          writer.send(record).await.unwrap();
        }
        open.retain(|&id| id != request_id);
      }
    }
    max_open
  }

  /// Sends `requests` requests at once through a connection made by
  /// `builder` to a server that answers `FCGI_GET_VALUES` with `values`.
  /// Returns the connection's limit and the most requests the server saw open
  /// at the same time.
  async fn run_requests(builder: &Builder, values: &'static [(&str, &str)], requests: usize) -> (usize, usize) {
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let server = tokio::spawn(serve_requests(server_io, values, None));
    let (send_req, connection) = builder
      .handshake::<_, http_body_util::Full<Bytes>>(client_io)
      .await
      .unwrap();
    let max_requests = send_req.max_requests();
    let connection = tokio::spawn(connection);

    let responses = (0..requests).map(|_| async {
      let request = http::Request::builder()
        .method("GET")
        .uri("http://example.com/")
        .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
        .unwrap();
      let (response, _reader) = send_req.send_request(request, CgiBuilder::new()).await.unwrap();
      response.into_body().collect().await.unwrap().to_bytes()
    });
    for body in futures_util::future::join_all(responses).await {
      assert!(!body.is_empty());
    }

    drop(send_req);
    connection.await.unwrap().unwrap();
    (max_requests, server.await.unwrap())
  }

  #[tokio::test]
  async fn test_requests_run_at_once_without_limit() {
    assert_eq!(run_requests(&Builder::new(), &[], 3).await.1, 3);
  }

  #[tokio::test]
  async fn test_max_requests_limits_open_requests() {
    assert_eq!(run_requests(Builder::new().max_requests(2), &[], 5).await, (2, 2));
    assert_eq!(run_requests(Builder::new().max_requests(1), &[], 3).await, (1, 1));
  }

  #[tokio::test]
  async fn test_get_values_from_multiplexing_server() {
    let values = &[("FCGI_MPXS_CONNS", "1"), ("FCGI_MAX_REQS", "2")];
    assert_eq!(run_requests(Builder::new().get_values(true), values, 5).await, (2, 2));
    // The smaller limit wins.
    let values = &[("FCGI_MPXS_CONNS", "1"), ("FCGI_MAX_REQS", "50")];
    let builder = Builder::new().get_values(true).max_requests(3).clone();
    assert_eq!(run_requests(&builder, values, 5).await, (3, 3));
  }

  #[tokio::test]
  async fn test_get_values_from_server_without_multiplexing() {
    // php-fpm's answer.
    let values = &[("FCGI_MAX_REQS", "5"), ("FCGI_MPXS_CONNS", "0")];
    assert_eq!(run_requests(Builder::new().get_values(true), values, 3).await, (1, 1));
  }

  #[tokio::test]
  async fn test_get_values_unknown_to_server() {
    let builder = Builder::new().get_values(true).max_requests(2).clone();
    assert_eq!(run_requests(&builder, &[], 4).await, (2, 2));
  }

  /// Sends `requests` requests at once and returns each response body or
  /// error.
  async fn send_requests(
    send_req: &SendRequest<http_body_util::Full<Bytes>>,
    requests: usize,
  ) -> Vec<Result<Bytes, std::io::Error>> {
    let responses = (0..requests).map(|_| async {
      let request = http::Request::builder()
        .method("GET")
        .uri("http://example.com/")
        .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
        .unwrap();
      let (response, _reader) = send_req.send_request(request, CgiBuilder::new()).await?;
      Ok(response.into_body().collect().await?.to_bytes())
    });
    futures_util::future::join_all(responses).await
  }

  /// The protocol status a request was rejected with.
  fn rejection(result: &Result<Bytes, std::io::Error>) -> Option<ProtocolStatus> {
    let error = result.as_ref().err()?.get_ref()?;
    error.downcast_ref::<RequestRejected>().map(|rejected| rejected.0)
  }

  #[tokio::test]
  async fn test_cant_mpx_conn_limits_connection() {
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let server = tokio::spawn(serve_requests(server_io, &[], Some(ProtocolStatus::CantMpxConn)));
    let (send_req, connection) = handshake::<_, http_body_util::Full<Bytes>>(client_io, true)
      .await
      .unwrap();
    let connection = tokio::spawn(connection);

    // The server takes the first request and rejects the others.
    let results = send_requests(&send_req, 3).await;
    assert!(results[0].is_ok());
    for result in &results[1..] {
      assert_eq!(rejection(result), Some(ProtocolStatus::CantMpxConn));
    }
    assert_eq!(send_req.max_requests(), 1);

    // From then on, one request at a time.
    assert!(send_requests(&send_req, 3).await.iter().all(Result::is_ok));

    drop(send_req);
    connection.await.unwrap().unwrap();
    server.await.unwrap();
  }

  #[tokio::test]
  async fn test_overloaded_and_unknown_role_close_connection() {
    for status in [ProtocolStatus::Overloaded, ProtocolStatus::UnknownRole] {
      let (client_io, server_io) = tokio::io::duplex(1 << 16);
      let server = tokio::spawn(serve_requests(server_io, &[], Some(status)));
      let (send_req, connection) = Builder::new()
        .keepalive(true)
        .max_requests(1)
        .handshake::<_, http_body_util::Full<Bytes>>(client_io)
        .await
        .unwrap();
      let connection = tokio::spawn(connection);

      // The first request is rejected; the second, still waiting, is not sent.
      let results = send_requests(&send_req, 2).await;
      assert_eq!(rejection(&results[0]), Some(status));
      let error = results[1].as_ref().unwrap_err();
      assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);
      assert!(send_req.is_closed());
      assert!(!send_req.is_ready());

      // The connection ends without the handle being dropped.
      connection.await.unwrap().unwrap();
      server.await.unwrap();
    }
  }

  /// How far [`serve_held_response`] gets with its response before it
  /// waits.
  #[derive(Clone, Copy, Debug)]
  enum Held {
    /// Nothing is sent: the script runs.
    BeforeHeaders,
    /// The headers and the start of the body are sent.
    InBody,
    /// The whole body is sent, without `FCGI_END_REQUEST`.
    BodyEnded,
  }

  /// A FastCGI server for tests that answers `FCGI_GET_VALUES` with
  /// `values` and gets as far with one response as `held` says.  It sends
  /// `FCGI_END_REQUEST` only once `end` fires, and returns whether an
  /// `FCGI_ABORT_REQUEST` had arrived by then.
  async fn serve_held_response(
    io: tokio::io::DuplexStream,
    values: &[(&str, &str)],
    held: Held,
    end: tokio::sync::oneshot::Receiver<()>,
  ) -> bool {
    use futures_util::{FutureExt, SinkExt};

    let (reader, writer) = tokio::io::split(io);
    let mut reader = FramedRead::new(reader, Decoder::default());
    let mut writer = FramedWrite::new(writer, Encoder);
    let mut request_id = None;
    while let Some(record) = reader.next().await {
      let record = record.unwrap();
      match RecordType::from_u8(record.record_type) {
        Some(RecordType::GetValues) => {
          let mut content = Vec::new();
          for (name, value) in values {
            content.extend_from_slice(&NameValuePair::from_slices(name.as_bytes(), value.as_bytes()).encode());
          }
          writer
            .send(Record::new(RecordType::GetValuesResult.as_u8(), 0, content))
            .await
            .unwrap();
        }
        Some(RecordType::Stdin) if record.content.is_empty() => {
          request_id = Some(record.request_id);
          break;
        }
        _ => {}
      }
    }
    let request_id = request_id.unwrap();
    let started = Record::new(
      RecordType::Stdout.as_u8(),
      request_id,
      b"Content-Type: text/plain\r\n\r\nstarted".to_vec(),
    );
    let records = match held {
      Held::BeforeHeaders => vec![],
      Held::InBody => vec![started],
      Held::BodyEnded => vec![started, Record::new(RecordType::Stdout.as_u8(), request_id, vec![])],
    };
    for record in records {
      writer.send(record).await.unwrap();
    }

    end.await.unwrap();
    let mut aborted = false;
    while let Some(Some(record)) = reader.next().now_or_never() {
      aborted |= record.unwrap().record_type == RecordType::AbortRequest.as_u8();
    }
    let end_request = vec![0, 0, 0, 0, ProtocolStatus::RequestComplete.as_u8(), 0, 0, 0];
    writer
      .send(Record::new(RecordType::EndRequest.as_u8(), request_id, end_request))
      .await
      .unwrap();
    aborted
  }

  /// Gets a response from [`serve_held_response`] through a connection made
  /// by `builder`, reads its body if `read_body`, drops it, and returns
  /// whether the server got `FCGI_ABORT_REQUEST` and whether the connection
  /// took no more requests while the server still ran the request.
  async fn drop_held_response(
    builder: &Builder,
    values: &'static [(&str, &str)],
    end_stdout: bool,
    read_body: bool,
  ) -> (bool, bool) {
    let held = if end_stdout { Held::BodyEnded } else { Held::InBody };
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let (end_tx, end_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve_held_response(server_io, values, held, end_rx));
    let (send_req, connection) = builder
      .handshake::<_, http_body_util::Full<Bytes>>(client_io)
      .await
      .unwrap();
    let connection = tokio::spawn(connection);

    let request = http::Request::builder()
      .method("GET")
      .uri("http://example.com/")
      .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
      .unwrap();
    let (response, _stderr_reader) = send_req.send_request(request, CgiBuilder::new()).await.unwrap();
    if read_body {
      let body = response.into_body().collect().await.unwrap().to_bytes();
      assert_eq!(body.as_ref(), b"started");
    } else {
      drop(response);
    }
    // Nothing more arrives from the server, so only the dropped reader can
    // wake the connection.
    for _ in 0..10 {
      tokio::task::yield_now().await;
    }
    // Before the server ends the request and closes its side.
    let closed = send_req.is_closed();
    end_tx.send(()).unwrap();

    let aborted = server.await.unwrap();
    drop(send_req);
    connection.await.unwrap().unwrap();
    (aborted, closed)
  }

  /// Hands a request to a connection made by `builder` to a server that
  /// sends nothing back, stops waiting for the response headers, and
  /// returns whether the server got `FCGI_ABORT_REQUEST`, and how many
  /// requests the connection still ran once the server ended the request.
  async fn stop_waiting_for_headers(builder: &Builder, values: &'static [(&str, &str)]) -> (bool, usize) {
    use futures_util::FutureExt;

    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let (end_tx, end_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve_held_response(server_io, values, Held::BeforeHeaders, end_rx));
    let (send_req, connection) = builder
      .handshake::<_, http_body_util::Full<Bytes>>(client_io)
      .await
      .unwrap();
    let connection = tokio::spawn(connection);

    let request = http::Request::builder()
      .method("GET")
      .uri("http://example.com/")
      .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
      .unwrap();
    let mut response = match send_req.try_send_request(request, CgiBuilder::new()) {
      Ok(response) => Box::pin(response),
      Err(unsent) => panic!("the connection takes the request: {:?}", unsent.request.uri()),
    };
    // Let the request reach the server.
    for _ in 0..10 {
      assert!((&mut response).now_or_never().is_none(), "no headers come");
      tokio::task::yield_now().await;
    }
    // A response timeout, say.
    drop(response);
    // Nothing arrives from the server, so only the caller leaving can wake
    // the connection.
    for _ in 0..10 {
      tokio::task::yield_now().await;
    }
    end_tx.send(()).unwrap();

    let aborted = server.await.unwrap();
    for _ in 0..10 {
      tokio::task::yield_now().await;
    }
    let in_flight = send_req.in_flight();
    drop(send_req);
    connection.await.unwrap().unwrap();
    (aborted, in_flight)
  }

  const MULTIPLEXES: &[(&str, &str)] = &[("FCGI_MPXS_CONNS", "1")];

  #[tokio::test]
  async fn test_a_request_nobody_waits_for_is_aborted_before_its_headers() {
    let builder = Builder::new().keepalive(true).get_values(true).clone();
    assert_eq!(stop_waiting_for_headers(&builder, MULTIPLEXES).await, (true, 0));
  }

  #[tokio::test]
  async fn test_a_request_nobody_waits_for_is_aborted_before_its_headers_without_multiplexing() {
    // A server that runs one request at a time gets the abort too.
    // Before: it got nothing, and ran the request to its end.
    let builder = Builder::new().keepalive(true).clone();
    assert_eq!(stop_waiting_for_headers(&builder, &[]).await, (true, 0));
  }

  #[tokio::test]
  async fn test_dropped_response_aborts_request() {
    let builder = Builder::new().keepalive(true).get_values(true).clone();
    assert_eq!(
      drop_held_response(&builder, MULTIPLEXES, false, false).await,
      (true, false),
      "a multiplexed connection runs on"
    );
  }

  #[tokio::test]
  async fn test_response_read_to_the_end_is_not_aborted() {
    // STDOUT has ended, but END_REQUEST is still to come.
    let builder = Builder::new().keepalive(true).get_values(true).clone();
    assert_eq!(
      drop_held_response(&builder, MULTIPLEXES, true, true).await,
      (false, false)
    );
    let builder = Builder::new().keepalive(true).clone();
    assert_eq!(drop_held_response(&builder, &[], true, true).await, (false, false));
  }

  #[tokio::test]
  async fn test_dropped_response_aborts_request_without_multiplexing() {
    // The server gets the abort, and the connection takes no request after
    // it: php-fpm reads an abort only after its request, as the start of
    // the next one, and then closes the connection.
    // Before: no abort, and the connection went on.
    let builder = Builder::new().keepalive(true).clone();
    assert_eq!(drop_held_response(&builder, &[], false, false).await, (true, true));
    let builder = Builder::new().keepalive(true).get_values(true).clone();
    assert_eq!(
      drop_held_response(&builder, &[("FCGI_MPXS_CONNS", "0")], false, false).await,
      (true, true)
    );
  }

  #[tokio::test]
  async fn test_is_ready_with_max_requests() {
    use futures_util::FutureExt;

    let (client_io, _server_io) = tokio::io::duplex(1 << 16);
    let (send_req, connection) = Builder::new()
      .max_requests(1)
      .handshake::<_, http_body_util::Full<Bytes>>(client_io)
      .await
      .unwrap();
    let connection_task = tokio::spawn(connection);
    assert_eq!(send_req.max_requests(), 1);
    assert!(send_req.is_ready());

    let request = http::Request::builder()
      .method("GET")
      .uri("http://example.com/")
      .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
      .unwrap();
    let mut response = Box::pin(send_req.send_request(request, CgiBuilder::new()));
    assert!((&mut response).now_or_never().is_none());
    assert!(!send_req.is_ready());

    drop(connection_task);
  }

  #[tokio::test]
  async fn test_cloned_handle_shares_the_connection() {
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let server = tokio::spawn(serve_requests(server_io, &[], None));
    let (send_req, connection) = Builder::new()
      .keepalive(true)
      .max_requests(4)
      .handshake::<_, http_body_util::Full<Bytes>>(client_io)
      .await
      .unwrap();
    let connection = tokio::spawn(connection);
    let clone = send_req.clone();

    // A clone sees the connection's limit and the original's requests.
    assert_eq!(clone.max_requests(), 4);
    {
      use futures_util::FutureExt;
      let mut pending = Vec::new();
      for _ in 0..4 {
        let request = http::Request::builder()
          .uri("http://example.com/")
          .body(http_body_util::Full::<Bytes>::from(b"".as_ref()))
          .unwrap();
        let mut response = Box::pin(send_req.send_request(request, CgiBuilder::new()));
        assert!((&mut response).now_or_never().is_none());
        pending.push(response);
      }
      assert!(!clone.is_ready());
      for response in futures_util::future::join_all(pending).await {
        response.unwrap();
      }
    }

    // Both handles' requests are open on the one connection at once.
    let (original_results, clone_results) = tokio::join!(send_requests(&send_req, 2), send_requests(&clone, 2));
    assert!(original_results.iter().chain(&clone_results).all(Result::is_ok));

    // The connection outlives the original handle while a clone is left.
    drop(send_req);
    assert!(!clone.is_closed());
    assert!(send_requests(&clone, 1).await[0].is_ok());

    drop(clone);
    connection.await.unwrap().unwrap();
    assert_eq!(server.await.unwrap(), 4);
  }
}

#[cfg(test)]
mod cut_off_tests {
  use super::*;
  use tokio::io::{AsyncReadExt, AsyncWriteExt};

  /// Yields until `task` is done, as long as a ready task could take; whether
  /// it is done by then.
  pub(super) async fn settles<T>(task: &tokio::task::JoinHandle<T>) -> bool {
    let mut rounds = 0;
    while rounds < 2000 && !task.is_finished() {
      tokio::task::yield_now().await;
      rounds += 1;
    }
    task.is_finished()
  }

  fn record(record_type: RecordType, content: &[u8]) -> Vec<u8> {
    record_for(record_type, 1, content)
  }

  /// A record of `record_type` for request `request_id`.
  pub(super) fn record_for(record_type: RecordType, request_id: u16, content: &[u8]) -> Vec<u8> {
    Record::new(record_type.as_u8(), request_id, content.to_vec())
      .encode()
      .unwrap()
  }

  fn end_request() -> Vec<u8> {
    record(RecordType::EndRequest, &[0, 0, 0, 0, 0, 0, 0, 0])
  }

  /// The outcome of one request to a server that waits for it, answers
  /// `answer`, and then closes the connection if `close` is set; and the
  /// outcome of the connection.
  async fn request(answer: Vec<u8>, close: bool) -> (Result<Bytes, std::io::Error>, Option<std::io::Result<Closed>>) {
    let (client_io, mut server_io) = tokio::io::duplex(65536);
    let (send_req, connection) = handshake::<_, http_body_util::Full<Bytes>>(client_io, true)
      .await
      .unwrap();
    let driver = tokio::spawn(connection);
    let server = tokio::spawn(async move {
      let mut buf = vec![0u8; 4096];
      let read = server_io.read(&mut buf).await.unwrap();
      assert!(read > 0, "the request arrives");
      server_io.write_all(&answer).await.unwrap();
      if close {
        drop(server_io);
      } else {
        // Keep the connection open until the test ends.
        std::future::pending::<()>().await;
      }
    });
    let req = http::Request::builder()
      .uri("http://backend/")
      .body(http_body_util::Full::<Bytes>::from(""))
      .unwrap();
    let response = tokio::spawn(async move {
      let (response, stderr) = send_req.send_request(req, CgiBuilder::new()).await?;
      drop(stderr);
      let mut body = response.into_body();
      let mut bytes = Vec::new();
      while let Some(frame) = body.frame().await {
        match frame?.into_data() {
          Ok(data) => bytes.extend_from_slice(&data),
          Err(frame) => panic!("a FastCGI response has no trailers: {frame:?}"),
        }
      }
      Ok::<_, std::io::Error>(Bytes::from(bytes))
    });
    assert!(settles(&response).await, "the request ends");
    let outcome = response.await.unwrap();
    let ended = if settles(&driver).await {
      Some(driver.await.unwrap())
    } else {
      None
    };
    server.abort();
    (outcome, ended)
  }

  #[tokio::test]
  async fn a_server_that_closes_before_answering_fails_the_request_and_the_connection() {
    let (outcome, ended) = request(Vec::new(), true).await;
    let err = outcome.expect_err("no response");
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    assert_eq!(
      err.to_string(),
      "the FastCGI server closed the connection before the response headers"
    );
    let err = ended.expect("the connection ends").expect_err("with an error");
    assert_eq!(
      err.to_string(),
      "the FastCGI server closed the connection with 1 requests running"
    );
  }

  #[tokio::test]
  async fn a_server_that_closes_after_the_headers_fails_the_body() {
    let answer = record(RecordType::Stdout, b"Content-Type: text/plain\r\n\r\npart");
    let (outcome, ended) = request(answer, true).await;
    let err = outcome.expect_err("the body is cut off");
    assert_eq!(err.to_string(), "the FastCGI connection ended before the response did");
    assert!(ended.expect("the connection ends").is_err());
  }

  #[tokio::test]
  async fn a_server_that_closes_after_answering_ends_the_connection_normally() {
    let mut answer = record(RecordType::Stdout, b"Content-Type: text/plain\r\n\r\nall");
    answer.extend(record(RecordType::Stdout, b""));
    answer.extend(end_request());
    let (outcome, ended) = request(answer, true).await;
    assert_eq!(outcome.expect("a response"), Bytes::from_static(b"all"));
    assert_eq!(
      ended.expect("the connection ends").expect("without an error"),
      Closed::ByServer
    );
  }

  #[tokio::test]
  async fn end_request_without_an_empty_stdout_ends_the_body() {
    let mut answer = record(RecordType::Stdout, b"Content-Type: text/plain\r\n\r\nall");
    answer.extend(end_request());
    let (outcome, ended) = request(answer, false).await;
    assert_eq!(outcome.expect("a response"), Bytes::from_static(b"all"));
    // The server keeps the connection open; it ends because the request's
    // handle, the only one, is gone.
    assert_eq!(
      ended.expect("the connection ends").expect("without an error"),
      Closed::ByClient
    );
  }

  #[tokio::test]
  async fn in_flight_counts_a_request_until_it_finishes() {
    let (client_io, mut server_io) = tokio::io::duplex(65536);
    let (send_req, connection) = handshake::<_, http_body_util::Full<Bytes>>(client_io, true)
      .await
      .unwrap();
    let driver = tokio::spawn(connection);
    assert_eq!(send_req.in_flight(), 0, "a new connection is idle");

    let sender = send_req.clone();
    let response = tokio::spawn(async move {
      let req = http::Request::builder()
        .uri("http://backend/")
        .body(http_body_util::Full::<Bytes>::from(""))
        .unwrap();
      let (response, stderr) = sender.send_request(req, CgiBuilder::new()).await?;
      drop(stderr);
      let body = response.into_body().collect().await?;
      Ok::<_, std::io::Error>(body.to_bytes())
    });
    let mut request = vec![0u8; 4096];
    let read = server_io.read(&mut request).await.unwrap();
    assert!(read > 0, "the request arrives");
    assert_eq!(send_req.in_flight(), 1, "the request runs");

    let mut answer = record(RecordType::Stdout, b"Content-Type: text/plain\r\n\r\ndone");
    answer.extend(record(RecordType::Stdout, b""));
    answer.extend(end_request());
    server_io.write_all(&answer).await.unwrap();
    assert!(settles(&response).await, "the response ends");
    assert_eq!(
      response.await.unwrap().expect("a response"),
      Bytes::from_static(b"done")
    );

    let mut rounds = 0;
    while rounds < 2000 && send_req.in_flight() > 0 {
      tokio::task::yield_now().await;
      rounds += 1;
    }
    assert_eq!(send_req.in_flight(), 0, "the connection is idle again");
    drop(send_req);
    assert!(settles(&driver).await, "the connection ends without handles");
    assert_eq!(driver.await.unwrap().expect("without an error"), Closed::ByClient);
  }

  /// A request that the server has read, on a connection whose driver runs;
  /// the server end of the connection, and the response it waits for.
  async fn running_request() -> (
    SendRequest<http_body_util::Full<Bytes>>,
    tokio::task::JoinHandle<std::io::Result<Closed>>,
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<Result<Bytes, std::io::Error>>,
  ) {
    let (client_io, mut server_io) = tokio::io::duplex(65536);
    let (send_req, connection) = handshake::<tokio::io::DuplexStream, http_body_util::Full<Bytes>>(client_io, true)
      .await
      .unwrap();
    let driver = tokio::spawn(connection);
    let sender = send_req.clone();
    let response = tokio::spawn(async move {
      let req = http::Request::builder()
        .uri("http://backend/")
        .body(http_body_util::Full::<Bytes>::from(""))
        .unwrap();
      let (response, stderr) = sender.send_request(req, CgiBuilder::new()).await?;
      drop(stderr);
      let body = response.into_body().collect().await?;
      Ok::<Bytes, std::io::Error>(body.to_bytes())
    });
    let mut request = vec![0u8; 4096];
    let read = server_io.read(&mut request).await.unwrap();
    assert!(read > 0, "the request arrives");
    (send_req, driver, server_io, response)
  }

  #[tokio::test]
  async fn a_new_connection_is_idle() {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let (send_req, connection) = handshake::<tokio::io::DuplexStream, http_body_util::Full<Bytes>>(client_io, true)
      .await
      .unwrap();
    assert_eq!(
      futures_util::FutureExt::now_or_never(send_req.idle()),
      Some(Settled::Idle)
    );
    drop((connection, server_io));
  }

  #[tokio::test]
  async fn idle_waits_until_the_running_request_finishes() {
    let (send_req, driver, mut server_io, response) = running_request().await;
    let sender = send_req.clone();
    let idle = tokio::spawn(async move { sender.idle().await });
    assert!(!settles(&idle).await, "idle waits while the request runs");

    let mut answer = record(RecordType::Stdout, b"Content-Type: text/plain\r\n\r\ndone");
    answer.extend(record(RecordType::Stdout, b""));
    answer.extend(end_request());
    server_io.write_all(&answer).await.unwrap();
    assert!(settles(&response).await, "the response ends");
    assert_eq!(
      response.await.unwrap().expect("a response"),
      Bytes::from_static(b"done")
    );
    assert!(settles(&idle).await, "idle ends with the request");
    assert_eq!(idle.await.unwrap(), Settled::Idle);
    assert!(!driver.is_finished(), "the connection stays open");
    driver.abort();
  }

  #[tokio::test]
  async fn idle_ends_when_the_connection_ends_with_a_request_running() {
    let (send_req, driver, server_io, response) = running_request().await;
    let sender = send_req.clone();
    let idle = tokio::spawn(async move { sender.idle().await });
    assert!(!settles(&idle).await, "idle waits while the request runs");

    // The driver goes away without finishing the request.
    driver.abort();
    assert!(settles(&idle).await, "idle ends with the connection");
    assert_eq!(idle.await.unwrap(), Settled::Ended);
    assert_eq!(send_req.in_flight(), 1, "the request never finished");
    assert!(settles(&response).await, "the response fails");
    assert!(response.await.unwrap().is_err());
    drop(server_io);
  }

  /// The [`NotSent`] inside `err`, if any.
  fn not_sent_of(err: &std::io::Error) -> Option<NotSent> {
    err.get_ref().and_then(|inner| inner.downcast_ref::<NotSent>()).copied()
  }

  fn get() -> http::Request<http_body_util::Full<Bytes>> {
    http::Request::builder()
      .uri("http://backend/")
      .body(http_body_util::Full::<Bytes>::from(""))
      .unwrap()
  }

  #[tokio::test]
  async fn idle_since_follows_the_requests() {
    let before = Instant::now();
    let (send_req, driver, mut server_io, response) = running_request().await;
    assert_eq!(send_req.idle_since(), None, "a request runs");

    let mut answer = record(RecordType::Stdout, b"Content-Type: text/plain\r\n\r\ndone");
    answer.extend(record(RecordType::Stdout, b""));
    answer.extend(end_request());
    let answered = Instant::now();
    server_io.write_all(&answer).await.unwrap();
    assert!(settles(&response).await, "the response ends");
    assert_eq!(
      response.await.unwrap().expect("a response"),
      Bytes::from_static(b"done")
    );
    let idle = send_req.idle().await;
    assert_eq!(idle, Settled::Idle);
    match send_req.idle_since() {
      Some(since) => assert!(since >= answered && since > before, "idle since the request ended"),
      None => panic!("the connection is idle"),
    }
    driver.abort();
  }

  #[tokio::test]
  async fn a_new_connection_is_idle_since_its_handshake() {
    let before = Instant::now();
    let (client_io, server_io) = tokio::io::duplex(65536);
    let (send_req, connection) = handshake::<tokio::io::DuplexStream, http_body_util::Full<Bytes>>(client_io, true)
      .await
      .unwrap();
    match send_req.idle_since() {
      Some(since) => assert!(since >= before),
      None => panic!("no request runs"),
    }
    drop((connection, server_io));
  }

  #[tokio::test]
  async fn close_lets_the_running_request_end_and_then_ends_the_connection() {
    let (send_req, driver, mut server_io, response) = running_request().await;
    assert!(send_req.close(), "this call closes the connection");
    assert!(!send_req.close(), "it is closed already");
    assert!(send_req.is_closed());

    match send_req.send_request(get(), CgiBuilder::new()).await {
      Ok(response) => panic!("a closed connection sent a request: {:?}", response.0.status()),
      Err(err) => assert_eq!(not_sent_of(&err), Some(NotSent::Closed)),
    }
    assert_eq!(send_req.in_flight(), 1, "the refused request isn't counted");

    let mut answer = record(RecordType::Stdout, b"Content-Type: text/plain\r\n\r\ndone");
    answer.extend(record(RecordType::Stdout, b""));
    answer.extend(end_request());
    server_io.write_all(&answer).await.unwrap();
    assert!(settles(&response).await, "the running request ends");
    assert_eq!(
      response.await.unwrap().expect("a response"),
      Bytes::from_static(b"done")
    );
    assert!(settles(&driver).await, "the connection ends after it");
    assert_eq!(driver.await.unwrap().expect("without an error"), Closed::ByClient);
    drop((send_req, server_io));
  }

  #[tokio::test]
  async fn a_request_waiting_when_the_server_closes_was_not_sent() {
    let (client_io, mut server_io) = tokio::io::duplex(65536);
    let (send_req, connection) = Builder::new()
      .keepalive(true)
      .max_requests(1)
      .handshake::<tokio::io::DuplexStream, http_body_util::Full<Bytes>>(client_io)
      .await
      .unwrap();
    let driver = tokio::spawn(connection);
    let requests = [send_req.clone(), send_req.clone()].map(|sender| {
      tokio::spawn(async move {
        match sender.send_request(get(), CgiBuilder::new()).await {
          Ok(response) => Ok(response.0.status()),
          Err(err) => Err(err),
        }
      })
    });
    let mut request = vec![0u8; 4096];
    let read = server_io.read(&mut request).await.unwrap();
    assert!(read > 0, "the first request arrives");
    assert!(comes_true(|| send_req.in_flight() == 2).await, "the second one waits");

    drop(server_io);
    let mut outcomes = Vec::new();
    for request in requests {
      assert!(settles(&request).await, "each request ends");
      match request.await.unwrap() {
        Ok(status) => panic!("no response comes: {status}"),
        Err(err) => outcomes.push((err.kind(), not_sent_of(&err))),
      }
    }
    outcomes.sort_by_key(|(kind, why)| (*why == Some(NotSent::Closed), format!("{kind:?}")));
    assert_eq!(
      outcomes,
      [
        (std::io::ErrorKind::UnexpectedEof, None),
        (std::io::ErrorKind::ConnectionAborted, Some(NotSent::Closed)),
      ],
      "the running request was cut off, the waiting one never sent"
    );
    assert!(settles(&driver).await);
    assert!(driver.await.unwrap().is_err(), "a request was cut off");
  }

  #[tokio::test]
  async fn try_send_request_gives_a_request_back_whole_once_closed() {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let (send_req, connection) = handshake::<tokio::io::DuplexStream, http_body_util::Full<Bytes>>(client_io, true)
      .await
      .unwrap();
    assert!(send_req.close());
    let req = http::Request::builder()
      .uri("http://backend/marked")
      .header("x-marker", "kept")
      .body(http_body_util::Full::<Bytes>::from("payload"))
      .unwrap();
    match send_req.try_send_request(req, CgiBuilder::new()) {
      Ok(response) => {
        drop(response);
        panic!("a closed connection took the request");
      }
      Err(unsent) => {
        let Unsent { request, builder } = *unsent;
        drop(builder);
        assert_eq!(request.uri(), "http://backend/marked");
        assert_eq!(request.headers()["x-marker"], "kept");
        let body = request.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(b"payload"));
      }
    }
    assert_eq!(send_req.in_flight(), 0, "the request given back isn't counted");
    assert!(send_req.idle_since().is_some());
    drop((connection, server_io));
  }

  #[tokio::test]
  async fn try_send_request_counts_the_request_at_once_and_sends_it() {
    let (client_io, mut server_io) = tokio::io::duplex(65536);
    let (send_req, connection) = handshake::<tokio::io::DuplexStream, http_body_util::Full<Bytes>>(client_io, true)
      .await
      .unwrap();
    let driver = tokio::spawn(connection);
    let response = match send_req.try_send_request(get(), CgiBuilder::new()) {
      Ok(response) => response,
      Err(unsent) => panic!("an open connection gave back {}", unsent.request.uri()),
    };
    assert_eq!(send_req.in_flight(), 1, "counted before anything is awaited");
    assert_eq!(send_req.idle_since(), None);
    let response = tokio::spawn(async move {
      let (response, stderr) = response.await?;
      drop(stderr);
      let body = response.into_body().collect().await?;
      Ok::<Bytes, std::io::Error>(body.to_bytes())
    });
    let mut request = vec![0u8; 4096];
    let read = server_io.read(&mut request).await.unwrap();
    assert!(read > 0, "the request arrives");
    let mut answer = record(RecordType::Stdout, b"Content-Type: text/plain\r\n\r\ndone");
    answer.extend(record(RecordType::Stdout, b""));
    answer.extend(end_request());
    server_io.write_all(&answer).await.unwrap();
    assert!(settles(&response).await, "the response ends");
    assert_eq!(
      response.await.unwrap().expect("a response"),
      Bytes::from_static(b"done")
    );
    driver.abort();
  }

  /// Yields until `done` holds, as long as a ready task could take; whether
  /// it does by then.
  async fn comes_true(done: impl Fn() -> bool) -> bool {
    let mut rounds = 0;
    while rounds < 2000 && !done() {
      tokio::task::yield_now().await;
      rounds += 1;
    }
    done()
  }
}

#[cfg(test)]
mod hardening_tests {
  //! How the client holds up against servers and callers that misbehave:
  //! slow or missing readers on either side, bodies that fail, and aborts
  //! the server doesn't answer. Most were known issues first; a test's
  //! comment says what happened before, where it helps.

  use std::sync::atomic::AtomicUsize;

  use http_body::Frame;
  use http_body_util::{combinators::BoxBody, Full, StreamBody};
  use tokio::io::AsyncWriteExt;

  use super::cut_off_tests::{record_for, settles};
  use super::*;
  use crate::protocol::codec::{Decoder, Encoder};

  type Server = FramedRead<ReadHalf<tokio::io::DuplexStream>, Decoder>;
  type Streamed = BoxBody<Bytes, std::io::Error>;

  /// Yields `rounds` times, so ready tasks can run.
  async fn yield_rounds(rounds: usize) {
    let mut done = 0;
    while done < rounds {
      tokio::task::yield_now().await;
      done += 1;
    }
  }

  /// Yields until `done` holds, as long as a ready task could take; whether
  /// it does by then.
  async fn comes_true(done: impl Fn() -> bool) -> bool {
    let mut rounds = 0;
    while rounds < 2000 && !done() {
      tokio::task::yield_now().await;
      rounds += 1;
    }
    done()
  }

  /// Reads records until a request's STDIN ends; that request's ID and the
  /// STDIN it got.
  async fn read_request(server: &mut Server) -> (u16, Vec<u8>) {
    let mut stdin = Vec::new();
    loop {
      let record = server.next().await.expect("a record").expect("a valid record");
      if record.record_type == RecordType::Stdin.as_u8() {
        if record.content.is_empty() {
          return (record.request_id, stdin);
        }
        stdin.extend_from_slice(&record.content);
      }
    }
  }

  fn request<B>(body: B) -> http::Request<B> {
    http::Request::builder().uri("http://backend/").body(body).unwrap()
  }

  /// Without multiplexing, a response nobody reads holds up the connection:
  /// once its channel is full, the connection reads nothing more, so the
  /// server waits for the reader. That is the backpressure a server that
  /// runs one request at a time, php-fpm say, needs.
  #[tokio::test]
  async fn without_multiplexing_an_unread_response_holds_up_the_connection() {
    let (client_io, server_io) = tokio::io::duplex(1 << 20);
    let (send_req, connection) = handshake::<tokio::io::DuplexStream, Full<Bytes>>(client_io, true)
      .await
      .unwrap();
    let driver = tokio::spawn(connection);
    let (server_read, mut server_write) = tokio::io::split(server_io);
    let mut server = FramedRead::new(server_read, Decoder::default());

    let sender = send_req.clone();
    let unread = tokio::spawn(async move { sender.send_request(request(Full::default()), CgiBuilder::new()).await });
    let (unread_id, unread_stdin) = read_request(&mut server).await;
    assert!(unread_stdin.is_empty());
    let sender = send_req.clone();
    let read = tokio::spawn(async move {
      let (response, stderr) = sender.send_request(request(Full::default()), CgiBuilder::new()).await?;
      drop(stderr);
      let body = response.into_body().collect().await?;
      Ok::<Bytes, std::io::Error>(body.to_bytes())
    });
    let (read_id, read_stdin) = read_request(&mut server).await;
    assert!(read_stdin.is_empty());

    // Far more STDOUT for the first request than its channel holds, then the
    // whole second response.
    let mut answer = record_for(RecordType::Stdout, unread_id, b"Content-Type: text/plain\r\n\r\n");
    answer.extend(std::iter::repeat_n(record_for(RecordType::Stdout, unread_id, &[b'u'; 1000]), 200).flatten());
    answer.extend(record_for(
      RecordType::Stdout,
      read_id,
      b"Content-Type: text/plain\r\n\r\nread",
    ));
    answer.extend(record_for(RecordType::Stdout, read_id, b""));
    answer.extend(record_for(RecordType::EndRequest, read_id, &[0; 8]));
    server_write.write_all(&answer).await.unwrap();

    assert!(settles(&unread).await, "the unread response's headers arrive");
    let (unread, unread_stderr) = unread.await.unwrap().expect("a response");
    assert!(!settles(&read).await, "the other response waits");
    assert_eq!(send_req.in_flight(), 2);

    // Counter-check: the unread response alone holds the other one up.
    drop((unread, unread_stderr));
    assert!(
      settles(&read).await,
      "the other response ends once the unread one is gone"
    );
    assert_eq!(read.await.unwrap().expect("a response"), Bytes::from_static(b"read"));
    driver.abort();
  }

  /// A response that is read, on a multiplexed connection, behind one that
  /// isn't read.
  struct BehindAnUnreadResponse {
    send_req: SendRequest<Full<Bytes>>,
    driver: tokio::task::JoinHandle<Result<Closed, std::io::Error>>,
    server: Server,
    server_write: WriteHalf<tokio::io::DuplexStream>,
    unread_id: u16,
    // The response nobody reads yet, once its headers came.
    unread: http::Response<CgiIncoming<cegla::client::CgiResponseInner<FcgiResponseReader>>>,
    unread_stderr: FcgiResponseReader,
    // The response that is read: its body, once it ended.
    read: tokio::task::JoinHandle<Result<Bytes, std::io::Error>>,
  }

  /// How much of the unread response's body the server sends.
  const UNREAD_BODY: usize = 200 * 1000;

  /// Two requests on a connection made with `max_unread` to a server that
  /// multiplexes: the first one's response has far more STDOUT than its
  /// channel holds and nobody reads it; the second one's is whole.
  async fn behind_an_unread_response(max_unread: usize) -> BehindAnUnreadResponse {
    use tokio::io::AsyncWriteExt;

    let (client_io, server_io) = tokio::io::duplex(1 << 20);
    let (server_read, mut server_write) = tokio::io::split(server_io);
    let mut server = FramedRead::new(server_read, Decoder::default());
    let builder = Builder::new()
      .keepalive(true)
      .get_values(true)
      .max_unread(max_unread)
      .clone();
    let handshake = tokio::spawn(async move {
      builder
        .handshake::<tokio::io::DuplexStream, Full<Bytes>>(client_io)
        .await
    });
    let get_values = server.next().await.expect("a record").expect("a valid record");
    assert_eq!(get_values.record_type, RecordType::GetValues.as_u8());
    let values = NameValuePair::from_slices(b"FCGI_MPXS_CONNS", b"1").encode();
    server_write
      .write_all(&record_for(RecordType::GetValuesResult, 0, &values))
      .await
      .unwrap();
    let (send_req, connection) = handshake.await.unwrap().unwrap();
    let driver = tokio::spawn(connection);

    let sender = send_req.clone();
    let unread = tokio::spawn(async move { sender.send_request(request(Full::default()), CgiBuilder::new()).await });
    let (unread_id, unread_stdin) = read_request(&mut server).await;
    assert!(unread_stdin.is_empty());
    let sender = send_req.clone();
    let read = tokio::spawn(async move {
      let (response, stderr) = sender.send_request(request(Full::default()), CgiBuilder::new()).await?;
      drop(stderr);
      let body = response.into_body().collect().await?;
      Ok::<Bytes, std::io::Error>(body.to_bytes())
    });
    let (read_id, read_stdin) = read_request(&mut server).await;
    assert!(read_stdin.is_empty());

    let mut answer = record_for(RecordType::Stdout, unread_id, b"Content-Type: text/plain\r\n\r\n");
    answer.extend(
      std::iter::repeat_n(
        record_for(RecordType::Stdout, unread_id, &[b'u'; 1000]),
        UNREAD_BODY / 1000,
      )
      .flatten(),
    );
    answer.extend(record_for(
      RecordType::Stdout,
      read_id,
      b"Content-Type: text/plain\r\n\r\nread",
    ));
    answer.extend(record_for(RecordType::Stdout, read_id, b""));
    answer.extend(record_for(RecordType::EndRequest, read_id, &[0; 8]));
    server_write.write_all(&answer).await.unwrap();

    assert!(settles(&unread).await, "the unread response's headers arrive");
    let (unread, unread_stderr) = unread.await.unwrap().expect("a response");
    BehindAnUnreadResponse {
      send_req,
      driver,
      server,
      server_write,
      unread_id,
      unread,
      unread_stderr,
      read,
    }
  }

  /// On a multiplexed connection, a response nobody reads holds up only
  /// itself: the connection keeps what waits, up to `max_unread`, and reads
  /// on for the other requests. The waiting response is whole once read.
  #[tokio::test]
  async fn an_unread_response_holds_up_only_itself() {
    use tokio::io::AsyncWriteExt;

    let mut behind = behind_an_unread_response(DEFAULT_MAX_UNREAD).await;
    assert!(settles(&behind.read).await, "the other response ends");
    assert_eq!(
      behind.read.await.unwrap().expect("a response"),
      Bytes::from_static(b"read")
    );
    assert_eq!(behind.send_req.in_flight(), 1, "the unread request runs on");

    let mut end = record_for(RecordType::Stdout, behind.unread_id, b"");
    end.extend(record_for(RecordType::EndRequest, behind.unread_id, &[0; 8]));
    behind.server_write.write_all(&end).await.unwrap();
    let body = behind.unread.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.len(), UNREAD_BODY, "nothing of it was lost");
    assert!(body.iter().all(|&byte| byte == b'u'));
    assert!(
      comes_true(|| behind.send_req.in_flight() == 0).await,
      "the request has finished"
    );
    behind.driver.abort();
    drop((behind.server, behind.unread_stderr));
  }

  /// On a multiplexed connection, a response of which more waits than
  /// `max_unread` is given up: its request is aborted, and reading it fails
  /// with [`ResponseUnread`]; the other requests run on.
  #[tokio::test]
  async fn a_response_unread_past_max_unread_is_given_up() {
    use tokio::io::AsyncWriteExt;

    let mut behind = behind_an_unread_response(64 * 1024).await;
    assert!(settles(&behind.read).await, "the other response ends");
    assert_eq!(
      behind.read.await.unwrap().expect("a response"),
      Bytes::from_static(b"read")
    );
    loop {
      let record = behind.server.next().await.expect("a record").expect("a valid record");
      if record.record_type == RecordType::AbortRequest.as_u8() {
        assert_eq!(record.request_id, behind.unread_id);
        break;
      }
    }
    match behind.unread.into_body().collect().await {
      Ok(body) => panic!(
        "the response was given up, yet {} bytes read to its end",
        body.to_bytes().len()
      ),
      Err(err) => assert_eq!(
        err.get_ref().and_then(|inner| inner.downcast_ref::<ResponseUnread>()),
        Some(&ResponseUnread)
      ),
    }

    // The server ends the aborted request; the connection runs on.
    behind
      .server_write
      .write_all(&record_for(RecordType::EndRequest, behind.unread_id, &[0; 8]))
      .await
      .unwrap();
    assert!(
      comes_true(|| behind.send_req.in_flight() == 0).await,
      "the aborted request has finished"
    );
    assert!(!behind.driver.is_finished());
    assert!(!behind.send_req.is_closed());
    behind.driver.abort();
    drop(behind.unread_stderr);
  }

  /// Sleeps on a thread of its own: the tests' runtime has no timer.
  struct ThreadTimer;

  impl Timer for ThreadTimer {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
      let (done_tx, done_rx) = tokio::sync::oneshot::channel();
      std::thread::spawn(move || {
        std::thread::sleep(duration);
        if let Err(()) = done_tx.send(()) {
          // The connection stopped waiting: there is nothing to wake.
        }
      });
      Box::pin(async move {
        match done_rx.await {
          Ok(()) => {}
          Err(gone) => panic!("the timer's thread ended: {gone}"),
        }
      })
    }
  }

  async fn wait(duration: Duration) {
    ThreadTimer.sleep(duration).await;
  }

  type ServerOut = FramedWrite<WriteHalf<tokio::io::DuplexStream>, Encoder>;

  /// A connection made by `builder` to a server that multiplexes, with a
  /// request whose response was dropped: the server got
  /// `FCGI_ABORT_REQUEST` for it and hasn't answered.
  async fn an_unanswered_abort(
    builder: &Builder,
  ) -> (
    SendRequest<Full<Bytes>>,
    tokio::task::JoinHandle<Result<Closed, std::io::Error>>,
    u16,
    Server,
    ServerOut,
  ) {
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let (server_read, server_write) = tokio::io::split(server_io);
    let mut server = FramedRead::new(server_read, Decoder::default());
    let mut server_out = FramedWrite::new(server_write, Encoder);
    let backend = tokio::spawn(async move {
      let get_values = server.next().await.expect("a record").expect("a valid record");
      assert_eq!(get_values.record_type, RecordType::GetValues.as_u8());
      let values = NameValuePair::from_slices(b"FCGI_MPXS_CONNS", b"1").encode().to_vec();
      server_out
        .send(Record::new(RecordType::GetValuesResult.as_u8(), 0, values))
        .await
        .unwrap();
      let (request_id, stdin) = read_request(&mut server).await;
      assert!(stdin.is_empty());
      let headers = b"Content-Type: text/plain\r\n\r\n".to_vec();
      server_out
        .send(Record::new(RecordType::Stdout.as_u8(), request_id, headers))
        .await
        .unwrap();
      loop {
        let record = server.next().await.expect("a record").expect("a valid record");
        if record.record_type == RecordType::AbortRequest.as_u8() {
          // The connection stays open, the request unanswered.
          return (request_id, server, server_out);
        }
      }
    });
    let (send_req, connection) = builder
      .handshake::<tokio::io::DuplexStream, Full<Bytes>>(client_io)
      .await
      .unwrap();
    let driver = tokio::spawn(connection);
    let (response, stderr) = send_req
      .send_request(request(Full::default()), CgiBuilder::new())
      .await
      .unwrap();
    drop((response, stderr));

    assert!(settles(&backend).await, "the server gets FCGI_ABORT_REQUEST");
    let (request_id, server, server_out) = backend.await.unwrap();
    (send_req, driver, request_id, server, server_out)
  }

  /// Without an abort timeout, a request whose `FCGI_ABORT_REQUEST` the
  /// server never answers keeps its place as long as the connection is
  /// open.
  #[tokio::test]
  async fn without_an_abort_timeout_an_unanswered_abort_keeps_its_place() {
    let builder = Builder::new().keepalive(true).get_values(true).clone();
    let (send_req, driver, request_id, server, mut server_out) = an_unanswered_abort(&builder).await;
    yield_rounds(2000).await;
    assert!(!driver.is_finished(), "the connection runs on");
    assert_eq!(send_req.in_flight(), 1, "the aborted request keeps its place");

    // Counter-check: the place is free once the server ends the request.
    server_out
      .send(Record::new(RecordType::EndRequest.as_u8(), request_id, vec![0; 8]))
      .await
      .unwrap();
    assert!(
      comes_true(|| send_req.in_flight() == 0).await,
      "the ended request gives up its place"
    );
    driver.abort();
    drop((server, server_out));
  }

  /// With an abort timeout, the connection ends once the server hasn't
  /// answered an abort for that long, and not before.
  #[tokio::test]
  async fn an_unanswered_abort_ends_the_connection_after_the_abort_timeout() {
    let after = Duration::from_millis(100);
    let builder = Builder::new()
      .keepalive(true)
      .get_values(true)
      .abort_timeout(after, Arc::new(ThreadTimer))
      .clone();
    let (send_req, driver, request_id, server, server_out) = an_unanswered_abort(&builder).await;
    wait(Duration::from_millis(30)).await;
    assert!(!driver.is_finished(), "not yet: the server may still end the request");
    wait(Duration::from_millis(200)).await;
    assert!(driver.is_finished(), "the abort timeout ended the connection");
    match driver.await.unwrap() {
      Ok(closed) => panic!("the connection ends with an error, not {closed:?}"),
      Err(err) => {
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(
          err.get_ref().and_then(|inner| inner.downcast_ref::<AbortUnanswered>()),
          Some(&AbortUnanswered(after))
        );
      }
    }
    assert!(send_req.is_closed(), "the connection takes no more requests");
    assert_eq!(send_req.idle().await, Settled::Ended);
    drop((request_id, server, server_out));
  }

  /// An abort the server answers in time doesn't end the connection.
  #[tokio::test]
  async fn an_answered_abort_keeps_the_connection() {
    let builder = Builder::new()
      .keepalive(true)
      .get_values(true)
      .abort_timeout(Duration::from_millis(100), Arc::new(ThreadTimer))
      .clone();
    let (send_req, driver, request_id, server, mut server_out) = an_unanswered_abort(&builder).await;
    server_out
      .send(Record::new(RecordType::EndRequest.as_u8(), request_id, vec![0; 8]))
      .await
      .unwrap();
    assert!(
      comes_true(|| send_req.in_flight() == 0).await,
      "the ended request gives up its place"
    );
    wait(Duration::from_millis(250)).await;
    assert!(!driver.is_finished(), "the connection runs on");
    assert!(!send_req.is_closed());
    driver.abort();
    drop((server, server_out));
  }

  /// STDIN goes to the server about as fast as the server reads it: the
  /// connection takes no more of the body than [`MAX_QUEUED`] ahead of what
  /// the server has read, and the whole body once the server reads it.
  /// Before: it took the whole body at once and queued it in memory.
  #[tokio::test]
  async fn a_request_body_goes_as_fast_as_the_server_reads() {
    let ahead = send_a_body_to_a_slow_server(4096).await;
    // The queue, the writer's buffer and the pipe.
    assert!(
      ahead <= MAX_QUEUED + 32 * 1024,
      "{} KiB of the body taken ahead of the server",
      ahead / 1024
    );
  }

  /// A pipe that takes a whole queue at once leaves nothing to wake the
  /// connection for more of the body: it wakes itself.
  #[tokio::test]
  async fn a_request_body_goes_on_when_the_pipe_takes_the_queue() {
    send_a_body_to_a_slow_server(1 << 20).await;
  }

  /// Sends a 1 MiB body to a server that reads nothing at first, through a
  /// pipe of `pipe` bytes; how much of it the connection took by then. Then
  /// the server reads the whole body.
  async fn send_a_body_to_a_slow_server(pipe: usize) -> usize {
    const CHUNKS: usize = 256;
    const CHUNK: usize = 4096;
    let (client_io, server_io) = tokio::io::duplex(pipe);
    let (send_req, connection) = handshake::<tokio::io::DuplexStream, Streamed>(client_io, true)
      .await
      .unwrap();
    let taken = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&taken);
    let frames = (0..CHUNKS).map(move |chunk| {
      counter.fetch_add(1, Ordering::Relaxed);
      let byte = u8::try_from(chunk % 256).expect("below 256");
      Ok::<Frame<Bytes>, std::io::Error>(Frame::data(Bytes::from(vec![byte; CHUNK])))
    });
    let body = BoxBody::new(StreamBody::new(futures_util::stream::iter(frames)));
    let sending = tokio::spawn(async move { send_req.send_request(request(body), CgiBuilder::new()).await });
    let driver = tokio::spawn(connection);

    yield_rounds(2000).await;
    let ahead = taken.load(Ordering::Relaxed) * CHUNK;
    assert!(ahead > 0, "the body starts");

    // Counter-check: the server reading takes the whole body, in order.
    let (server_read, server_write) = tokio::io::split(server_io);
    let mut server = FramedRead::new(server_read, Decoder::default());
    let (request_id, stdin) = read_request(&mut server).await;
    assert_eq!(taken.load(Ordering::Relaxed), CHUNKS);
    assert_eq!(stdin.len(), CHUNKS * CHUNK);
    for (index, chunk) in stdin.chunks(CHUNK).enumerate() {
      let byte = u8::try_from(index % 256).expect("below 256");
      assert!(
        chunk.iter().all(|&got| got == byte),
        "chunk {index} of request {request_id}"
      );
    }
    assert!(!sending.is_finished(), "the server has not answered");
    sending.abort();
    driver.abort();
    drop(server_write);
    ahead
  }

  /// A body that fails after "part".
  fn failing_body() -> Streamed {
    let frames: Vec<Result<Frame<Bytes>, std::io::Error>> = vec![
      Ok(Frame::data(Bytes::from_static(b"part"))),
      Err(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
    ];
    BoxBody::new(StreamBody::new(futures_util::stream::iter(frames)))
  }

  /// Whether `result` failed with the body's error inside
  /// [`RequestBodyFailed`].
  fn body_failed(result: &ResponseResult) -> bool {
    match result {
      Ok((response, stderr)) => panic!("no response comes: {:?} {}", response.status(), stderr.ended),
      Err(err) => match err
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<RequestBodyFailed>())
      {
        Some(failed) => match failed.0.downcast_ref::<std::io::Error>() {
          Some(source) => source.kind() == std::io::ErrorKind::ConnectionReset,
          None => panic!("the body's own error is inside: {failed}"),
        },
        None => panic!("the body failed, not: {err}"),
      },
    }
  }

  /// A body that fails aborts its request on a server that doesn't
  /// multiplex too: STDIN doesn't end, the caller gets the body's error, and
  /// the connection takes no other request and ends once the server has
  /// ended this one.
  /// Before: the connection ended at once with `BodyCutOff`, no abort.
  #[tokio::test]
  async fn a_failed_request_body_aborts_its_request_without_multiplexing() {
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let (server_read, server_write) = tokio::io::split(server_io);
    let mut server = FramedRead::new(server_read, Decoder::default());
    let server_out = FramedWrite::new(server_write, Encoder);
    let backend = tokio::spawn(async move {
      let mut stdin = Vec::new();
      loop {
        let record = server.next().await.expect("a record").expect("a valid record");
        if record.record_type == RecordType::Stdin.as_u8() {
          assert!(!record.content.is_empty(), "STDIN doesn't end");
          stdin.extend_from_slice(&record.content);
        }
        if record.record_type == RecordType::AbortRequest.as_u8() {
          return (record.request_id, stdin, server, server_out);
        }
      }
    });
    let (send_req, connection) = handshake::<tokio::io::DuplexStream, Streamed>(client_io, true)
      .await
      .unwrap();
    let driver = tokio::spawn(connection);
    let result = send_req.send_request(request(failing_body()), CgiBuilder::new()).await;
    assert!(body_failed(&result));

    assert!(settles(&backend).await, "the server gets FCGI_ABORT_REQUEST");
    let (request_id, stdin, server, mut server_out) = backend.await.unwrap();
    assert_eq!(stdin, b"part");
    assert!(send_req.is_closed(), "no other request after the abort");
    assert!(!driver.is_finished(), "the connection waits for the server");
    server_out
      .send(Record::new(RecordType::EndRequest.as_u8(), request_id, vec![0; 8]))
      .await
      .unwrap();
    assert!(settles(&driver).await, "the connection ends");
    match driver.await.unwrap() {
      Ok(Closed::ByClient | Closed::ByServer) => {}
      Err(err) => panic!("the connection ends without an error: {err}"),
    }
    assert_eq!(send_req.idle().await, Settled::Ended);
    drop((server, server_out));
  }

  /// A body that fails aborts its request on a server that multiplexes; the
  /// connection runs on, and the request's place is free once the server
  /// has ended it.
  #[tokio::test]
  async fn a_failed_request_body_aborts_its_request_on_a_multiplexed_connection() {
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let (server_read, server_write) = tokio::io::split(server_io);
    let mut server = FramedRead::new(server_read, Decoder::default());
    let mut server_out = FramedWrite::new(server_write, Encoder);
    let backend = tokio::spawn(async move {
      let get_values = server.next().await.expect("a record").expect("a valid record");
      assert_eq!(get_values.record_type, RecordType::GetValues.as_u8());
      let values = NameValuePair::from_slices(b"FCGI_MPXS_CONNS", b"1").encode().to_vec();
      server_out
        .send(Record::new(RecordType::GetValuesResult.as_u8(), 0, values))
        .await
        .unwrap();
      let mut stdin = Vec::new();
      loop {
        let record = server.next().await.expect("a record").expect("a valid record");
        if record.record_type == RecordType::Stdin.as_u8() {
          assert!(!record.content.is_empty(), "STDIN doesn't end");
          stdin.extend_from_slice(&record.content);
        }
        if record.record_type == RecordType::AbortRequest.as_u8() {
          return (record.request_id, stdin, server, server_out);
        }
      }
    });
    let builder = Builder::new().keepalive(true).get_values(true).clone();
    let (send_req, connection) = builder
      .handshake::<tokio::io::DuplexStream, Streamed>(client_io)
      .await
      .unwrap();
    let driver = tokio::spawn(connection);
    let result = send_req.send_request(request(failing_body()), CgiBuilder::new()).await;
    assert!(body_failed(&result));

    assert!(settles(&backend).await, "the server gets FCGI_ABORT_REQUEST");
    let (request_id, stdin, server, mut server_out) = backend.await.unwrap();
    assert_eq!(stdin, b"part");
    assert!(!driver.is_finished(), "the connection runs on");
    assert!(!send_req.is_closed());
    server_out
      .send(Record::new(RecordType::EndRequest.as_u8(), request_id, vec![0; 8]))
      .await
      .unwrap();
    assert!(
      comes_true(|| send_req.in_flight() == 0).await,
      "the ended request gives up its place"
    );
    driver.abort();
    drop((server, server_out));
  }
}
