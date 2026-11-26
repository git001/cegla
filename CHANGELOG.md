# Changelog

## `cegla` UNRELEASED

- Replaced `vibeio-cegla` with `zincio-cegla`, following the rename of `vibeio` to `zincio`.
- Fixed FastCGI client dropping response data when the server sends more than 64 STDOUT records before they are read.
- Fixed FastCGI client dropping error output when the server sends more than 64 STDERR records before they are read.
- Fixed FastCGI client requests hanging, and their connection never ending, when the server closes the connection before it ends them: such requests now fail with `UnexpectedEof`, a response body cut off this way fails instead of ending early, and the connection resolves to an error.
- Added `Builder` for FastCGI client connections, with a limit on the requests a connection runs at the same time (`max_requests`), and `SendRequest::is_ready` and `SendRequest::max_requests`.
- Added `Builder::get_values`, which asks the FastCGI server for `FCGI_MAX_REQS` and `FCGI_MPXS_CONNS` during the handshake and limits the requests a connection runs at the same time to what the server supports.
- The FastCGI client now handles the protocol status in `FCGI_END_REQUEST`: a rejected request fails with `RequestRejected`, `FCGI_CANT_MPX_CONN` limits the connection to one request at a time, and `FCGI_OVERLOADED` or `FCGI_UNKNOWN_ROLE` closes the connection to new requests.
- The FastCGI client now sends `FCGI_ABORT_REQUEST` when a response is dropped before it ends, the caller stops waiting for its headers, or the request body fails, so the server can stop the request. Every server gets it. A connection to a server that doesn't multiplex takes no other request after an abort and ends once the server has ended the aborted one: such a server may read the abort only after its request, as php-fpm does, which reads it as the start of the next request.
- FastCGI client's `SendRequest` is now `Clone`; clones send over the same connection.
- Added `SendRequest::in_flight`, the number of requests on a FastCGI client connection that haven't finished.
- Added `SendRequest::idle`, which waits until no request runs on a FastCGI client connection any more, or the connection has ended (`Settled::Idle`, `Settled::Ended`).
- Added `SendRequest::idle_since`, since when no request runs on a FastCGI client connection, and `SendRequest::close`, which stops it from taking new requests and ends it once the running ones have.
- Added `SendRequest::try_send_request`, which hands a request to a FastCGI client connection at once and gives it back whole (`Unsent`) if the connection doesn't take requests any more, so that it can be sent on another connection.
- A FastCGI client request that the connection never sent now fails with a `NotSent` error inside the `std::io::Error` (`NotSent::Closed`, `NotSent::NoRequestId`), so a caller can tell that sending it again is safe; the error kinds stay as they were, except that a request sent on a closed connection now fails with `NotSent::Closed` instead of "underlying connection closed".
- A FastCGI client `Connection` now resolves to `Closed` when it ends without an error, saying whether the server closed it (`Closed::ByServer`) or every `SendRequest` was dropped (`Closed::ByClient`), instead of `()`.

## `cegla` 0.2.3 - April 25, 2026

- Fixed EOF not being sent for error stream when the client receives FastCGI END_REQUEST record.
- Improved the handling of closed FastCGI client connections (closed by the server).

## `cegla` 0.2.2 - April 21, 2026

- Added a function for checking if FastCGI connection is closed in `SendRequest`.

## `cegla` 0.2.1 - April 19, 2026

- Added `vibeio-cegla` (support for `vibeio` async runtime)
- Added `cegla-fcgi` (a high-level FastCGI implementation for Rust)

## `cegla` 0.2.0 - March 22, 2026

- Added `cwd` argument to `start_child` function of `Runtime` trait in `cegla-cgi`
- The `CONTENT_LENGTH` CGI variable is now overridden if it's present before building a `CgiEnvironment`
- Improved HTTP header environment variable handling consistency between client-side and server-side implementations

## `cegla` 0.1.2 - January 24, 2026

- Added `cegla-scgi` (a high-level SCGI implementation for Rust)
- Disabled `cgi-client` feature of `tokio-cegla` by default

## `cegla` 0.1.1 - January 24, 2026

- Added `cegla-cgi` (a high-level CGI implementation for Rust)
- Added `tokio-cegla` (a Tokio-based runtime support for `cegla-*` crates)

## `cegla` 0.1.0 - January 23, 2026

- First release
