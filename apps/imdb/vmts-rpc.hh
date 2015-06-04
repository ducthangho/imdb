// Copyright (c) 2013, Kenton Varda <temporal@gmail.com>
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef CAPNP_Vmts_RPC_H_
#define CAPNP_Vmts_RPC_H_

#if defined(__GNUC__) && !CAPNP_HEADER_WARNINGS
#pragma GCC system_header
#endif

#include "uv.h"
#include "capnp/rpc.h"
#include "capnp/message.h"
#include "core/shared_ptr.hh"
#include "core/reactor.hh"
#include "event_port.hh"

struct sockaddr;

namespace kj { class AsyncIoProvider; class LowLevelAsyncIoProvider;}

namespace capnp {

class VmtsRpcContext;

class VmtsRpcClient {
  // Super-simple interface for setting up a Cap'n Proto RPC client.  Example:
  //
  //     # Cap'n Proto schema
  //     interface Adder {
  //       add @0 (left :Int32, right :Int32) -> (value :Int32);
  //     }
  //
  //     // C++ client
  //     int main() {
  //       capnp::VmtsRpcClient client("localhost:3456");
  //       Adder::Client adder = client.getMain<Adder>();
  //       auto request = adder.addRequest();
  //       request.setLeft(12);
  //       request.setRight(34);
  //       auto response = request.send().wait(client.getWaitScope());
  //       assert(response.getValue() == 46);
  //       return 0;
  //     }
  //
  //     // C++ server
  //     class AdderImpl final: public Adder::Server {
  //     public:
  //       kj::Promise<void> add(AddContext context) override {
  //         auto params = context.getParams();
  //         context.getResults().setValue(params.getLeft() + params.getRight());
  //         return kj::READY_NOW;
  //       }
  //     };
  //
  //     int main() {
  //       capnp::VmtsRpcServer server("*:3456");
  //       server.exportCap("adder", kj::heap<AdderImpl>());
  //       kj::NEVER_DONE.wait(server.getWaitScope());
  //     }
  //
  // This interface is easy, but it hides a lot of useful features available from the lower-level
  // classes:
  // - The server can only export a small set of public, singleton capabilities under well-known
  //   string names.  This is fine for transient services where no state needs to be kept between
  //   connections, but hides the power of Cap'n Proto when it comes to long-lived resources.
  // - VmtsRpcClient/VmtsRpcServer automatically set up a `kj::EventLoop` and make it current for the
  //   thread.  Only one `kj::EventLoop` can exist per thread, so you cannot use these interfaces
  //   if you wish to set up your own event loop.  (However, you can safely create multiple
  //   VmtsRpcClient / VmtsRpcServer objects in a single thread; they will make sure to make no more
  //   than one EventLoop.)
  // - These classes only support simple two-party connections, not multilateral VatNetworks.
  // - These classes only support communication over a raw, unencrypted socket.  If you want to
  //   build on an abstract stream (perhaps one which supports encryption), you must use the
  //   lower-level interfaces.
  //
  // Some of these restrictions will probably be lifted in future versions, but some things will
  // always require using the low-level interfaces directly.  If you are interested in working
  // at a lower level, start by looking at these interfaces:
  // - `kj::startAsyncIo()` in `kj/async-io.h`.
  // - `RpcSystem` in `capnp/rpc.h`.
  // - `TwoPartyVatNetwork` in `capnp/rpc-twoparty.h`.

public:
  explicit VmtsRpcClient(kj::StringPtr serverAddress, uint defaultPort = 0,
                       ReaderOptions readerOpts = ReaderOptions());
  // Construct a new VmtsRpcClient and connect to the given address.  The connection is formed in
  // the background -- if it fails, calls to capabilities returned by importCap() will fail with an
  // appropriate exception.
  //
  // `defaultPort` is the IP port number to use if `serverAddress` does not include it explicitly.
  // If unspecified, the port is required in `serverAddress`.
  //
  // The address is parsed by `kj::Network` in `kj/async-io.h`.  See that interface for more info
  // on the address format, but basically it's what you'd expect.
  //
  // `readerOpts` is the ReaderOptions structure used to read each incoming message on the
  // connection. Setting this may be necessary if you need to receive very large individual
  // messages or messages. However, it is recommended that you instead think about how to change
  // your protocol to send large data blobs in multiple small chunks -- this is much better for
  // both security and performance. See `ReaderOptions` in `message.h` for more details.

  VmtsRpcClient(const struct sockaddr* serverAddress, uint addrSize,
              ReaderOptions readerOpts = ReaderOptions());
  // Like the above constructor, but connects to an already-resolved socket address.  Any address
  // format supported by `kj::Network` in `kj/async-io.h` is accepted.

  explicit VmtsRpcClient(int socketFd, ReaderOptions readerOpts = ReaderOptions());
  // Create a client on top of an already-connected socket.
  // `readerOpts` acts as in the first constructor.

  ~VmtsRpcClient() noexcept(false);

  template <typename Type>
  typename Type::Client getMain();
  Capability::Client getMain();
  // Get the server's main (aka "bootstrap") interface.

  template <typename Type>
  typename Type::Client importCap(kj::StringPtr name)
      KJ_DEPRECATED("Change your server to export a main interface, then use getMain() instead.");
  Capability::Client importCap(kj::StringPtr name)
      KJ_DEPRECATED("Change your server to export a main interface, then use getMain() instead.");
  // ** DEPRECATED **
  //
  // Ask the sever for the capability with the given name.  You may specify a type to automatically
  // down-cast to that type.  It is up to you to specify the correct expected type.
  //
  // Named interfaces are deprecated. The new preferred usage pattern is for the server to export
  // a "main" interface which itself has methods for getting any other interfaces.

  kj::WaitScope& getWaitScope();
  // Get the `WaitScope` for the client's `EventLoop`, which allows you to synchronously wait on
  // promises.

  kj::AsyncIoProvider& getIoProvider();
  // Get the underlying AsyncIoProvider set up by the RPC system.  This is useful if you want
  // to do some non-RPC I/O in asynchronous fashion.

  kj::LowLevelAsyncIoProvider& getLowLevelIoProvider();
  // Get the underlying LowLevelAsyncIoProvider set up by the RPC system.  This is useful if you
  // want to do some non-RPC I/O in asynchronous fashion.

  //Return the associated libuv loop
  uv_loop_t* getUvLoop();

private:
  struct Impl;
  kj::Own<Impl> impl;
};

class VmtsRpcServer {
  // The server counterpart to `VmtsRpcClient`.  See `VmtsRpcClient` for an example.

public:

  VmtsRpcServer(Capability::Client mainInterface,  
      lw_shared_ptr<kj::connection>& _conn, kj::WaitScope& ws, ReaderOptions readerOpts = ReaderOptions());

  explicit VmtsRpcServer(Capability::Client mainInterface, kj::StringPtr bindAddress,
                       uint defaultPort = 0, ReaderOptions readerOpts = ReaderOptions());
  // Construct a new `VmtsRpcServer` that binds to the given address.  An address of "*" means to
  // bind to all local addresses.
  //
  // `defaultPort` is the IP port number to use if `serverAddress` does not include it explicitly.
  // If unspecified, a port is chosen automatically, and you must call getPort() to find out what
  // it is.
  //
  // The address is parsed by `kj::Network` in `kj/async-io.h`.  See that interface for more info
  // on the address format, but basically it's what you'd expect.
  //
  // The server might not begin listening immediately, especially if `bindAddress` needs to be
  // resolved.  If you need to wait until the server is definitely up, wait on the promise returned
  // by `getPort()`.
  //
  // `readerOpts` is the ReaderOptions structure used to read each incoming message on the
  // connection. Setting this may be necessary if you need to receive very large individual
  // messages or messages. However, it is recommended that you instead think about how to change
  // your protocol to send large data blobs in multiple small chunks -- this is much better for
  // both security and performance. See `ReaderOptions` in `message.h` for more details.

  VmtsRpcServer(Capability::Client mainInterface, struct sockaddr* bindAddress, uint addrSize,
              ReaderOptions readerOpts = ReaderOptions());
  // Like the above constructor, but binds to an already-resolved socket address.  Any address
  // format supported by `kj::Network` in `kj/async-io.h` is accepted.

  VmtsRpcServer(Capability::Client mainInterface, int socketFd, uint port,
              ReaderOptions readerOpts = ReaderOptions());
  // Create a server on top of an already-listening socket (i.e. one on which accept() may be
  // called).  `port` is returned by `getPort()` -- it serves no other purpose.
  // `readerOpts` acts as in the other two above constructors.

  explicit VmtsRpcServer(kj::StringPtr bindAddress, uint defaultPort = 0,
                       ReaderOptions readerOpts = ReaderOptions())
      KJ_DEPRECATED("Please specify a main interface for your server.");
  VmtsRpcServer(struct sockaddr* bindAddress, uint addrSize,
              ReaderOptions readerOpts = ReaderOptions())
      KJ_DEPRECATED("Please specify a main interface for your server.");
  VmtsRpcServer(int socketFd, uint port, ReaderOptions readerOpts = ReaderOptions())
      KJ_DEPRECATED("Please specify a main interface for your server.");

  ~VmtsRpcServer() noexcept(false);

  void exportCap(kj::StringPtr name, Capability::Client cap);
  // Export a capability publicly under the given name, so that clients can import it.
  //
  // Keep in mind that you can implicitly convert `kj::Own<MyType::Server>&&` to
  // `Capability::Client`, so it's typical to pass something like
  // `kj::heap<MyImplementation>(<constructor params>)` as the second parameter.

  kj::Promise<uint> getPort();
  // Get the IP port number on which this server is listening.  This promise won't resolve until
  // the server is actually listening.  If the address was not an IP address (e.g. it was a Unix
  // domain socket) then getPort() resolves to zero.

  kj::WaitScope& getWaitScope();
  // Get the `WaitScope` for the client's `EventLoop`, which allows you to synchronously wait on
  // promises.

  kj::AsyncIoProvider& getIoProvider();
  // Get the underlying AsyncIoProvider set up by the RPC system.  This is useful if you want
  // to do some non-RPC I/O in asynchronous fashion.

  kj::LowLevelAsyncIoProvider& getLowLevelIoProvider();
  // Get the underlying LowLevelAsyncIoProvider set up by the RPC system.  This is useful if you
  // want to do some non-RPC I/O in asynchronous fashion.

  //Return the associated libuv loop
  uv_loop_t* getUvLoop();

private:
  struct Impl;
  kj::Own<Impl> impl;
};

// =======================================================================================
// inline implementation details

template <typename Type>
inline typename Type::Client VmtsRpcClient::getMain() {
  return getMain().castAs<Type>();
}

template <typename Type>
inline typename Type::Client VmtsRpcClient::importCap(kj::StringPtr name) {
  return importCap(name).castAs<Type>();
}

}  // namespace capnp

#endif  // CAPNP_Vmts_RPC_H_
