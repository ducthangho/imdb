// Copyright (c) 2014 Sandstorm Development Group, Inc. and contributors
// Licensed under the MIT License:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#if __cplusplus >= 201300
// Hack around stdlib bug with C++14.
#include <initializer_list> // force libstdc++ to include its config
#undef _GLIBCXX_HAVE_GETS // correct broken config
// End hack.
#endif
#include <uv.h>
#include "capnp/rpc.h"
#include <capnp/dynamic.h>
#include <capnp/schema-parser.h>
#include <kj/debug.h>
#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/vector.h>
#include <errno.h>
#include <unistd.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/rpc.capnp.h>
#include <capnp/serialize.h>
#include <unordered_map>
#include <inttypes.h>
#include <set>
#include <stdlib.h>
#include <sys/uio.h>
#include <kj/threadlocal.h>
#include "core/reactor.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/bitops.hh"
#include "core/slab.hh"
#include "core/align.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include "core/do_with.hh"
#include <capnp/rpc-twoparty.h>

#include <typeinfo>
#include <typeindex>
#include <cxxabi.h>



namespace kj { // so we get warnings if anything declared in this file is left undefined...

typedef unsigned char byte;
typedef unsigned int uint;

// =======================================================================================
// KJ <-> libuv glue.

// #define UV_CALL(code, loop, ...)
// KJ_ASSERT(code == 0, uv_strerror(uv_last_error(loop)), ##__VA_ARGS__)
// KJ_ASSERT(code == 0, uv_strerror(code), ##__VA_ARGS__)
#define UV_CALL(code, ...) \
  do { \
    int error = code;  \
    if (error<0){printf("Error found. %s\n",uv_strerror(error));}    \
  } while (0)

/*class UvEventPort: public kj::EventPort {
public:
  UvEventPort(reactor& e, uv_loop_t* loop): engine(e), loop(loop), kjLoop(*this) {
  }

  ~UvEventPort() {
    // if (scheduled) {
    //   UV_CALL(uv_timer_stop(&timer), loop);
    // }
  }

  kj::EventLoop& getKjLoop() { return kjLoop; }
  uv_loop_t* getUvLoop() { return loop; }
  reactor& getEngine() { return engine; };

  bool wait() override {
    // TODO(someday): Detect if loop will never have an event.
    // printf("Try to wait here\n");
    // UV_CALL(uv_run(loop, UV_RUN_ONCE), loop);
    // return true;
    return engine.wait_and_process();
    // int errno = uv_run(loop, UV_RUN_ONCE);
    // if (errno<0){
    //   printf("Error found. %s\n",uv_strerror(errno));
    // }
  }

  bool poll() override {
    // UV_CALL(uv_run(loop, UV_RUN_NOWAIT), loop);
    // return true;
    return engine.poll_once();
  }

  void setRunnable(bool runnable) override {
    if (runnable != this->runnable) {
      this->runnable = runnable;
      if (runnable) {
        // schedule();
        engine.run();
      } else engine.stop();
    }
  }

private:
  reactor& engine;
  uv_loop_t* loop;
  uv_timer_t timer;
  kj::EventLoop kjLoop;
  friend class kj::EventLoop;
  bool runnable = false;
  bool scheduled = false;

  void schedule() {
    UV_CALL(uv_timer_init(loop, &timer), loop);
    timer.data = this;
    UV_CALL(uv_timer_start(&timer, &doRun, 0, 0), loop);
    scheduled = true;
  }

  void run() {
    KJ_ASSERT(scheduled);

    UV_CALL(uv_timer_stop(&timer), loop);

    if (runnable) {
      kjLoop.run();
    }

    scheduled = false;

    if (runnable) {
      // Apparently either we never became non-runnable, or we did but then became runnable again.
      // Since `scheduled` has been true the whole time, we won't have been rescheduled, so do that
      // now.
      schedule();
    } else {
      scheduled = false;
    }
  }

  static void doRun(uv_timer_t* handle) {
    // if (status == 0) {
    reinterpret_cast<UvEventPort*>(handle->data)->run();
    // }
  }
};//*/

// inline void setNonblocking(int fd) {
//   int flags;
//   KJ_SYSCALL(flags = fcntl(fd, F_GETFL));
//   if ((flags & O_NONBLOCK) == 0) {
//     KJ_SYSCALL(fcntl(fd, F_SETFL, flags | O_NONBLOCK));
//   }
// }

// inline void setCloseOnExec(int fd) {
//   int flags;
//   KJ_SYSCALL(flags = fcntl(fd, F_GETFD));
//   if ((flags & FD_CLOEXEC) == 0) {
//     KJ_SYSCALL(fcntl(fd, F_SETFD, flags | FD_CLOEXEC));
//   }
// }

// static constexpr uint NEW_FD_FLAGS =
// #if __linux__
//   kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC || kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK ||
// #endif
//   kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP;
// // We always try to open FDs with CLOEXEC and NONBLOCK already set on Linux, but on other platforms
// // this is not possible.

// class OwnedFileDescriptor {
// public:
//   OwnedFileDescriptor(uv_loop_t* loop, int fd, uint flags): uvLoop(loop), fd(fd), flags(flags) {
//     if (flags & kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK) {
//       KJ_DREQUIRE(fcntl(fd, F_GETFL) & O_NONBLOCK, "You claimed you set NONBLOCK, but you didn't.");
//     } else {
//       setNonblocking(fd);
//     }

//     if (flags & kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP) {
//       if (flags & kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC) {
//         KJ_DREQUIRE(fcntl(fd, F_GETFD) & FD_CLOEXEC,
//                     "You claimed you set CLOEXEC, but you didn't.");
//       } else {
//         setCloseOnExec(fd);
//       }
//     }

//     UV_CALL(uv_poll_init(uvLoop, &uvPoller, fd), uvLoop);
//     UV_CALL(uv_poll_start(&uvPoller, 0, &pollCallback), uvLoop);
//     uvPoller.data = this;
//   }

//   ~OwnedFileDescriptor() noexcept(false) {
//     if (!stopped) {
//       UV_CALL(uv_poll_stop(&uvPoller), uvLoop);
//     }

//     // Don't use KJ_SYSCALL() here because close() should not be repeated on EINTR.
//     if ((flags & kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP) && close(fd) < 0) {
//       KJ_FAIL_SYSCALL("close", errno, fd) {
//         // Recoverable exceptions are safe in destructors.
//         break;
//       }
//     }
//   }

//   kj::Promise<void> onReadable() {
//     if (stopped) return kj::READY_NOW;

//     KJ_REQUIRE(readable == nullptr, "Must wait for previous event to complete.");

//     auto paf = kj::newPromiseAndFulfiller<void>();
//     readable = kj::mv(paf.fulfiller);

//     int flags = UV_READABLE | (writable == nullptr ? 0 : UV_WRITABLE);
//     UV_CALL(uv_poll_start(&uvPoller, flags, &pollCallback), uvLoop);

//     return kj::mv(paf.promise);
//   }

//   kj::Promise<void> onWritable() {
//     if (stopped) return kj::READY_NOW;

//     KJ_REQUIRE(writable == nullptr, "Must wait for previous event to complete.");

//     auto paf = kj::newPromiseAndFulfiller<void>();
//     writable = kj::mv(paf.fulfiller);

//     int flags = UV_WRITABLE | (readable == nullptr ? 0 : UV_READABLE);
//     UV_CALL(uv_poll_start(&uvPoller, flags, &pollCallback), uvLoop);

//     return kj::mv(paf.promise);
//   }

// protected:
//   uv_loop_t* const uvLoop;
//   const int fd;

// private:
//   uint flags;
//   kj::Maybe<kj::Own<kj::PromiseFulfiller<void>>> readable;
//   kj::Maybe<kj::Own<kj::PromiseFulfiller<void>>> writable;
//   bool stopped = false;
//   uv_poll_t uvPoller;

//   static void pollCallback(uv_poll_t* handle, int status, int events) {
//     reinterpret_cast<OwnedFileDescriptor*>(handle->data)->pollDone(status, events);
//   }

//   void pollDone(int status, int events) {
//     if (status != 0) {
//       // Error. libuv produces a non-zero status if polling produced POLLERR. The error code
//       // reported by libuv is always EBADF, even if the file descriptor is perfectly legitimate but
//       // has simply become disconnected. Instead of throwing an exception, we'd rather report
//       // that the fd is now readable/writable and let the caller discover the error when they
//       // actually attempt to read/write.
//       KJ_IF_MAYBE(r, readable) {
//         r->get()->fulfill();
//         readable = nullptr;
//       }
//       KJ_IF_MAYBE(w, writable) {
//         w->get()->fulfill();
//         writable = nullptr;
//       }

//       // libuv automatically performs uv_poll_stop() before calling poll_cb with an error status.
//       stopped = true;

//     } else {
//       // Fire the events.
//       if (events & UV_READABLE) {
//         KJ_ASSERT_NONNULL(readable)->fulfill();
//         readable = nullptr;
//       }
//       if (events & UV_WRITABLE) {
//         KJ_ASSERT_NONNULL(writable)->fulfill();
//         writable = nullptr;
//       }

//       // Update the poll flags.
//       int flags = (readable == nullptr ? 0 : UV_READABLE) |
//                   (writable == nullptr ? 0 : UV_WRITABLE);
//       UV_CALL(uv_poll_start(&uvPoller, flags, &pollCallback), uvLoop);
//     }
//   }
// };


struct connection {
  connected_socket _fd;
  input_stream<char> _read_buf;
  output_stream<char> _write_buf;


  connection(connected_socket&& fd, socket_address addr)
    : _fd(std::move(fd))
    , _read_buf(_fd.input())
    , _write_buf(_fd.output())
  {}
};


class UvIoStream: public kj::AsyncIoStream {
  // IoStream implementation on top of libuv. This is mostly a copy of the UnixEventPort-based
  // implementation in kj/async-io.c++. We use uv_poll, which the libuv docs say is slow
  // "especially on Windows". I'm guessing it's not so slow on Unix, since it matches the
  // underlying APIs.
  //
  // TODO(cleanup): Allow better code sharing between the two.

public:
  UvIoStream(lw_shared_ptr<kj::connection>& _conn ): conn(_conn) {    
  };
  virtual ~UvIoStream() noexcept(false) {}

  kj::Promise<size_t> read(void* buffer, size_t minBytes, size_t maxBytes) override {
    return conn->_read_buf.read(buffer, minBytes, maxBytes);
  }

  kj::Promise<size_t> tryRead(void* buffer, size_t minBytes, size_t maxBytes) override {    
    return conn->_read_buf.tryRead(buffer, minBytes, maxBytes);
  }

  kj::Promise<void> write(const void* buffer, size_t size) override {
    return conn->_write_buf.write(buffer, size);
  }

  kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const byte>> pieces) override {
    return conn->_write_buf.write(pieces);
  }

  void shutdownWrite() override {
    // There's no legitimate way to get an AsyncStreamFd that isn't a socket through the
    // UnixAsyncIoProvider interface.
    conn->_write_buf.close();
  }

  lw_shared_ptr<connection> conn;
private:
  
};

class UvConnectionReceiver final: public kj::ConnectionReceiver {
  // Like UvIoStream but for ConnectionReceiver. This is also largely copied from kj/async-io.c++.

public:
  UvConnectionReceiver(lw_shared_ptr<connection>& _conn): conn(_conn), port(0) {};

  kj::Promise<kj::Own<kj::AsyncIoStream>> accept() override {
    // return listener->kj_accept().then([this](auto val) {
    //   connected_socket fd = std::move(val.first);
    //   socket_address addr = std::move(val.second);
    //   auto conn = make_lw_shared<connection>(std::move(fd), addr);      
    //   port = ntohs(addr.u.in.sin_port);        
    //   return kj::Promise< kj::Own<kj::AsyncIoStream> > (kj::heap<kj::UvIoStream>( conn ));
    // });
    return kj::Promise<kj::Own<kj::AsyncIoStream>>(kj::heap<kj::UvIoStream>( conn ));
  }

  uint getPort() override {
    // socklen_t addrlen;
    // union {
    //   struct sockaddr generic;
    //   struct sockaddr_in inet4;
    //   struct sockaddr_in6 inet6;
    // } addr;
    // addrlen = sizeof(addr);
    // KJ_SYSCALL(getsockname(fd, &addr.generic, &addrlen));
    // switch (addr.generic.sa_family) {
    // case AF_INET: return ntohs(addr.inet4.sin_port);
    // case AF_INET6: return ntohs(addr.inet6.sin6_port);
    // default: return 0;
    // }
    return port;
  }
  
  
  lw_shared_ptr<connection> conn;
  uint port;
};

class UvLowLevelAsyncIoProvider final: public kj::LowLevelAsyncIoProvider {
public:
  UvLowLevelAsyncIoProvider(){    
  };
  UvLowLevelAsyncIoProvider(lw_shared_ptr<connection>& _conn): conn(_conn) {    
  }
  
  kj::Own<kj::AsyncInputStream> wrapInputFd(int fd, uint flags = 0) override {
    return kj::heap<UvIoStream>(conn);
  }
  kj::Own<kj::AsyncOutputStream> wrapOutputFd(int fd, uint flags = 0) override {
    return kj::heap<UvIoStream>(conn);
  }
  kj::Own<kj::AsyncIoStream> wrapSocketFd(int fd, uint flags = 0) override {
    return kj::heap<UvIoStream>(conn);
  }
  kj::Promise<kj::Own<kj::AsyncIoStream>> wrapConnectingSocketFd(int fd, uint flags = 0) override {
    return kj::Promise<kj::Own<kj::AsyncIoStream>>(kj::heap<kj::UvIoStream>( conn ));
  }
  kj::Own<kj::ConnectionReceiver> wrapListenSocketFd(int fd, uint flags = 0) override {
    return kj::heap<UvConnectionReceiver>(conn);
  }

  // kj::Own<kj::DatagramPort> wrapDatagramSocketFd(int fd, uint flags = 0) override{

  // }

  kj::Timer& getTimer() override {
    // TODO(soon): Implement this.
    KJ_FAIL_ASSERT("Timers not implemented.");
  }

private:    
  lw_shared_ptr<connection> conn;    
};

}

namespace capnp{

// class SeastarNetworkAddressImpl final: public kj::NetworkAddress{
// public:
//   SeastarNetworkAddressImpl(UvLowLevelAsyncIoProvider& _lowLevel): lowLevel(_lowLevel) {};

//   kj::Promise<kj::Own<kj::AsyncIoStream>> connect() override {
//     return lowLevel.wrapConnectingSocketFd(0,0);
//   }

//   kj::Own<kj::ConnectionReceiver> listen() override {    
//     return lowLevel.wrapListenSocketFd(0, 0);
//   }

//   kj::Own<kj::NetworkAddress> clone(){
//     return kj::heap<SeastarNetworkAddressImpl>(lowLevel);  
//   }

//   kj::String toString() override {
//     return kj::String();
//   }


// private:
//   UvLowLevelAsyncIoProvider& lowLevel;  
// };


class SeastarNetwork final : public TwoPartyVatNetworkBase,
                          private TwoPartyVatNetworkBase::Connection{

public:
  SeastarNetwork(kj::AsyncIoStream& stream, rpc::twoparty::Side side,
                     ReaderOptions receiveOptions = ReaderOptions());

kj::Promise<void> onDisconnect() { return disconnectPromise.addBranch(); }
  // Returns a promise that resolves when the peer disconnects.

  // implements VatNetwork -----------------------------------------------------

  kj::Maybe<kj::Own<TwoPartyVatNetworkBase::Connection>> connect(
      rpc::twoparty::VatId::Reader ref) override;
  kj::Promise<kj::Own<TwoPartyVatNetworkBase::Connection>> accept() override;

private:
  class OutgoingMessageImpl;
  class IncomingMessageImpl;

  kj::AsyncIoStream& stream;
  rpc::twoparty::Side side;
  ReaderOptions receiveOptions;
  bool accepted = false;

  kj::Maybe<kj::Promise<void>> previousWrite;
  // Resolves when the previous write completes.  This effectively serves as the write queue.
  // Becomes null when shutdown() is called.

  kj::Own<kj::PromiseFulfiller<kj::Own<TwoPartyVatNetworkBase::Connection>>> acceptFulfiller;
  // Fulfiller for the promise returned by acceptConnectionAsRefHost() on the client side, or the
  // second call on the server side.  Never fulfilled, because there is only one connection.

  kj::ForkedPromise<void> disconnectPromise = nullptr;

  class FulfillerDisposer: public kj::Disposer {
    // Hack:  TwoPartyVatNetwork is both a VatNetwork and a VatNetwork::Connection.  Whet the RPC
    //   system detects (or initiates) a disconnection, it drops its reference to the Connection.
    //   When all references have been dropped, then we want onDrained() to fire.  So we hand out
    //   Own<Connection>s with this disposer attached, so that we can detect when they are dropped.

  public:
    mutable kj::Own<kj::PromiseFulfiller<void>> fulfiller;
    mutable uint refcount = 0;

    void disposeImpl(void* pointer) const override;
  };
  FulfillerDisposer disconnectFulfiller;

  kj::Own<TwoPartyVatNetworkBase::Connection> asConnection();
  // Returns a pointer to this with the disposer set to drainedFulfiller.

  // implements Connection -----------------------------------------------------

  kj::Own<OutgoingRpcMessage> newOutgoingMessage(uint firstSegmentWordSize) override;
  kj::Promise<kj::Maybe<kj::Own<IncomingRpcMessage>>> receiveIncomingMessage() override;
  kj::Promise<void> shutdown() override;

};//*/

}
