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


namespace kj { 
typedef unsigned char byte;
typedef unsigned int uint;

// =======================================================================================

struct connection {
  connected_socket _fd;
  input_stream<char> _read_buf;
  output_stream<char> _write_buf;


  connection(connected_socket&& fd, socket_address addr)
    : _fd(std::move(fd))
    , _read_buf(_fd.input())
    , _write_buf(_fd.output())
  {}

  connection(connection&&) = default;
  connection& operator=(connection&&) = default;
};

#define DEFAUlT_SCRACH_SPACE_SIZE 8192
#define DEFAUlT_RELOCATE_THREADSHOLD 512

struct ZeroCopyScratchspace {
  kj::ArrayPtr<char> scratchSpace;
  size_t readCouter = 0;
  size_t writerCounter = 0;

  ZeroCopyScratchspace(size_t sz = DEFAUlT_SCRACH_SPACE_SIZE) : scratchSpace(kj::heapArray<char>(sz)) {};

  ZeroCopyScratchspace(ZeroCopyScratchspace&&) = default;
  ZeroCopyScratchspace& operator=(ZeroCopyScratchspace&&) = default;

  inline void read(size_t cnt) {
    readCouter += cnt;
  }

  inline char* startRead() {
    char* start = scratchSpace.begin();
    return start + readCouter;
  }

  inline size_t maxReadable() {
    return scratchSpace.size() - readCouter;
  }

  inline size_t size() {
    return scratchSpace.size();
  }

  inline size_t available() {
    return scratchSpace.size() - writerCounter;
  }

  inline size_t consumable() {
    return readCouter - writerCounter;
  }

  inline kj::ArrayPtr<capnp::word> consuming(size_t cnt) {
    char* start = scratchSpace.begin() + writerCounter;
    return kj::ArrayPtr<capnp::word>(reinterpret_cast<capnp::word*>(start), reinterpret_cast<capnp::word*>(start + cnt));
  }

  inline void copy(void* buffer, size_t len) {
    char* start = scratchSpace.begin() + writerCounter;
    std::copy(start, start + len, reinterpret_cast<char*>(buffer)  );
  }

  inline void consumed(size_t cnt) {
    writerCounter += cnt;
    if (writerCounter == readCouter) {
      // printf("reset consumed\n");
      readCouter = 0;
      writerCounter = 0;
    }
    // KJ_DBG(readCouter,writerCounter);
  }


  inline void reserve(size_t sz) {
    printf("reserve\n");
    if (scratchSpace.size() < sz) {
      auto ownedSpace =  kj::heapArray<char>(sz);
      if (readCouter > writerCounter) {
        char* start = scratchSpace.begin();
        std::copy(start + writerCounter, start + readCouter, ownedSpace.begin()  );
      }
      scratchSpace = ownedSpace;

    }
  }

  inline shrink_to_fit(size_t sz = DEFAUlT_SCRACH_SPACE_SIZE) {
    if (sz < scratchSpace.size()) {
      sz = std::max(sz, readCouter - writerCounter);
      auto ownedSpace =  kj::heapArray<char>(sz);
      if (readCouter > writerCounter) {
        char* start = scratchSpace.begin();
        std::copy(start + writerCounter, start + readCouter, ownedSpace.begin()  );
      }
      scratchSpace = ownedSpace;
    }
  }

  inline void reset() {
    printf("reset\n");
    if (readCouter > writerCounter) {
      char* start = scratchSpace.begin();
      std::copy(start + writerCounter, start + readCouter, start  );
    }

    readCouter = readCouter - writerCounter;
    writerCounter = 0;
  }

};

struct UvIoStream: public kj::AsyncIoStream {
  // IoStream implementation on top of libuv. This is mostly a copy of the UnixEventPort-based
  // implementation in kj/async-io.c++. We use uv_poll, which the libuv docs say is slow
  // "especially on Windows". I'm guessing it's not so slow on Unix, since it matches the
  // underlying APIs.
  //
  // TODO(cleanup): Allow better code sharing between the two.


  UvIoStream(kj::connection&& _conn, size_t buffer_size = DEFAUlT_SCRACH_SPACE_SIZE ): conn(kj::mv(_conn) ), buffer(buffer_size) {};

  ~UvIoStream() noexcept(false) {}

  UvIoStream(UvIoStream&&) = default;
  UvIoStream& operator=(UvIoStream&&) = default;


  kj::Promise<size_t> read(size_t minBytes) {
    size_t maxReadable = buffer.maxReadable();
    size_t consumable = buffer.consumable();
    return read(reinterpret_cast<void*>(buffer.startRead()), minBytes, maxReadable).then([consumable, this](auto n) {
      buffer.read(n);
      return n + consumable;
    });
  }

  kj::Promise<size_t> read(void* buffer, size_t minBytes, size_t maxBytes) override {
    return conn._read_buf.read(buffer, minBytes, maxBytes);
  }
  Promise<void> read(void* buffer, size_t bytes) {
    return read(buffer, bytes, bytes).then([](size_t) {});
  }

  kj::Promise<size_t> tryRead(void* buffer, size_t minBytes, size_t maxBytes) override {    
    return conn._read_buf.tryRead(buffer, minBytes, maxBytes);
  }

  kj::Promise<void> write(const void* buffer, size_t size) override {
    return conn._write_buf.write(buffer, size);
  }

  kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const byte>> pieces) override {
    return conn._write_buf.write(pieces);
  }

  void shutdownWrite() override {
    // There's no legitimate way to get an AsyncStreamFd that isn't a socket through the
    // UnixAsyncIoProvider interface.
    conn._write_buf.close();
  }

  kj::connection conn;
  ZeroCopyScratchspace buffer;
  
};


}//end of namespace kj

namespace capnp {
class SeastarNetwork final : public TwoPartyVatNetworkBase,
                          private TwoPartyVatNetworkBase::Connection{

public:
  SeastarNetwork(kj::UvIoStream& stream, rpc::twoparty::Side side,
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

  kj::UvIoStream& stream;
  rpc::twoparty::Side side;
  MallocMessageBuilder peerVatId;
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

  rpc::twoparty::VatId::Reader getPeerVatId() override;
  kj::Own<OutgoingRpcMessage> newOutgoingMessage(uint firstSegmentWordSize) override;
  kj::Promise<kj::Maybe<kj::Own<IncomingRpcMessage>>> receiveIncomingMessage() override;
  kj::Promise<void> shutdown() override;

};//*/
class SeastarServer: private kj::TaskSet::ErrorHandler {
  // Convenience class which implements a simple server which accepts connections on a listener
  // socket and serices them as two-party connections.

public:
  explicit SeastarServer(kj::Maybe<Capability::Client> bootstrapInterface);

  kj::Promise<void> listen(ipv4_addr addr);
  // Listens for connections on the given listener. The returned promise never resolves unless an
  // exception is thrown while trying to accept. You may discard the returned promise to cancel
  // listening.

private:
  kj::Own<server_socket> server;
  kj::Maybe<Capability::Client> bootstrapInterface;
  kj::TaskSet tasks;

  struct AcceptedConnection;

  void taskFailed(kj::Exception&& exception) override;
};


}  // namespace capnp

#endif  // CAPNP_Vmts_RPC_H_
