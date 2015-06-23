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

#define DEFAUlT_SCRATCH_SPACE_SIZE 8192
#define DEFAUlT_RELOCATE_THREADSHOLD 512

struct ZeroCopyScratchspace {
  class ArrDisposer : public kj::ArrayDisposer {
  public:
    ArrDisposer() : parent(nullptr) {};
    ArrDisposer(ZeroCopyScratchspace* parent) : parent(parent) {};

    virtual void disposeImpl(void* firstElement, size_t elementSize, size_t elementCount,
                             size_t capacity, void (*destroyElement)(void*)) const {
      KJ_IF_MAYBE(v, parent) {
        // KJ_DBG("Free ",elementCount,v);
        v->release(elementCount);
        // KJ_DBG("Freed  ",v->freed,v->readCounter,v->writerCounter);
      }
    };

    kj::Maybe<ZeroCopyScratchspace&> parent;
  };
  kj::ArrayPtr<char> scratchSpace;
  size_t readCounter;
  size_t writerCounter;
  size_t freed;
  kj::Maybe<ArrDisposer> disposer;

  ZeroCopyScratchspace(size_t sz = DEFAUlT_SCRATCH_SPACE_SIZE) : scratchSpace(kj::heapArray<char>(sz)), readCounter(0), writerCounter(0), freed(0), disposer(this) { };

  ZeroCopyScratchspace(ZeroCopyScratchspace&&) = default;
  ZeroCopyScratchspace& operator=(ZeroCopyScratchspace&&) = default;

  inline void read(size_t cnt) {
    readCounter += cnt;
    // freeMem -= cnt;
  }

  inline char operator[](size_t i){
    return scratchSpace[writerCounter+i];
  }

  inline char* startRead() {
    char* start = scratchSpace.begin();
    return start + readCounter;
  }

  inline size_t maxReadable() {
    return scratchSpace.size() - readCounter;
  }

  inline size_t size() {
    return scratchSpace.size();
  }

  inline size_t available() {
    return scratchSpace.size() - writerCounter;
  }

  inline size_t consumable() {
    return readCounter - writerCounter;
  }

  inline void release(size_t amount) {
    freed += amount;    
    if (freed > writerCounter) freed = writerCounter;
        
    // if (freed == writerCounter && writerCounter == readCounter ) {
    //   printf("reset consumed\n");
    //   readCounter = 0;
    //   writerCounter = 0;
    //   freed = 0;
    // }
  }

  inline kj::Array<char> consuming(size_t cnt) {
    char* start = scratchSpace.begin() + writerCounter;
    KJ_IF_MAYBE(d, disposer) {
      return kj::Array<char>(start, cnt, *d );
    }
    return kj::Array<char>();
  }

  inline void copy(void* buffer, size_t len) {
    char* start = scratchSpace.begin() + writerCounter;
    std::copy(start, start + len, reinterpret_cast<char*>(buffer)  );
  }

  inline void consumed(size_t cnt) {
    writerCounter += cnt;
    // KJ_DBG(readCounter,writerCounter);
  }


  inline void reserve(size_t sz) {
    printf("reserve\n");
    if (scratchSpace.size() < sz) {
      auto ownedSpace =  kj::heapArray<char>(sz);
      if (readCounter > writerCounter) {
        char* start = scratchSpace.begin();
        std::copy(start + writerCounter, start + readCounter, ownedSpace.begin()  );
      }
      scratchSpace = ownedSpace;

    }
  }

  inline shrink_to_fit(size_t sz = DEFAUlT_SCRATCH_SPACE_SIZE) {
    if (sz < scratchSpace.size()) {
      sz = std::max(sz, readCounter - writerCounter);
      auto ownedSpace =  kj::heapArray<char>(sz);
      if (readCounter > writerCounter) {
        char* start = scratchSpace.begin();
        std::copy(start + writerCounter, start + readCounter, ownedSpace.begin()  );
      }
      scratchSpace = ownedSpace;
    }
  }

  inline void reset() {
    if (freed == writerCounter) {
      printf("reset\n");
      if (readCounter > writerCounter ) {
        char* start = scratchSpace.begin();
        std::copy(start + writerCounter, start + readCounter, start  );
      }

      readCounter = readCounter - writerCounter;
      writerCounter = 0;
      freed = 0;
    }
  }

};

struct UvIoStream: public kj::AsyncIoStream {
  // IoStream implementation on top of libuv. This is mostly a copy of the UnixEventPort-based
  // implementation in kj/async-io.c++. We use uv_poll, which the libuv docs say is slow
  // "especially on Windows". I'm guessing it's not so slow on Unix, since it matches the
  // underlying APIs.
  //
  // TODO(cleanup): Allow better code sharing between the two.


  UvIoStream(kj::connection&& _conn, size_t buffer_size = DEFAUlT_SCRATCH_SPACE_SIZE ): conn(kj::mv(_conn) ),buffer( kj::heap<ZeroCopyScratchspace>(buffer_size) ) {    
  };

  ~UvIoStream() noexcept(false) {
  
    // if (buffer) delete buffer;
    // for (auto pool : pools){
    //   if (pool) delete pool;
    // }
  }

  UvIoStream(UvIoStream&&) = default;
  UvIoStream& operator=(UvIoStream&&) = default;


  kj::Promise<size_t> read(size_t minBytes) {
    // KJ_DBG(freed,readCounter,writerCounter,amount);    
    
    size_t consumable = buffer->consumable();    
    
    // if (buffer->freed == buffer->writerCounter && consumable==0 ) {
    //   printf("reset consumed\n");
    //   buffer->readCounter = 0;
    //   buffer->writerCounter = 0;
    //   buffer->freed = 0;
    // }

    minBytes -= consumable;
    size_t maxReadable = buffer->maxReadable();

    if (minBytes<=0) return kj::Promise<size_t>(consumable);
    return read(reinterpret_cast<void*>(buffer->startRead()), minBytes, maxReadable).then([consumable, this](auto n) {
      buffer->read(n);
      return n+consumable;
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

  kj::String toString() {
    return kj::str("ReadCounter ", buffer->readCounter,"   WriteCounter ", buffer->writerCounter, "    Freed memory = ", buffer->freed);
  }

  void createNewSegment(size_t size) {

    // int currentIdx = 0;
    // for (currentIdx = 0; currentIdx < pools.size(); ++currentIdx) {
    //   auto& pool = pools[currentIdx];
    //   if (&pool == &buffer) break;//Ignore current buffer
    // }

    // if (currentIdx == pools.size() )//Not found
    //   currentIdx = -1;

    if (size < DEFAUlT_SCRATCH_SPACE_SIZE) {
      for (int i = 0; i < pools.size(); ++i) {
        auto& pool = pools[i];

        // if (i == currentIdx) continue;//Ignore current buffer
        if (pool->readCounter == 0 && pool->writerCounter == 0) {
          //Found an empty pool
          auto sz = buffer->consumable();
          if (sz > 0) {
            buffer->copy(pool->scratchSpace.begin(), sz );
            pool->read(sz);
          }

          //Swap place
          buffer->readCounter = buffer->writerCounter;
          auto tmp = kj::mv(buffer);
          buffer = kj::mv(pool);
          pool = kj::mv(tmp);
          return;
        }
      }
    }

    //Need to create a new segment.
    size_t sz = kj::max(DEFAUlT_SCRATCH_SPACE_SIZE, size);
    pools.emplace_back(  kj::heap<ZeroCopyScratchspace>(sz)  );
    auto& pool = pools.back();
    sz = buffer->consumable();
    if (sz > 0) {
      buffer->copy(pool->scratchSpace.begin(), sz );
      pool->read(sz);
    }

    buffer->readCounter = buffer->writerCounter;
    auto tmp = kj::mv(buffer);
    buffer = kj::mv(pool);
    pool = kj::mv(tmp);

  }

  kj::connection conn;

  std::vector< kj::Own<ZeroCopyScratchspace> > pools;
  kj::Own<ZeroCopyScratchspace> buffer;

};



}//end of namespace kj

namespace capnp {

/*namespace rpc {
inline kj::String KJ_STRINGIFY(Message::Which which) {
  return kj::str(static_cast<uint16_t>(which));
}
}  // namespace rpc


class RpcDumper {
  // Class which stringifies RPC messages for debugging purposes, including decoding params and
  // results based on the call's interface and method IDs and extracting cap descriptors.
  //
  // TODO(cleanup):  Clean this code up and move it to someplace reusable, so it can be used as
  //   a packet inspector / debugging tool for Cap'n Proto network traffic.

public:
  void addSchema(InterfaceSchema schema) {
    schemas[schema.getProto().getId()] = schema;
  }

  enum Sender {
    CLIENT,
    SERVER
  };

  kj::String dump(rpc::Message::Reader message, Sender sender) {
    const char* senderName = sender == CLIENT ? "client" : "server";

    switch (message.which()) {
    case rpc::Message::CALL: {
      auto call = message.getCall();
      auto iter = schemas.find(call.getInterfaceId());
      if (iter == schemas.end()) {
        break;
      }
      InterfaceSchema schema = iter->second;
      auto methods = schema.getMethods();
      if (call.getMethodId() >= methods.size()) {
        break;
      }
      InterfaceSchema::Method method = methods[call.getMethodId()];

      auto schemaProto = schema.getProto();
      auto interfaceName =
        schemaProto.getDisplayName().slice(schemaProto.getDisplayNamePrefixLength());

      auto methodProto = method.getProto();
      auto paramType = method.getParamType();
      auto resultType = method.getResultType();

      if (call.getSendResultsTo().isCaller()) {
        returnTypes[std::make_pair(sender, call.getQuestionId())] = resultType;
      }

      auto payload = call.getParams();
      auto params = kj::str(payload.getContent().getAs<DynamicStruct>(paramType));

      auto sendResultsTo = call.getSendResultsTo();

      return kj::str(senderName, "(", call.getQuestionId(), "): call ",
                     call.getTarget(), " <- ", interfaceName, ".",
                     methodProto.getName(), params,
                     " caps:[", kj::strArray(payload.getCapTable(), ", "), "]",
                     sendResultsTo.isCaller() ? kj::str()
                     : kj::str(" sendResultsTo:", sendResultsTo));
    }

    case rpc::Message::RETURN: {
      auto ret = message.getReturn();

      auto iter = returnTypes.find(
                    std::make_pair(sender == CLIENT ? SERVER : CLIENT, ret.getAnswerId()));
      if (iter == returnTypes.end()) {
        break;
      }

      auto schema = iter->second;
      returnTypes.erase(iter);
      if (ret.which() != rpc::Return::RESULTS) {
        // Oops, no results returned.  We don't check this earlier because we want to make sure
        // returnTypes.erase() gets a chance to happen.
        break;
      }

      auto payload = ret.getResults();

      if (schema.getProto().isStruct()) {
        auto results = kj::str(payload.getContent().getAs<DynamicStruct>(schema.asStruct()));

        return kj::str(senderName, "(", ret.getAnswerId(), "): return ", results,
                       " caps:[", kj::strArray(payload.getCapTable(), ", "), "]");
      } else if (schema.getProto().isInterface()) {
        payload.getContent().getAs<DynamicCapability>(schema.asInterface());
        return kj::str(senderName, "(", ret.getAnswerId(), "): return cap ",
                       kj::strArray(payload.getCapTable(), ", "));
      } else {
        break;
      }
    }

    case rpc::Message::BOOTSTRAP: {
      auto restore = message.getBootstrap();

      returnTypes[std::make_pair(sender, restore.getQuestionId())] = InterfaceSchema();

      return kj::str(senderName, "(", restore.getQuestionId(), "): bootstrap ");
    }

    default:
      break;
    }

    return kj::str(senderName, ": ", message);
  }

private:
  std::map<uint64_t, InterfaceSchema> schemas;
  std::map<std::pair<Sender, uint32_t>, Schema> returnTypes;
};

//*/

class SeastarNetwork final : public TwoPartyVatNetworkBase,
  private TwoPartyVatNetworkBase::Connection {

public:
  SeastarNetwork(kj::UvIoStream& stream, rpc::twoparty::Side side,
                 ReaderOptions receiveOptions = ReaderOptions());

  kj::Promise<void> onDisconnect() { return disconnectPromise.addBranch(); }
  // Returns a promise that resolves when the peer disconnects.

  // implements VatNetwork -----------------------------------------------------

  kj::Maybe<kj::Own<TwoPartyVatNetworkBase::Connection>> connect(
        rpc::twoparty::VatId::Reader ref) override;
  kj::Promise<kj::Own<TwoPartyVatNetworkBase::Connection>> accept() override;

  // RpcDumper dumper;

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
