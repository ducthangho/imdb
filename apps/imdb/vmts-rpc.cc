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

#include "vmts-rpc.hh"
#include "capnp/rpc-twoparty.h"
#include <capnp/rpc.capnp.h>
#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/debug.h>
#include <kj/threadlocal.h>
#include "capnp/rpc.h"
#include "capnp/message.h"
#include <kj/vector.h>
#include <kj/async.h>
#include <kj/one-of.h>
#include <kj/function.h>
#include <kj/common.h>
#include <unordered_map>
#include <map>
#include <queue>
#include <capnp/rpc.capnp.h>
#include <kj/common.h>
#include "capnp/serialize-async.h"
#include "core/future.hh"
#include "core/reactor.hh"
// #include "event_port.hh"

using namespace kj;

namespace capnp {


/****************************************************************************************/
SeastarNetwork::SeastarNetwork(kj::UvIoStream& stream, rpc::twoparty::Side side,
                                       ReaderOptions receiveOptions)
  : stream(stream), side(side), peerVatId(4), receiveOptions(receiveOptions), previousWrite(kj::READY_NOW) {

  peerVatId.initRoot<rpc::twoparty::VatId>().setSide(
    side == rpc::twoparty::Side::CLIENT ? rpc::twoparty::Side::SERVER
    : rpc::twoparty::Side::CLIENT);
  auto paf = kj::newPromiseAndFulfiller<void>();
  disconnectPromise = paf.promise.fork();
  disconnectFulfiller.fulfiller = kj::mv(paf.fulfiller);
}

rpc::twoparty::VatId::Reader SeastarNetwork::getPeerVatId() {
  return peerVatId.getRoot<rpc::twoparty::VatId>();
}

void SeastarNetwork::FulfillerDisposer::disposeImpl(void* pointer) const {
  if (--refcount == 0) {
    fulfiller->fulfill();
  }
}

kj::Own<TwoPartyVatNetworkBase::Connection> SeastarNetwork::asConnection() {
  ++disconnectFulfiller.refcount;
  return kj::Own<TwoPartyVatNetworkBase::Connection>(this, disconnectFulfiller);
}

kj::Maybe<kj::Own<TwoPartyVatNetworkBase::Connection>> SeastarNetwork::connect(
    rpc::twoparty::VatId::Reader ref) {
  if (ref.getSide() == side) {
    return nullptr;
  } else {
    return asConnection();
  }
}

kj::Promise<kj::Own<TwoPartyVatNetworkBase::Connection>> SeastarNetwork::accept() {
  if (side == rpc::twoparty::Side::SERVER && !accepted) {
    accepted = true;
    return asConnection();
  } else {
    // Create a promise that will never be fulfilled.
    auto paf = kj::newPromiseAndFulfiller<kj::Own<TwoPartyVatNetworkBase::Connection>>();
    acceptFulfiller = kj::mv(paf.fulfiller);
    return kj::mv(paf.promise);
  }
}


class SeastarAsyncMessageReader: public MessageReader {
public:
  inline SeastarAsyncMessageReader(kj::UvIoStream& _inputStream, ReaderOptions options): MessageReader(options), inputStream(_inputStream), totalWords(0) {    
    memset(firstWord, 0, sizeof(firstWord));
  }
  ~SeastarAsyncMessageReader() noexcept(false) {}

  kj::Promise<bool> read();

  // implements MessageReader ----------------------------------------

  kj::ArrayPtr<const word> getSegment(uint id) override {
    if (id >= segmentCount()) {
      return nullptr;
    } else {
      uint32_t size = id == 0 ? segment0Size() : moreSizes[id - 1].get();
      return kj::arrayPtr(segmentStarts[id], size);
    }
  }

private:
  kj::UvIoStream& inputStream;
  size_t totalWords;
  _::WireValue<uint32_t> firstWord[2];
  kj::Array<_::WireValue<uint32_t>> moreSizes;
  kj::Array<const word*> segmentStarts;

  kj::Array<word> ownedSpace;
  // Only if scratchSpace wasn't big enough.

  inline uint segmentCount() { return firstWord[0].get() + 1; }
  inline uint segment0Size() { return firstWord[1].get(); }

  kj::Promise<void> readAfterFirstWord();
  kj::Promise<void> readSegments();
};

kj::Promise<bool> SeastarAsyncMessageReader::read() {
  size_t consumable = inputStream.buffer.consumable();
  
  // KJ_DBG(consumable, sizeof(firstWord) );
  
  if (consumable >= sizeof(firstWord)) { //If already read
    inputStream.buffer.copy(firstWord, sizeof(firstWord) );
    inputStream.buffer.consumed(sizeof(firstWord));
    // KJ_DBG(segmentCount() , segment0Size());
    // KJ_DBG(this);
    return readAfterFirstWord().then([]() { return true; });
  }

  if (inputStream.buffer.available() < sizeof(firstWord) ) {
    KJ_DBG("Reset");
    inputStream.buffer.reset();
  }


  return inputStream.read(sizeof(firstWord)- consumable).then([this](size_t n) mutable -> kj::Promise<bool> {    
    if (n == 0) {
      return false;
    } else if (n < sizeof(firstWord)) {
      // EOF in first word.
      KJ_FAIL_REQUIRE("Premature EOF.") {
        return false;
      }
    }

    inputStream.buffer.copy(firstWord, sizeof(firstWord));
    // KJ_DBG(segmentCount() , segment0Size());
    // KJ_DBG(inputStream.buffer.consumable());
    inputStream.buffer.consumed(sizeof(firstWord));
    // KJ_DBG(inputStream.buffer.consumable());

    // printf("Read : %zu bytes. Segment count =   %d, segment 0 size   %d\n",segmentCount(),segment0Size());

    return readAfterFirstWord().then([]() { return true; });
  });
}

kj::Promise<void> SeastarAsyncMessageReader::readAfterFirstWord() {
  if (segmentCount() == 0) {
    firstWord[1].set(0);
  }

  // Reject messages with too many segments for security reasons.
  KJ_REQUIRE(segmentCount() < 512, "Message has too many segments.") {
    return kj::READY_NOW;  // exception will be propagated
  }

  // printf("scratchSpace size = %zu\n",scratchSpace.size());

  if (segmentCount() > 1) {
    // Read sizes for all segments except the first.  Include padding if necessary.
    moreSizes = kj::heapArray<_::WireValue<uint32_t>>(segmentCount() & ~1);

    size_t size = moreSizes.size() * sizeof(moreSizes[0]);    
    size_t consumable = inputStream.buffer.consumable();
    if (consumable >= size) { //If already read
      inputStream.buffer.copy(moreSizes.begin(), size);
      // KJ_DBG(consumable);
      inputStream.buffer.consumed(size);
      // KJ_DBG(size);
      return readSegments();
    }


    if (inputStream.buffer.available() < size ) {
      KJ_DBG("Reset 2");
      inputStream.buffer.reset();
    }

    // KJ_DBG(size,consumable);
    return inputStream.read(size - consumable).then([this, size](size_t n) mutable -> kj::Promise<void> {
      if (n == 0) {
        return kj::READY_NOW;
      } else if (n < size) {
        // EOF in first word.
        KJ_FAIL_REQUIRE("Premature EOF.") {
          return kj::READY_NOW;
        }
      }

      inputStream.buffer.copy(moreSizes.begin(), size);
      inputStream.buffer.consumed(size);

      return readSegments();
    });
  } else {
    return readSegments();
  }
}

kj::Promise<void> SeastarAsyncMessageReader::readSegments() {  
  // KJ_DBG(segmentCount() , segment0Size());
  totalWords = segment0Size();

  if (segmentCount() > 1) {
    for (uint i = 0; i < segmentCount() - 1; i++) {
      totalWords += moreSizes[i].get();
    }
  }
  

  // Don't accept a message which the receiver couldn't possibly traverse without hitting the
  // traversal limit.  Without this check, a malicious client could transmit a very large segment
  // size to make the receiver allocate excessive space and possibly crash.
  KJ_REQUIRE(totalWords <= getOptions().traversalLimitInWords,
             "Message is too large.  To increase the limit on the receiving end, see "
             "capnp::ReaderOptions.") {
    return kj::READY_NOW;  // exception will be propagated
  }

  totalWords *= sizeof(word);

  if (inputStream.buffer.size() < totalWords) {
    // TODO(perf):  Consider allocating each segment as a separate chunk to reduce memory
    //   fragmentation.
    // printf("Allocate more space\n");
    KJ_DBG("Reset 3");
    inputStream.buffer.reserve(totalWords);
  } else if (inputStream.buffer.available() < totalWords) {
    KJ_DBG("Reset 4");
    KJ_DBG(totalWords,segmentCount(),inputStream.buffer.readCouter,inputStream.buffer.available());
    inputStream.buffer.reset();
    KJ_DBG(totalWords,segmentCount(),inputStream.buffer.readCouter,inputStream.buffer.available());    
  }

  segmentStarts = kj::heapArray<const word*>(segmentCount());
  kj::ArrayPtr<capnp::word> scratchSpace(inputStream.buffer.consuming(totalWords));
  segmentStarts[0] = scratchSpace.begin();

  if (segmentCount() > 1) {
    size_t offset = segment0Size()*sizeof(word);

    for (uint i = 1; i < segmentCount(); i++) {
      segmentStarts[i] = scratchSpace.begin() + offset;
      offset += moreSizes[i - 1].get()*sizeof(word);
    }
  }
  
  size_t consumable = inputStream.buffer.consumable();
  // KJ_DBG(totalWords,consumable);
  if (consumable >= totalWords) { //If already read
    // printf("Now read everything %zu     %zu\n",(size_t)scratchSpace.begin(),totalWords * sizeof(word));
    inputStream.buffer.consumed(totalWords);
    return kj::READY_NOW;
  }


  // printf("Now read everything %zu     %zu\n",(size_t)scratchSpace.begin(),totalWords * sizeof(word));
  return inputStream.read(totalWords - consumable).then([this](auto n) -> kj::Promise<void> {
    inputStream.buffer.consumed(totalWords);
    return kj::READY_NOW;
    // return this->readSegments();

  });
}
class SeastarNetwork::OutgoingMessageImpl final
    : public OutgoingRpcMessage, public kj::Refcounted {
public:
  OutgoingMessageImpl(SeastarNetwork& network, uint firstSegmentWordSize)
      : network(network),
        message(firstSegmentWordSize == 0 ? SUGGESTED_FIRST_SEGMENT_WORDS : firstSegmentWordSize) {}

  AnyPointer::Builder getBody() override {
    return message.getRoot<AnyPointer>();
  }

  kj::ArrayPtr<kj::Maybe<kj::Own<ClientHook>>> getCapTable() override {
    return message.getCapTable();
  }

  void send() override {
    network.previousWrite = KJ_ASSERT_NONNULL(network.previousWrite, "already shut down")
        .then([&]() {
      // Note that if the write fails, all further writes will be skipped due to the exception.
      // We never actually handle this exception because we assume the read end will fail as well
      // and it's cleaner to handle the failure there.
      return writeMessage(network.stream, message);
    }).attach(kj::addRef(*this))
      // Note that it's important that the eagerlyEvaluate() come *after* the attach() because
      // otherwise the message (and any capabilities in it) will not be released until a new
      // message is written! (Kenton once spent all afternoon tracking this down...)
      .eagerlyEvaluate(nullptr);
  }

private:
  SeastarNetwork& network;
  MallocMessageBuilder message;
};

class SeastarNetwork::IncomingMessageImpl final: public IncomingRpcMessage {
public:
  IncomingMessageImpl(kj::Own<MessageReader> message): message(kj::mv(message)) {}

  AnyPointer::Reader getBody() override {
    return message->getRoot<AnyPointer>();
  }

  void initCapTable(kj::Array<kj::Maybe<kj::Own<ClientHook>>>&& capTable) override {
    message->initCapTable(kj::mv(capTable));
  }

private:
  kj::Own<MessageReader> message;
};

kj::Own<OutgoingRpcMessage> SeastarNetwork::newOutgoingMessage(uint firstSegmentWordSize) {
  return kj::refcounted<OutgoingMessageImpl>(*this, firstSegmentWordSize);
}
kj::Promise<kj::Maybe<kj::Own<MessageReader>>> _tryReadMessage(
  kj::UvIoStream& input, ReaderOptions options) {
  auto reader = kj::heap<SeastarAsyncMessageReader>(input, options);
  auto promise = reader->read();
  return promise.then(kj::mvCapture(reader,
  [](kj::Own<MessageReader> && reader, bool success) -> kj::Maybe<kj::Own<MessageReader>> {
    if (success) {
      return kj::mv(reader);
    } else {
      return nullptr;
    }
  }));
}

kj::Promise<kj::Maybe<kj::Own<IncomingRpcMessage>>> SeastarNetwork::receiveIncomingMessage() {
  return kj::evalLater([this]() {
    return _tryReadMessage(this->stream, receiveOptions)
        .then([&](kj::Maybe<kj::Own<MessageReader>>&& message)
              -> kj::Maybe<kj::Own<IncomingRpcMessage>> {
      KJ_IF_MAYBE(m, message) {
        return kj::Own<IncomingRpcMessage>(kj::heap<IncomingMessageImpl>(kj::mv(*m)));
      } else {
        return nullptr;
      }
    });
  });
}

kj::Promise<void> SeastarNetwork::shutdown() {
  kj::Promise<void> result = KJ_ASSERT_NONNULL(previousWrite, "already shut down").then([this]() {
    stream.shutdownWrite();
  });
  previousWrite = nullptr;
  return kj::mv(result);
}

SeastarServer::SeastarServer(kj::Maybe<Capability::Client> bootstrapInterface)
  : bootstrapInterface(bootstrapInterface), tasks(*this) {}

struct SeastarServer::AcceptedConnection {
  kj::UvIoStream connection;
  SeastarNetwork network;
  RpcSystem<rpc::twoparty::VatId> rpcSystem;


  explicit AcceptedConnection(kj::Maybe<Capability::Client> bootstrapInterface,
                              kj::UvIoStream&& connectionParam)
    : connection(kj::mv(connectionParam)),
      network(connection, rpc::twoparty::Side::SERVER),
      rpcSystem(makeRpcServer(network, *(::kj::_::readMaybe(bootstrapInterface)) )) {}
};

kj::Promise<void> SeastarServer::listen(ipv4_addr addr) {
  // uint32_t ip = addr.ip;
  // uint16_t port = addr.port;
  listen_options lo;
  lo.reuse_address = true;

  
  server = kj::heap<server_socket>(engine().listen(make_ipv4_address(addr), lo));

  return server->kj_accept()
  .then([this](auto val) mutable -> kj::Promise<void> { 
    connected_socket fd = std::move(val.first);
    socket_address addr = std::move(val.second);    
    // std::cout<<"Thread "<<which<<" : "<<std::endl;
    kj::connection conn(std::move(fd), addr);    
    kj::UvIoStream stream(kj::mv(conn));    

    auto connectionState = kj::heap<AcceptedConnection>(this->bootstrapInterface, kj::mv(stream));
    
    // Run the connection until disconnect.
    auto promise = connectionState->network.onDisconnect().then([](){
        printf("Client disconnected!\n");
    });  
    tasks.add(promise.attach(kj::mv(connectionState)));//attach so that connectionstate remains valid until the promise's fulfilled

    return kj::READY_NOW;
  }, [](kj::Exception && e) {
    printf("Exception : %s\n", e.getDescription().cStr());

  });
  // .then([this,ip,port](){
  //   return listen(ipv4_addr(ip,port));
  // });
}

void SeastarServer::taskFailed(kj::Exception&& exception) {
  KJ_LOG(ERROR, exception);
}

/*class TwoPartyClient {
  // Convenience class which implements a simple client.

public:
  explicit TwoPartyClient(kj::AsyncIoStream& connection);
  TwoPartyClient(kj::AsyncIoStream& connection, Capability::Client bootstrapInterface);

  Capability::Client bootstrap();
  // Get the server's bootstrap interface.

  inline kj::Promise<void> onDisconnect() { return network.onDisconnect(); }

private:
  TwoPartyVatNetwork network;
  RpcSystem<rpc::twoparty::VatId> rpcSystem;
};//*/


// =======================================================================================


}  // namespace capnp
