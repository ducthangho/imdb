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
#include "capnp/serialize-async.h"
#include <map>
#include "core/future.hh"
#include "core/reactor.hh"
// #include "event_port.hh"

using namespace kj;

namespace capnp {

KJ_THREADLOCAL_PTR(VmtsRpcContext) threadVmtsContext = nullptr;

class VmtsRpcContext: public kj::Refcounted {
public:

  VmtsRpcContext() : ioContext( kj::newAsyncIoProvider(llaiop)) {    
    threadVmtsContext = this;
  };

  VmtsRpcContext(lw_shared_ptr<kj::connection>& _conn,kj::WaitScope& _waitScope): llaiop(_conn), ioContext( kj::newAsyncIoProvider(llaiop) ),waitScope(&_waitScope) {    
    threadVmtsContext = this;
  }

  ~VmtsRpcContext() noexcept(false) {
    KJ_REQUIRE(threadVmtsContext == this,
               "VmtsRpcContext destroyed from different thread than it was created.") {
      return;
    }
    threadVmtsContext = nullptr;
  }

  kj::WaitScope& getWaitScope() {
    return *waitScope;
  }

  kj::AsyncIoProvider& getIoProvider() {
    return *ioContext;
  }

  kj::LowLevelAsyncIoProvider& getLowLevelIoProvider() {
    return llaiop;
  }


  static kj::Own<VmtsRpcContext> getThreadLocal() {
    VmtsRpcContext* existing = threadVmtsContext;
    if (existing != nullptr) {
      return kj::addRef(*existing);
    } else {
      return kj::refcounted<VmtsRpcContext>();
    }
  }


  static kj::Own<VmtsRpcContext> getThreadLocal(lw_shared_ptr<kj::connection>& _conn,kj::WaitScope& _waitScope) {
    VmtsRpcContext* existing = threadVmtsContext;
    if (existing != nullptr) {
      printf("Alo\n");
      return kj::addRef(*existing);
    } else {
      printf("123\n");
      try{
        auto a = kj::refcounted<VmtsRpcContext>(_conn,_waitScope);
        return a;
      } catch( kj::Exception& ex ){
        std::cout<<"Exception "<<ex.getDescription().cStr()<<std::endl;
      }
      return kj::refcounted<VmtsRpcContext>(_conn,_waitScope);
    }
  }

private:
  kj::UvLowLevelAsyncIoProvider llaiop;
  kj::Own<kj::AsyncIoProvider> ioContext;
  kj::WaitScope * waitScope;
};

// =======================================================================================

kj::Promise<kj::Own<kj::AsyncIoStream>> connectAttach(kj::Own<kj::NetworkAddress>&& addr) {
  return addr->connect().attach(kj::mv(addr));
}

struct VmtsRpcClient::Impl {
  kj::Own<VmtsRpcContext> context;

  struct ClientContext {
    kj::Own<kj::AsyncIoStream> stream;
    TwoPartyVatNetwork network;
    RpcSystem<rpc::twoparty::VatId> rpcSystem;

    ClientContext(kj::Own<kj::AsyncIoStream>&& stream, ReaderOptions readerOpts)
      : stream(kj::mv(stream)),
        network(*this->stream, rpc::twoparty::Side::CLIENT, readerOpts),
        rpcSystem(makeRpcClient(network)) {}

    Capability::Client getMain() {
      word scratch[4];
      memset(scratch, 0, sizeof(scratch));
      MallocMessageBuilder message(scratch);
      auto hostId = message.getRoot<rpc::twoparty::VatId>();
      hostId.setSide(rpc::twoparty::Side::SERVER);
      return rpcSystem.bootstrap(hostId);
    }

    Capability::Client restore(kj::StringPtr name) {
      word scratch[64];
      memset(scratch, 0, sizeof(scratch));
      MallocMessageBuilder message(scratch);

      auto hostIdOrphan = message.getOrphanage().newOrphan<rpc::twoparty::VatId>();
      auto hostId = hostIdOrphan.get();
      hostId.setSide(rpc::twoparty::Side::SERVER);

      auto objectId = message.getRoot<AnyPointer>();
      objectId.setAs<Text>(name);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
      return rpcSystem.restore(hostId, objectId);
#pragma GCC diagnostic pop
    }
  };

  kj::ForkedPromise<void> setupPromise;

  kj::Maybe<kj::Own<ClientContext>> clientContext;
  // Filled in before `setupPromise` resolves.

  Impl(kj::StringPtr serverAddress, uint defaultPort,
       ReaderOptions readerOpts)
    : context(VmtsRpcContext::getThreadLocal()),
      setupPromise(context->getIoProvider().getNetwork()
                   .parseAddress(serverAddress, defaultPort)
                   .then([readerOpts](kj::Own<kj::NetworkAddress> && addr) {
    return connectAttach(kj::mv(addr));
  }).then([this, readerOpts](kj::Own<kj::AsyncIoStream>&& stream) {
    clientContext = kj::heap<ClientContext>(kj::mv(stream),
                                            readerOpts);
  }).fork()) {}

  Impl(const struct sockaddr* serverAddress, uint addrSize,
       ReaderOptions readerOpts)
    : context(VmtsRpcContext::getThreadLocal()),
      setupPromise(
        connectAttach(context->getIoProvider().getNetwork()
                      .getSockaddr(serverAddress, addrSize))
        .then([this, readerOpts](kj::Own<kj::AsyncIoStream> && stream) {
    clientContext = kj::heap<ClientContext>(kj::mv(stream),
                                            readerOpts);
  }).fork()) {}

  Impl(int socketFd, ReaderOptions readerOpts)
    : context(VmtsRpcContext::getThreadLocal()),
      setupPromise(kj::Promise<void>(kj::READY_NOW).fork()),
      clientContext(kj::heap<ClientContext>(
                      context->getLowLevelIoProvider().wrapSocketFd(socketFd),
                      readerOpts)) {}
};

VmtsRpcClient::VmtsRpcClient(kj::StringPtr serverAddress, uint defaultPort, ReaderOptions readerOpts)
  : impl(kj::heap<Impl>(serverAddress, defaultPort, readerOpts)) {}

VmtsRpcClient::VmtsRpcClient(const struct sockaddr* serverAddress, uint addrSize, ReaderOptions readerOpts)
  : impl(kj::heap<Impl>(serverAddress, addrSize, readerOpts)) {}

VmtsRpcClient::VmtsRpcClient(int socketFd, ReaderOptions readerOpts)
  : impl(kj::heap<Impl>(socketFd, readerOpts)) {}

VmtsRpcClient::~VmtsRpcClient() noexcept(false) {}

Capability::Client VmtsRpcClient::getMain() {
  KJ_IF_MAYBE(client, impl->clientContext) {
    return client->get()->getMain();
  } else {
    return impl->setupPromise.addBranch().then([this]() {
      return KJ_ASSERT_NONNULL(impl->clientContext)->getMain();
    });
  }
}

Capability::Client VmtsRpcClient::importCap(kj::StringPtr name) {
  KJ_IF_MAYBE(client, impl->clientContext) {
    return client->get()->restore(name);
  } else {
    return impl->setupPromise.addBranch().then(kj::mvCapture(kj::heapString(name),
    [this](kj::String && name) {
      return KJ_ASSERT_NONNULL(impl->clientContext)->restore(name);
    }));
  }
}

kj::WaitScope& VmtsRpcClient::getWaitScope() {
  return impl->context->getWaitScope();
}

kj::AsyncIoProvider& VmtsRpcClient::getIoProvider() {
  return impl->context->getIoProvider();
}

kj::LowLevelAsyncIoProvider& VmtsRpcClient::getLowLevelIoProvider() {
  return impl->context->getLowLevelIoProvider();
}


/****************************************************************************************/

SeastarNetwork::SeastarNetwork(kj::AsyncIoStream& stream, rpc::twoparty::Side side,
                                       ReaderOptions receiveOptions)
    : stream(stream), side(side), receiveOptions(receiveOptions), previousWrite(kj::READY_NOW) {
  auto paf = kj::newPromiseAndFulfiller<void>();
  disconnectPromise = paf.promise.fork();
  disconnectFulfiller.fulfiller = kj::mv(paf.fulfiller);
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
  FlatMessageBuilder message;
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

kj::Promise<kj::Maybe<kj::Own<IncomingRpcMessage>>> SeastarNetwork::receiveIncomingMessage() {
  return kj::evalLater([&]() {
    return tryReadMessage(stream, receiveOptions)
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



// =======================================================================================

struct VmtsRpcServer::Impl final: public SturdyRefRestorer<AnyPointer>,
  public kj::TaskSet::ErrorHandler {
  Capability::Client mainInterface;
  kj::Own<VmtsRpcContext> context;

  struct ExportedCap {
    kj::String name;
    Capability::Client cap = nullptr;

    ExportedCap(kj::StringPtr name, Capability::Client cap)
      : name(kj::heapString(name)), cap(cap) {}

    ExportedCap() = default;
    ExportedCap(const ExportedCap&) = delete;
    ExportedCap(ExportedCap&&) = default;
    ExportedCap& operator=(const ExportedCap&) = delete;
    ExportedCap& operator=(ExportedCap&&) = default;
    // Make std::map happy...
  };

  std::map<kj::StringPtr, ExportedCap> exportMap;

  kj::ForkedPromise<uint> portPromise;

  kj::TaskSet tasks;  

  struct ServerContext {
    kj::Own<kj::AsyncIoStream> stream;
    SeastarNetwork network;
    RpcSystem<rpc::twoparty::VatId> rpcSystem;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    ServerContext(kj::Own<kj::AsyncIoStream>&& stream, SturdyRefRestorer<AnyPointer>& restorer,
                  ReaderOptions readerOpts)
      : stream(kj::mv(stream)),
        network(*this->stream, rpc::twoparty::Side::SERVER, readerOpts),
        rpcSystem(makeRpcServer(network, restorer)) { }
#pragma GCC diagnostic pop
        ~ServerContext(){
          printf("ServerContext destroyed\n");
        }
  };

  kj::Own<ServerContext> server;

  void handleLoop(lw_shared_ptr<kj::connection>& conn, ReaderOptions readerOpts){    
    try{      
      kj::Own<kj::AsyncIoStream> stream(kj::heap<UvIoStream>(conn));    
      server = kj::heap<ServerContext>(kj::mv(stream), *this, readerOpts);                
      tasks.add(server->network.onDisconnect().then([](){
        printf("Client disconnected!\n");
      }));      
    } catch(kj::Exception& e){
      printf("Exception : %s\n",e.getDescription().cStr());
    }
    // handleLoop(conn,readerOpts);
  }

  ~Impl(){
    printf("Impl destroyed\n");  
  }


  Impl(Capability::Client mainInterface, lw_shared_ptr<kj::connection>& _conn, kj::WaitScope& ws, ReaderOptions readerOpts):
    mainInterface(kj::mv(mainInterface)), context( kj::heap<VmtsRpcContext>(_conn, ws)),  portPromise(nullptr), tasks(*this) {
      // acceptLoop(context->getLowLevelIoProvider().wrapListenSocketFd(0), readerOpts);            
      handleLoop(_conn,readerOpts);
  }

  Impl(Capability::Client mainInterface, kj::StringPtr bindAddress, uint defaultPort,
       ReaderOptions readerOpts)
    : mainInterface(kj::mv(mainInterface)),
      context(VmtsRpcContext::getThreadLocal()), portPromise(nullptr), tasks(*this) {
    auto paf = kj::newPromiseAndFulfiller<uint>();
    portPromise = paf.promise.fork();

    tasks.add(context->getIoProvider().getNetwork().parseAddress(bindAddress, defaultPort)
              .then(kj::mvCapture(paf.fulfiller,
                                  [this, readerOpts](kj::Own<kj::PromiseFulfiller<uint>> && portFulfiller,
    kj::Own<kj::NetworkAddress> && addr) {
      auto listener = addr->listen();
      portFulfiller->fulfill(listener->getPort());
      acceptLoop(kj::mv(listener), readerOpts);
    })));
  }

  Impl(Capability::Client mainInterface, struct sockaddr* bindAddress, uint addrSize,
       ReaderOptions readerOpts)
    : mainInterface(kj::mv(mainInterface)),
      context(VmtsRpcContext::getThreadLocal()), portPromise(nullptr), tasks(*this) {
    auto listener = context->getIoProvider().getNetwork()
                    .getSockaddr(bindAddress, addrSize)->listen();
    portPromise = kj::Promise<uint>(listener->getPort()).fork();
    acceptLoop(kj::mv(listener), readerOpts);
  }

  Impl(Capability::Client mainInterface, int socketFd, uint port, ReaderOptions readerOpts)
    : mainInterface(kj::mv(mainInterface)),
      context(VmtsRpcContext::getThreadLocal()),
      portPromise(kj::Promise<uint>(port).fork()),
      tasks(*this) {
    acceptLoop(context->getLowLevelIoProvider().wrapListenSocketFd(socketFd), readerOpts);
  }
  
  void acceptLoop(kj::Own<kj::ConnectionReceiver>&& listener, ReaderOptions readerOpts) {
    auto ptr = listener.get();
    tasks.add(ptr->accept().then(kj::mvCapture(kj::mv(listener),
                                 [this, readerOpts](kj::Own<kj::ConnectionReceiver> && listener,
    kj::Own<kj::AsyncIoStream> && connection) {
      acceptLoop(kj::mv(listener), readerOpts);

      auto server = kj::heap<ServerContext>(kj::mv(connection), *this, readerOpts);

      // Arrange to destroy the server context when all references are gone, or when the
      // VmtsRpcServer is destroyed (which will destroy the TaskSet).
      tasks.add(server->network.onDisconnect().attach(kj::mv(server)));
    }
                                              ))
             );
  }

  Capability::Client restore(AnyPointer::Reader objectId) override {
    if (objectId.isNull()) {
      return mainInterface;
    } else {
      auto name = objectId.getAs<Text>();
      auto iter = exportMap.find(name);
      if (iter == exportMap.end()) {
        KJ_FAIL_REQUIRE("Server exports no such capability.", name) { break; }
        return nullptr;
      } else {
        return iter->second.cap;
      }
    }
  }

  void taskFailed(kj::Exception&& exception) override {
    kj::throwFatalException(kj::mv(exception));
  }
};

VmtsRpcServer::VmtsRpcServer(Capability::Client mainInterface, lw_shared_ptr<kj::connection>& _conn, kj::WaitScope& ws, ReaderOptions readerOpts)
  : impl(kj::heap<Impl>(kj::mv(mainInterface),  _conn, ws, readerOpts)) {}


VmtsRpcServer::VmtsRpcServer(Capability::Client mainInterface, kj::StringPtr bindAddress,
                             uint defaultPort, ReaderOptions readerOpts)
  : impl(kj::heap<Impl>(kj::mv(mainInterface), bindAddress, defaultPort, readerOpts)) {}

VmtsRpcServer::VmtsRpcServer(Capability::Client mainInterface, struct sockaddr* bindAddress,
                             uint addrSize, ReaderOptions readerOpts)
  : impl(kj::heap<Impl>(kj::mv(mainInterface), bindAddress, addrSize, readerOpts)) {}

VmtsRpcServer::VmtsRpcServer(Capability::Client mainInterface, int socketFd, uint port,
                             ReaderOptions readerOpts)
  : impl(kj::heap<Impl>(kj::mv(mainInterface), socketFd, port, readerOpts)) {}

VmtsRpcServer::VmtsRpcServer(kj::StringPtr bindAddress, uint defaultPort,
                             ReaderOptions readerOpts)
  : VmtsRpcServer(nullptr, bindAddress, defaultPort, readerOpts) {}

VmtsRpcServer::VmtsRpcServer(struct sockaddr* bindAddress, uint addrSize,
                             ReaderOptions readerOpts)
  : VmtsRpcServer(nullptr, bindAddress, addrSize, readerOpts) {}

VmtsRpcServer::VmtsRpcServer(int socketFd, uint port, ReaderOptions readerOpts)
  : VmtsRpcServer(nullptr, socketFd, port, readerOpts) {}

VmtsRpcServer::~VmtsRpcServer() noexcept(false) {
  printf("VmtsRpcServer destroyed\n");
}

void VmtsRpcServer::exportCap(kj::StringPtr name, Capability::Client cap) {
  Impl::ExportedCap entry(kj::heapString(name), cap);
  impl->exportMap[entry.name] = kj::mv(entry);
}

kj::Promise<uint> VmtsRpcServer::getPort() {
  return impl->portPromise.addBranch();
}

kj::WaitScope& VmtsRpcServer::getWaitScope() {
  return  impl->context->getWaitScope();
}

kj::AsyncIoProvider& VmtsRpcServer::getIoProvider() {
  return impl->context->getIoProvider();
}

kj::LowLevelAsyncIoProvider& VmtsRpcServer::getLowLevelIoProvider() {
  return impl->context->getLowLevelIoProvider();
}

}  // namespace capnp
