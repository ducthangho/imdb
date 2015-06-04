#include "adder.capnp.hh"
#include <kj/debug.h>
#include <capnp/ez-rpc.h>
#include <capnp/message.h>
#include <iostream>
#include <map>

using namespace std;
class AdderImpl final: public Adder::Server {
public:
    kj::Promise<void> add(AddContext context) override {
        auto params = context.getParams();
        context.getResults().setValue(params.getLeft() + params.getRight());
        printf("Hello from server %d    +   %d\n",params.getLeft(), params.getRight());
        return kj::READY_NOW;
    }
};

int main(int argc, const char* argv[]) {
    if (argc != 2) {
      std::cerr << "usage: " << argv[0] << " ADDRESS[:PORT]\n"
          "Runs the server bound to the given address/port.\n"
          "ADDRESS may be '*' to bind to all local addresses.\n"
          ":PORT may be omitted to choose a port automatically." << std::endl;
      return 1;
    }

  // Set up a server.
  capnp::EzRpcServer server(kj::heap<AdderImpl>(), argv[1]);

  // Write the port number to stdout, in case it was chosen automatically.
  auto& waitScope = server.getWaitScope();
  uint port = server.getPort().wait(waitScope);
  if (port == 0) {
     // The address format "unix:/path/to/socket" opens a unix domain socket,
     // in which case the port will be zero.
     std::cout << "Listening on Unix socket..." << std::endl;
  } else {
     std::cout << "Listening on port " << port << "..." << std::endl;
  }

    // Run forever, accepting connections and handling requests.
    kj::NEVER_DONE.wait(waitScope);
}