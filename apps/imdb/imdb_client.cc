#include <kj/async.h>
#include <kj/debug.h>
#include <capnp/ez-rpc.h>
#include <capnp/message.h>
#include <iostream>
#include <chrono>
#include "adder.capnp.hh"

using namespace std;
int main(int argc, const char* argv[]){
	if (argc != 2) {
    	std::cerr << "usage: " << argv[0] << " HOST:PORT\n"
        	"Connects to the Hash server at the given address and "
        	"does some RPCs." << std::endl;
    	return 1;
  	}

  	capnp::EzRpcClient client(argv[1]);
  	auto cap = client.getMain<Adder>();

  	auto& waitScope = client.getWaitScope();



    auto start = std::chrono::high_resolution_clock::now();

    auto request = cap.addRequest();
    request.setLeft(20);
    request.setRight(30);
    auto promise = request.send();

    promise.then([](auto val) -> kj::Promise<void>{
    	printf("Sum = %d\n",val.getValue());
    	return kj::READY_NOW;
    }).wait(waitScope);
  	

    auto finish = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count();
    std::cout << "Time = " << duration << " milliseconds" << std::endl;    
}
