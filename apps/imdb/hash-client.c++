#include "hashprotocol.capnp.hh"
#include <kj/debug.h>
#include <capnp/ez-rpc.h>
#include <capnp/message.h>
#include <iostream>
#include <chrono>

using namespace std;
int main(int argc, const char* argv[]) {
 	if (argc != 2) {
    	std::cerr << "usage: " << argv[0] << " HOST:PORT\n"
        	"Connects to the Hash server at the given address and "
        	"does some RPCs." << std::endl;
    	return 1;
  	}

  	capnp::EzRpcClient client(argv[1]);
  	HashProtocol::Client hashprotocol = client.getMain<HashProtocol>();

  	auto& waitScope = client.getWaitScope();

    int iterationsCnt= 100000;

    auto start = std::chrono::high_resolution_clock::now();

  	for (int iterations = 0; iterations < iterationsCnt; iterations++){
        {

    		auto request = hashprotocol.setRequest();
    		request.setKey(123);
    		// cout << "Client set Key is  : " << request.getKey() << endl;
    		request.setValue(iterations*10);
    		// cout << "Client set Value is: " << request.getValue() << endl;
    		auto promise = request.send();
    		promise.wait(waitScope);
    	}

    	{
            auto request = hashprotocol.getRequest();
    		request.setKey(iterations);
    		// cout << "Client request value of Key " << request.getKey() << endl;
    		auto promise = request.send();
    		// auto pm = promise.then([](capnp::Response<HashProtocol::GetResults>&& val){
    		// 	// cout << "Received value is "<< val.getValue() << endl;
    		// 	return 0;
    		// });
    		// printf("Value received = %d\n",promise.wait(waitScope).getValue());
    		// int val = promise.wait(waitScope).getValue();
    		// printf("Get here\n");
    		promise.wait(waitScope);
        }
    }

    auto finish = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count();
    std::cout << "Time = " << duration << " milliseconds" << std::endl;
    std::cout << "Iteration = "<< iterationsCnt << endl;

}
