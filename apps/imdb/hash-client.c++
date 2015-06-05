#include "hashprotocol.capnp.hh"
#include <kj/debug.h>
#include <capnp/ez-rpc.h>
#include <capnp/message.h>
#include <iostream>
#include <chrono>

using namespace std;

class ErrorHandlerImpl: public kj::TaskSet::ErrorHandler {
public:
  int exceptionCount = 0;
  void taskFailed(kj::Exception&& exception) override {
    // EXPECT_TRUE(exception.getDescription().endsWith("example TaskSet failure"));
    // ++exceptionCount;
  }
};

int main(int argc, const char* argv[]) {
 	if (argc != 2) {
    	std::cerr << "usage: " << argv[0] << " HOST:PORT\n"
        	"Connects to the Hash server at the given address and "
        	"does some RPCs." << std::endl;
    	return 1;
  	}

  	capnp::EzRpcClient client(argv[1]);
  	HashProtocol::Client hashprotocol = client.getMain<HashProtocol>();

      int iterrationsCnt =10000;
    // printf("IterationCnt =  %d\n", iterrationsCnt);

  	auto& waitScope = client.getWaitScope();

    auto start = std::chrono::high_resolution_clock::now();
    ErrorHandlerImpl errorHandler;
    kj::TaskSet tasks(errorHandler);

    auto builder = kj::heapArrayBuilder<kj::Promise<int>>(iterrationsCnt);
    for (int i = 0; i < iterrationsCnt; i++){
        {

        auto request = hashprotocol.setRequest();
        request.setKey(i);
        // cout << "Client set Key is  : " << request.getKey() << endl;
        request.setValue(123);
        // cout << "Client set Value is: " << request.getValue() << endl;
        auto promise = request.send().then([&]( capnp::Response<HashProtocol::SetResults> && v){
          auto request = hashprotocol.getRequest();
          request.setKey(i);
          // cout << "Client request value of Key " << request.getKey() << endl;
          auto promise = request.send();
          
          auto pm = promise.then([](capnp::Response<HashProtocol::GetResults>&& val) -> kj::Promise<int>{
            // cout << "Received value is "<< val.getValue() << endl;          
            return val.getValue();
          });
          return kj::mv(pm);
        });
        builder.add(kj::mv(promise));  
        // promise.wait(waitScope);
        // tasks.add(promise);
        // promise.detach([](kj::Exception&& ex){});
       }

       
      
      
      // for (int i =0; i <10000; i++){
        {
          
        // printf("Value received = %d\n",promise.wait(waitScope).getValue());
        // int val = promise.wait(waitScope).getValue();
        // printf("Get here\n");
        // promise.wait(waitScope);
        // pm.detach([](kj::Exception&& ex){});
        // tasks.add(kj::mv(pm) );
        }
    }
    auto promise = kj::joinPromises(builder.finish());
    promise.wait(waitScope);

    auto finish = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count();
    std::cout << "Time = " << duration << " milliseconds" << std::endl << std::endl;

}
