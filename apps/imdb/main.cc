/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2014 Cloudius Systems
 */


#include "core/reactor.hh"
#include "vmts-rpc.hh"
#include <kj/compat/gtest.h>
#include "core/app-template.hh"
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
#include "adder.capnp.hh"
#include "hashprotocol.capnp.hh"



namespace bpo = boost::program_options;


using namespace kj;
using namespace net;

// static std::string str_ping{"ping"};
// static std::string str_txtx{"txtx"};
// static std::string str_rxrx{"rxrx"};
// static std::string str_pong{"pong"};
// static std::string str_unknow{"unknow cmd"};
// static int tx_msg_total_size = 100 * 1024 * 1024;
// static int tx_msg_size = 4 * 1024;
// static int tx_msg_nr = tx_msg_total_size / tx_msg_size;
// static int rx_msg_size = 4 * 1024;
// static std::string str_txbuf(tx_msg_size, 'X');


/*TEST(Async, EvalVoid) {
  EventLoop loop;
  WaitScope waitScope(loop);

  bool done = false;

  Promise<void> promise = evalLater([&]() { done = true; });
  EXPECT_FALSE(done);
  promise.wait(waitScope);
  EXPECT_TRUE(done);
}

TEST(Async, EvalInt) {
  EventLoop loop;
  WaitScope waitScope(loop);

  bool done = false;

  Promise<int> promise = evalLater([&]() { done = true; return 123; });
  EXPECT_FALSE(done);
  EXPECT_EQ(123, promise.wait(waitScope));
  EXPECT_TRUE(done);
}//*/

class AdderImpl final: public Adder::Server {
public:
    kj::Promise<void> add(AddContext context) override {
        auto params = context.getParams();
        context.getResults().setValue(params.getLeft() + params.getRight());
        printf("Hello from server %d    +   %d\n", params.getLeft(), params.getRight());
        return kj::READY_NOW;
    }
};

class HashProtocolImpl final: public HashProtocol::Server {

    std::map<int, int> mymap;

    // Implementation of the Hash Cap'n Proto interface.
    kj::Promise<void> get(GetContext context) override {
        auto request = context.getParams();
        int key = request.getKey();
        // std::cout<<"Server received Client's request key "<< key << endl;

        auto response = context.getResults();
        response.setValue(mymap[key]);
        return kj::READY_NOW;

    }

    kj::Promise<void> set(SetContext context) override {
        auto request = context.getParams();
        int key = request.getKey();
        

        int value = request.getValue();
        // KJ_DBG(key,value);
        // KJ_DBG(request);
        // std::cout<<"Client set value "<<value<<endl;
        mymap[key] = value;
        // auto response = context.getResults();
        // KJ_DBG(response);
        return kj::READY_NOW;
    }
};



class tcp_server : private kj::TaskSet::ErrorHandler {
    kj::WaitScope waitScope;    
    bool done;
    capnp::Capability::Client serverImpl;
    capnp::SeastarServer server;
    kj::TaskSet tasks;
public:
    tcp_server(): waitScope(engine()), done(false), serverImpl(kj::heap<HashProtocolImpl>()), server(serverImpl), tasks(*this) {       
        
    };
    ~tcp_server(){
        printf("Tcp Server destroyed\n");
    }

    void taskFailed(kj::Exception&& exception) {
        KJ_LOG(ERROR, exception);
    }

    void acceptLoop(ipv4_addr addr){        
        tasks.add(
            server.listen(addr).then([this,addr](){
                printf("Connection accepted\n");
                acceptLoop(addr);
            })
        );    
    }

    // future<> listen(ipv4_addr addr) {
        
    //     // listen_options lo;
    //     // lo.reuse_address = true;
    //     // _listeners.push_back(engine().listen(make_ipv4_address(addr), lo));
    //     // // do_accepts(_listeners.size() - 1);
    //     // KJ_DETACH(kj_keep_doing([this, which = _listeners.size() - 1]() {
    //     //     return do_accepts(which);
    //     // }));
    //     return make_ready_future<>();
    //     // return kj::READY_NOW;
    // }



//     kj::Promise<void> do_accepts(int which) {
//         auto pm = _listeners[which].kj_accept().then([this, which] (auto val) mutable {
//             connected_socket fd = std::move(val.first);
//             socket_address addr = std::move(val.second);
//             // std::cout<<"Thread "<<which<<" : "<<std::endl;
//             auto conn =  make_lw_shared<kj::connection>(std::move(fd), addr);
//             // std::cout<<"Connection created "<<std::endl;
//             // printf("Conn stream = %zu\n",(size_t)&(conn->_read_buf)  );
//             servers.emplace_back( kj::heap<capnp::VmtsRpcServer>(serverImpl, conn, this->waitScope)  );

//             // auto pm = conn->kj_process().then([this, conn] () {
//             //     delete conn;
//             //     try {
//             //      // printf("Connection accepted\n");
//             //     } catch (std::exception& ex) {
//             //         std::cout << "request error " << ex.what() << "\n";
//             //     }
//             // },[conn](kj::Exception&& ex){
//             //     if (conn) delete conn;
//             //     std::cout<<"Connection error "<<ex.getDescription().cStr()<<std::endl;
//             // });

//             // printf("Detach accept connect\n");
//             // KJ_DETACH(pm);
//             // pm.wait(waitScope);

//             // return pm;//*/
//             /*auto conn = new connection(*this, std::move(fd), addr);
//             conn->process().then_wrapped([this, conn] (auto&& f) {
//                 delete conn;
//                 try {
//                     f.get();
//                 } catch (std::exception& ex) {
//                     std::cout << "request error " << ex.what() << "\n";
//                 }
//             });
//             do_accepts(which);//*/
//         }, [](kj::Exception && e) {
//             printf("Exception : %s\n", e.getDescription().cStr());

//         });
//         // KJ_DETACH(pm);
//         return pm;
// // .then_wrapped([] (auto&& f) {
// //             try {
// //                 f.get();
// //             } catch (std::exception& ex) {
// //                 std::cout << "accept failed: " << ex.what() << "\n";
// //             }
// //         });
//     }
    /*class connection {
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
    public:
        connection(tcp_server& server, connected_socket&& fd, socket_address addr)
            : _fd(std::move(fd))
            , _read_buf(_fd.input())
            , _write_buf(_fd.output())
             {}
        future<> process() {
             return read();
        }
        future<> read() {
            if (_read_buf.eof()) {
                return make_ready_future();
            }
            // Expect 4 bytes cmd from client
            size_t n = 4;
            return _read_buf.read_exactly(n).then([this] (temporary_buffer<char> buf) {
                if (buf.size() == 0) {
                    return make_ready_future();
                }
                auto cmd = std::string(buf.get(), buf.size());
                // pingpong test
                if (cmd == str_ping) {
                    return _write_buf.write(str_pong).then([this] {
                        return _write_buf.flush();
                    }).then([this] {
                        return this->read();
                    });
                // server tx test
                } else if (cmd == str_txtx) {
                    return tx_test();
                // server tx test
                } else if (cmd == str_rxrx) {
                    return rx_test();
                // unknow test
                } else {
                    return _write_buf.write(str_unknow).then([this] {
                        return _write_buf.flush();
                    }).then([this] {
                        return make_ready_future();
                    });
                }
            });
        }
        future<> do_write(int end) {
            if (end == 0) {
                return make_ready_future<>();
            }
            return _write_buf.write(str_txbuf).then([this, end] {
                return _write_buf.flush();
            }).then([this, end] {
                return do_write(end - 1);
            });
        }
        future<> tx_test() {
            return do_write(tx_msg_nr).then([this] {
                return _write_buf.close();
            }).then([this] {
                return make_ready_future<>();
            });
        }
        future<> do_read() {
            return _read_buf.read_exactly(rx_msg_size).then([this] (temporary_buffer<char> buf) {
                if (buf.size() == 0) {
                    return make_ready_future();
                } else {
                    return do_read();
                }
            });
        }
        future<> rx_test() {
            return do_read().then([] {
                return make_ready_future<>();
            });
        }


    #ifdef __USE_KJ__

        kj::Promise<void> kj_process() {
             return kj_read();
        }
        kj::Promise<void> kj_read() {
            if (_read_buf.eof()) {
                return kj::READY_NOW;
            }
            // Expect 4 bytes cmd from client
            size_t n = 4;
            return _read_buf.kj_read_exactly(n).then([this] (temporary_buffer<char> buf) -> kj::Promise<void> {
                if (buf.size() == 0) {
                    return kj::READY_NOW;
                }
                auto cmd = std::string(buf.get(), buf.size());
                // printf("%s\n",cmd.c_str());
                // pingpong test
                if (cmd == str_ping) {
                    return _write_buf.kj_write(str_pong).then([this] {
                        return _write_buf.kj_flush();
                    }).then([this] {
                        return this->kj_read();
                    });
                // server tx test
                } else if (cmd == str_txtx) {
                    return kj_tx_test();
                // server tx test
                } else if (cmd == str_rxrx) {
                    return kj_rx_test();
                // unknow test
                } else {
                    return _write_buf.kj_write(str_unknow).then([this] {
                        return _write_buf.kj_flush();
                    });
                }
            });
        }
        kj::Promise<void> kj_do_write(int end) {
            if (end == 0) {
                return kj::READY_NOW;
            }
            return _write_buf.kj_write(str_txbuf).then([this, end] {
                return _write_buf.kj_flush();
            }).then([this, end] {
                return kj_do_write(end - 1);
            });
        }
        kj::Promise<void> kj_tx_test() {
            return kj_do_write(tx_msg_nr).then([this] {
                return _write_buf.kj_close();
            });
        }
        kj::Promise<void> kj_do_read() {
            return _read_buf.kj_read_exactly(rx_msg_size).then([this] (temporary_buffer<char> buf) -> kj::Promise<void> {
                if (buf.size() == 0) {
                    return kj::READY_NOW;
                } else {
                    return this->kj_do_read();
                }
            });
        }
        kj::Promise<void> kj_rx_test() {
            return kj_do_read();
        }
    #endif
    };
    };


    inline kj::Promise<void> convert(future<>&& pm){
    auto pair = kj::newPromiseAndFulfiller<void>();
    // auto kj_pm = kj::mv(pair.fulfiller);
    // auto lambda = capture2 ( kj::mv(pair.fulfiller),  [](kj::Own<kj::PromiseFulfiller<void> >&& fulfiller) mutable{
    //     fulfiller->fulfill();//FuallFill kj::Promise
    // });
    // pm.then([lambda = capture2 ( kj::mv(pair.fulfiller),  [](auto&& fulfiller) mutable {
    //     fulfiller->fulfill();//FullFill kj::Promise
    // }) ] () mutable {
    //     lambda();
    // });
    auto f = [&pm] (auto&& fulfiller){

        return pm.then([&fulfiller]{
            std::cout<<"ABCDEF"<<std::endl;
            fulfiller->fulfill();
        });
    };

    do_with( kj::mv(pair.fulfiller), std::move(f) );
    // kj::Own<kj::Own< kj::PromiseFulfiller<void>>>  fulfiller = kj::heap<kj::Own<kj::PromiseFulfiller<void>>>(kj::mv(pair.fulfiller));
    // pm.then([f = kj::mv(fulfiller)]() mutable{
    //     (*f)->fulfill();
    //     std::cout<<"ABCDEF"<<std::endl;
    // });
    // auto kj_pm = pair.promise.then([](){
    //     std::cout<<"Caught"<<std::endl;
    // });
    // pair.fulfiller->fulfill();
    // kj_pm.wait(waitScope);
    return kj::mv(pair.promise);
    }//*/
};




int main(int ac, char** av)
{ 

    app_template app;
    app.add_options()("port", bpo::value<uint16_t>()->default_value(9090),
                      "Database Server port");
    return app.kj_run(ac, av,
    [&] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        auto server = new distributed<tcp_server>;
        // auto kj_promise =
        server->start().then([server = std::move(server), port] () mutable {
            server->invoke_on_all(&tcp_server::acceptLoop, ipv4_addr{port});
        }).then([port] {
            std::cout << "Seastar TCP server listening on port " << port << " ...\n";
        });


        // EventLoop& loop = (EventLoop)(engine());
        // WaitScope waitScope(engine());

        // auto pair2 = kj::newPromiseAndFulfiller<int>();

        // auto pm2 = pair2.promise.then([](int val){
        //     std::cout<<"Fired : "<<val<<std::endl;
        //     // return kj::READY_NOW;
        // });

        // auto pair = kj::newPromiseAndFulfiller<void>();

        // auto pm = pair.promise.then([](){
        //     std::cout<<"Fired : "<<"void"<<std::endl;
        //     // return kj::READY_NOW;
        // }).then([](){
        //     std::cout<<"Ehem"<<std::endl;
        // });

        // // bool done = false;

        // std::cout<<"Prepare to fire"<<std::endl;
        // EXPECT_TRUE(pair.fulfiller->isWaiting());
        // pair.fulfiller->fulfill();
        // EXPECT_FALSE(pair.fulfiller->isWaiting());

        // pair2.fulfiller->fulfill(235);
        // pm.wait(waitScope);
        // pm2.wait(waitScope);
        // kj_promise.detach([&](kj::Exception&& e) {
        //     std::cout<<"End attachment"<<std::endl;
        // });

        // auto pr = kj_promise.then([](){
        //     printf("What the fuck\n");
        // }).then([](){
        //     printf("Akay.com\n");
        // });

        // KJ_DETACH(pr);

        std::cout << "Done" << std::endl;

        //
        // EXPECT_EQ(123, npm.wait(waitScope));
        // std::cout<<"123"<<std::endl;
        // kj_promise.wait(waitScope);
    });
    return 0;
}


