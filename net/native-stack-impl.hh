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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef NET_NATIVE_STACK_IMPL_HH_
#define NET_NATIVE_STACK_IMPL_HH_

#include "core/reactor.hh"

#ifdef __USE_KJ__
#include "kj/debug.h"
#include "kj/async.h"
#include "kj/async-io.h"
#endif
namespace net {

template <typename Protocol>
class native_server_socket_impl;

template <typename Protocol>
class native_connected_socket_impl;

class native_network_stack;

// native_server_socket_impl
template <typename Protocol>
class native_server_socket_impl : public server_socket_impl {
    typename Protocol::listener _listener;
public:
    native_server_socket_impl(Protocol& proto, uint16_t port, listen_options opt);
    virtual future<connected_socket, socket_address> accept() override;
    virtual void abort_accept() override;

#ifdef __USE_KJ__
    virtual kj::Promise<std::pair<connected_socket, socket_address>> kj_accept() override;
#endif    

};

template <typename Protocol>
native_server_socket_impl<Protocol>::native_server_socket_impl(Protocol& proto, uint16_t port, listen_options opt)
    : _listener(proto.listen(port)) {
}

template <typename Protocol>
future<connected_socket, socket_address>
native_server_socket_impl<Protocol>::accept() {
    return _listener.accept().then([this] (typename Protocol::connection conn) {
        return make_ready_future<connected_socket, socket_address>(
                connected_socket(std::make_unique<native_connected_socket_impl<Protocol>>(std::move(conn))),
                socket_address()); // FIXME: don't fake it
    });
}

template <typename Protocol>
void
native_server_socket_impl<Protocol>::abort_accept() {
    _listener.abort_accept();
}

#ifdef __USE_KJ__
template <typename Protocol>
kj::Promise<std::pair<connected_socket, socket_address>>
native_server_socket_impl<Protocol>::kj_accept() {
    return _listener.kj_accept().then([this] (typename Protocol::connection conn) {
        return kj::Promise< std::pair<connected_socket, socket_address> >(
                std::make_pair(connected_socket(std::make_unique<native_connected_socket_impl<Protocol>>(std::move(conn))), socket_address() ) ); // FIXME: don't fake it
    });
}

#endif//*/

// native_connected_socket_impl
template <typename Protocol>
class native_connected_socket_impl : public connected_socket_impl {
    typename Protocol::connection _conn;
    class native_data_source_impl;
    class native_data_sink_impl;
public:
    explicit native_connected_socket_impl(typename Protocol::connection conn)
        : _conn(std::move(conn)) {}
    virtual input_stream<char> input() override;
    virtual output_stream<char> output() override;
    virtual void shutdown_input() override;
    virtual void shutdown_output() override;
};

template <typename Protocol>
class native_connected_socket_impl<Protocol>::native_data_source_impl final
    : public data_source_impl {
    typename Protocol::connection& _conn;
    size_t _cur_frag = 0;
    bool _eof = false;
    packet _buf;
public:
    explicit native_data_source_impl(typename Protocol::connection& conn)
        : _conn(conn) {}
    virtual future<temporary_buffer<char>> get() override {
        if (_eof) {
            return make_ready_future<temporary_buffer<char>>(temporary_buffer<char>(0));
        }
        if (_cur_frag != _buf.nr_frags()) {
            auto& f = _buf.fragments()[_cur_frag++];
            return make_ready_future<temporary_buffer<char>>(
                    temporary_buffer<char>(f.base, f.size,
                            make_deleter(deleter(), [p = _buf.share()] () mutable {})));
        }
        return _conn.wait_for_data().then([this] {
            _buf = _conn.read();
            _cur_frag = 0;
            _eof = !_buf.len();
            return get();
        });
    }
#ifdef __USE_KJ__

      virtual void setBuffer(temporary_buffer<char> && buf) override{
        
    }

    virtual kj::Promise<temporary_buffer<char>> kj_get(size_t maxBytes = 8192) override{
        if (_eof) {
            return kj::Promise<temporary_buffer<char>>(temporary_buffer<char>(0));
        }
        if (_cur_frag != _buf.nr_frags()) {
            auto& f = _buf.fragments()[_cur_frag++];
            return 
                    kj::Promise<temporary_buffer<char>>(temporary_buffer<char>(f.base, f.size,
                            make_deleter(deleter(), [p = _buf.share()] () mutable {})));
        }
        return _conn.kj_wait_for_data().then([this] {
            _buf = _conn.read();
            _cur_frag = 0;
            _eof = !_buf.len();
            return kj_get();
        });    
    };
#endif
};

template <typename Protocol>
class native_connected_socket_impl<Protocol>::native_data_sink_impl final
    : public data_sink_impl {
    typename Protocol::connection& _conn;
public:
    explicit native_data_sink_impl(typename Protocol::connection& conn)
        : _conn(conn) {}
    virtual future<> put(packet p) override {
        return _conn.send(std::move(p));
    }
    virtual future<> close() override {
        _conn.close_write();
        return make_ready_future<>();
    }
    #ifdef __USE_KJ__
    virtual kj::Promise<void> kj_put(packet p) override {
        return _conn.kj_send(std::move(p));
    }
    virtual kj::Promise<void> kj_close() override {
        _conn.close_write();
        return kj::READY_NOW;
    }
    #endif
};

template <typename Protocol>
input_stream<char>
native_connected_socket_impl<Protocol>::input() {
    data_source ds(std::make_unique<native_data_source_impl>(_conn));
    return input_stream<char>(std::move(ds));
}

template <typename Protocol>
output_stream<char>
native_connected_socket_impl<Protocol>::output() {
    data_sink ds(std::make_unique<native_data_sink_impl>(_conn));
    return output_stream<char>(std::move(ds), 8192);
}
template <typename Protocol>
void
native_connected_socket_impl<Protocol>::shutdown_input() {
    _conn.close_read();
}

template <typename Protocol>
void
native_connected_socket_impl<Protocol>::shutdown_output() {
    _conn.close_write();
}

}



#endif /* NET_NATIVE_STACK_IMPL_HH_ */
