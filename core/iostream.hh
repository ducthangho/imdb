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
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

//
// Buffered input and output streams
//
// Two abstract classes (data_source and data_sink) provide means
// to acquire bulk data from, or push bulk data to, some provider.
// These could be tied to a TCP connection, a disk file, or a memory
// buffer.
//
// Two concrete classes (input_stream and output_stream) buffer data
// from data_source and data_sink and provide easier means to process
// it.
//

#pragma once

#include "future.hh"
#include "temporary_buffer.hh"
#include "scattered_message.hh"
#ifdef __USE_KJ__
#include "core/do_with.hh"
#include "kj/debug.h"
#include "kj/async.h"
#include "kj/async-io.h"
#include <limits.h>
#endif

#if !defined(IOV_MAX) && defined(UIO_MAXIOV)
#define IOV_MAX UIO_MAXIOV
#endif

namespace net { class packet; }

class data_source_impl {
public:
    virtual ~data_source_impl() {}
    virtual future<temporary_buffer<char>> get() = 0;
#ifdef __USE_KJ__
    virtual void setBuffer(temporary_buffer<char> && buf) = 0;
    virtual kj::Promise<temporary_buffer<char>> kj_get(size_t maxBytes = 8192) = 0;
#endif
};

class data_source {
    std::unique_ptr<data_source_impl> _dsi;
protected:
    data_source_impl* impl() const { return _dsi.get(); }
public:
    data_source() = default;
    explicit data_source(std::unique_ptr<data_source_impl> dsi) : _dsi(std::move(dsi)) {}
    data_source(data_source&& x) = default;
    data_source& operator=(data_source&& x) = default;
    future<temporary_buffer<char>> get() { return _dsi->get(); }
#ifdef __USE_KJ__

    void setBuffer(temporary_buffer<char> && buf){
        _dsi->setBuffer(std::move(buf) );
    }

    kj::Promise<temporary_buffer<char>> kj_get(size_t maxBytes = 8192) {
        return _dsi->kj_get(maxBytes);
    };
#endif
};

class data_sink_impl {
public:
    virtual ~data_sink_impl() {}
    virtual temporary_buffer<char> allocate_buffer(size_t size) {
        return temporary_buffer<char>(size);
    }
    virtual future<> put(net::packet data) = 0;
    virtual future<> put(std::vector<temporary_buffer<char>> data) {
        net::packet p;
        p.reserve(data.size());
        for (auto& buf : data) {
            p = net::packet(std::move(p), net::fragment{buf.get_write(), buf.size()}, buf.release());
        }
        return put(std::move(p));
    }
    virtual future<> put(temporary_buffer<char> buf) {
        return put(net::packet(net::fragment{buf.get_write(), buf.size()}, buf.release()));
    }
    virtual future<> close() = 0;
#ifdef __USE_KJ__
    virtual kj::Promise<void> kj_put(net::packet data) = 0;
    virtual kj::Promise<void> kj_put(std::vector<temporary_buffer<char>> data) {
        net::packet p;        
        p.reserve(data.size());
        for (auto& buf : data) {
            p = net::packet(std::move(p), net::fragment{buf.get_write(), buf.size()}, buf.release());
        }
        return kj_put(std::move(p));
    }
    virtual kj::Promise<void> kj_put(temporary_buffer<char> buf) {
        return kj_put(net::packet(net::fragment{buf.get_write(), buf.size()}, buf.release()));
    }

    virtual  kj::Promise<void> kj_close() = 0;
#endif
};

class data_sink {
    std::unique_ptr<data_sink_impl> _dsi;
public:
    data_sink() = default;
    explicit data_sink(std::unique_ptr<data_sink_impl> dsi) : _dsi(std::move(dsi)) {}
    data_sink(data_sink&& x) = default;
    data_sink& operator=(data_sink&& x) = default;
    temporary_buffer<char> allocate_buffer(size_t size) {
        return _dsi->allocate_buffer(size);
    }
    future<> put(std::vector<temporary_buffer<char>> data) {
        return _dsi->put(std::move(data));
    }
    future<> put(temporary_buffer<char> data) {
        return _dsi->put(std::move(data));
    }
    future<> put(net::packet p) {
        return _dsi->put(std::move(p));
    }
    future<> close() { return _dsi->close(); }
#ifdef __USE_KJ__
    kj::Promise<void> kj_put(std::vector<temporary_buffer<char>> data) {
        return _dsi->kj_put(std::move(data));
    }
    kj::Promise<void> kj_put(temporary_buffer<char> data) {
        return _dsi->kj_put(std::move(data));
    }
    kj::Promise<void> kj_put(net::packet p) {
        return _dsi->kj_put(std::move(p));
    }
    kj::Promise<void> kj_close() { return _dsi->kj_close(); }
#endif
};

#ifdef __USE_KJ__
template <typename CharType>
class input_stream final : public kj::AsyncInputStream {
#else
template <typename CharType>
class input_stream final {
#endif
    static_assert(sizeof(CharType) == 1, "must buffer stream of bytes");
    data_source _fd;
    temporary_buffer<CharType> _buf;
    bool _eof = false;
private:
    using tmp_buf = temporary_buffer<CharType>;
    size_t available() const { return _buf.size(); }
protected:
    void reset() { _buf = {}; }
    data_source* fd() { return &_fd; }
public:
    // Consumer concept, for consume() method:
    using unconsumed_remainder = std::experimental::optional<tmp_buf>;
    struct ConsumerConcept {
        // The consumer should operate on the data given to it, and
        // return a future "unconsumed remainder", which can be undefined
        // if the consumer consumed all the input given to it and is ready
        // for more, or defined when the consumer is done (and in that case
        // the value is the unconsumed part of the last data buffer - this
        // can also happen to be empty).
        future<unconsumed_remainder> operator()(tmp_buf data);
    };
    using char_type = CharType;
    input_stream() = default;
    explicit input_stream(data_source fd) : _fd(std::move(fd)), _buf(0) {}
    input_stream(input_stream&&) = default;
    input_stream& operator=(input_stream&&) = default;
    future<temporary_buffer<CharType>> read_exactly(size_t n);
    template <typename Consumer>
    future<> consume(Consumer& c);
    bool eof() { return _eof; }
#ifdef __USE_KJ__

    template <typename Consumer>
    kj::Promise<void> kj_consume(Consumer& c);

    kj::Promise<size_t> tryRead(void* buffer, size_t minBytes, size_t maxBytes) override {
        return tryReadInternal(buffer, minBytes, maxBytes, 0);
    }

    kj::Promise<size_t> read(void* buffer, size_t minBytes, size_t maxBytes) override {
        return tryReadInternal(buffer, minBytes, maxBytes, 0).then([ = ](size_t result) {
            KJ_REQUIRE(result >= minBytes, "Premature EOF") {
                // Pretend we read zeros from the input.
                memset(reinterpret_cast<kj::byte*>(buffer) + result, 0, minBytes - result);
                return minBytes;
            }
            return result;
        });
    }

    kj::Promise<temporary_buffer<CharType>> kj_read_exactly(size_t n);

#endif
private:
    future<temporary_buffer<CharType>> read_exactly_part(size_t n, tmp_buf buf, size_t completed);
#ifdef __USE_KJ__


    kj::Promise<temporary_buffer<CharType>> kj_read_exactly_part(size_t n, tmp_buf buf, size_t completed);

    kj::Promise<size_t> tryReadInternal(void* buffer, size_t minBytes, size_t maxBytes,
                                        size_t alreadyRead) {


        // printf("tryReadInternal  %zu, %zu \n",minBytes,maxBytes);
        if (available()) {//Should not happen now
            printf("Available \n");
            auto now = std::min(maxBytes, available());
            if (_buf.get() != reinterpret_cast<CharType*>(buffer) + alreadyRead){
                printf("Copy here\n");
                std::copy(_buf.get(), _buf.get() + now, reinterpret_cast<CharType*>(buffer) + alreadyRead);
            }
            _buf.trim_front(now);
            alreadyRead += now;
            minBytes -= now;
            maxBytes -= now;
        }

        // printf("Buffer now = %zu    %zu\t%zu\t%zu\n",(size_t)buffer,alreadyRead,minBytes,maxBytes);

        if (minBytes <= 0) {
            // printf("Done %zu\n",alreadyRead);
            return alreadyRead;
        }

        if (_buf.get() != reinterpret_cast<CharType*>(buffer) + alreadyRead){
            // printf("Copy here\n");
            _buf = std::move( temporary_buffer<CharType>(reinterpret_cast<CharType*>(buffer)+alreadyRead, maxBytes, make_free_deleter(NULL) ) );
        }

        // printf("_buf now = %zu    %zu\n",(size_t)_buf.get(),alreadyRead);

        // _buf is now empty
        _fd.setBuffer(std::move(_buf));
        return _fd.kj_get(maxBytes).then([this, minBytes, maxBytes, buffer, alreadyRead] (auto buf) mutable {
            if (buf.size() == 0) {
                _eof = true;
                // printf("OK, Done  %zu\n",alreadyRead);
                return kj::Promise<size_t>(alreadyRead);
            }
            _buf = std::move(buf);
            // printf("Buff now = %zu     %zu\n",(size_t)_buf.get(),_buf.size() );

            auto now = std::min(maxBytes, _buf.size());
            if (_buf.get() != reinterpret_cast<CharType*>(buffer) + alreadyRead)
                std::copy(_buf.get(), _buf.get() + now, reinterpret_cast<CharType*>(buffer) + alreadyRead);
            _buf.trim_front(now);

            alreadyRead += now;
            minBytes -= now;
            maxBytes -= now;
            
            // printf("Buff after trim now = %zu\t%zu\n",(size_t)_buf.get(), (size_t)(reinterpret_cast<CharType*>(buffer) + alreadyRead) );
            // printf("Hey a, alreadyRead = %zu,  buff size = %zu,  minBytes = %zu\n",alreadyRead,_buf.size(),minBytes);

            if (minBytes<=0) {
                // printf("Returned %zu\n",alreadyRead);
                return kj::Promise<size_t>(alreadyRead);
            }
            return this->tryReadInternal(buffer, minBytes, maxBytes, alreadyRead);
        });

    }
#endif
};

// Facilitates data buffering before it's handed over to data_sink.
//
// When trim_to_size is true it's guaranteed that data sink will not receive
// chunks larger than the configured size, which could be the case when a
// single write call is made with data larger than the configured size.
//
// The data sink will not receive empty chunks.
//
#ifdef __USE_KJ__
template <typename CharType>
class output_stream final : public kj::AsyncOutputStream {
#else
template <typename CharType>
class output_stream final {
#endif
    static_assert(sizeof(CharType) == 1, "must buffer stream of bytes");
    data_sink _fd;
    temporary_buffer<CharType> _buf;
    size_t _size = 0;
    size_t _begin = 0;
    size_t _end = 0;
    bool _trim_to_size = false;
private:
    size_t available() const { return _end - _begin; }
    size_t possibly_available() const { return _size - _begin; }
    future<> split_and_put(temporary_buffer<CharType> buf);
#ifdef __USE_KJ__
    kj::Promise<void> kj_split_and_put(temporary_buffer<CharType> buf);
#endif
public:
    using char_type = CharType;
    output_stream() = default;
    output_stream(data_sink fd, size_t size, bool trim_to_size = false)
        : _fd(std::move(fd)), _size(size), _trim_to_size(trim_to_size) {}
    output_stream(output_stream&&) = default;
    output_stream& operator=(output_stream&&) = default;
    future<> write(const char_type* buf, size_t n);
    future<> write(const char_type* buf);

    template <typename StringChar, typename SizeType, SizeType MaxSize>
    future<> write(const basic_sstring<StringChar, SizeType, MaxSize>& s);
    future<> write(const std::basic_string<char_type>& s);

    future<> write(net::packet p);
    future<> write(scattered_message<char_type> msg);
    future<> flush();
    future<> close() { return flush().then([this] { return _fd.close(); }); }
#ifdef __USE_KJ__

    kj::Promise<void> kj_write(const char_type* buf, size_t n);
    kj::Promise<void> kj_write(const char_type* buf);

    template <typename StringChar, typename SizeType, SizeType MaxSize>
    kj::Promise<void> kj_write(const basic_sstring<StringChar, SizeType, MaxSize>& s);

    kj::Promise<void> kj_write(const std::basic_string<char_type>& s);

    kj::Promise<void> kj_write(net::packet p);
    kj::Promise<void> kj_write(scattered_message<char_type> msg);
    kj::Promise<void> kj_flush();
    kj::Promise<void> kj_close() { return kj_flush().then([this] { return _fd.kj_close(); }); }

    kj::Promise<void> write(const void* buffer, size_t size) override {
        return kj_write((const char_type*)buffer, size).then([this]() {            
            return kj_flush();
        });
    }

    kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const kj::byte>> pieces) override {
        if (pieces.size() == 0) {
            return writeInternal(nullptr, nullptr);
        } else {
            return writeInternal(pieces[0], pieces.slice(1, pieces.size()));
        }
    }

private:
    kj::Promise<void> writeInternal(kj::ArrayPtr<const kj::byte> firstPiece,
                                    kj::ArrayPtr<const kj::ArrayPtr<const kj::byte>> morePieces) {

        auto promise = kj_write((const char_type*)firstPiece.begin(),firstPiece.size() );
        for (size_t i = 0;i<morePieces.size();++i){
            auto & pieces = morePieces[i];
            promise = promise.then([&pieces,this](){   
                return kj_write((const char_type*)pieces.begin(),pieces.size() );
            });
        };

        return promise.then([this]() {            
            return kj_flush();
        });
        /*KJ_NONBLOCKING_SYSCALL(writeResult = ::writev(fd, iov.begin(), iov.size())) {
          // Error.

          // We can't "return kj::READY_NOW;" inside this block because it causes a memory leak due to
          // a bug that exists in both Clang and GCC:
          // http://gcc.gnu.org/bugzilla/show_bug.cgi?id=33799
          // http://llvm.org/bugs/show_bug.cgi?id=12286
          goto error;
        }
        if (false) {
        error:
          return kj::READY_NOW;
        }

        // A negative result means EAGAIN, which we can treat the same as having written zero bytes.
        size_t n = writeResult < 0 ? 0 : writeResult;

        // Discard all data that was written, then issue a new write for what's left (if any).
        for (;;) {
          if (n < firstPiece.size()) {
            // Only part of the first piece was consumed. Wait for POLLOUT and then write again.
            firstPiece = firstPiece.slice(n, firstPiece.size());
            return onWritable().then([=]() {
              return writeInternal(firstPiece, morePieces);
            });
          } else if (morePieces.size() == 0) {
            // First piece was fully-consumed and there are no more pieces, so we're done.
            KJ_DASSERT(n == firstPiece.size(), n);
            return kj::READY_NOW;
          } else {
            // First piece was fully consumed, so move on to the next piece.
            n -= firstPiece.size();
            firstPiece = morePieces[0];
            morePieces = morePieces.slice(1, morePieces.size());
          }
        }//*/
    }

#endif
private:
};


#include "iostream-impl.hh"
