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

#ifndef FILE_HH_
#define FILE_HH_

#include "stream.hh"
#include "sstring.hh"
#include "core/shared_ptr.hh"
#include "core/align.hh"
#include "core/future-util.hh"
#include <experimental/optional>
#include <system_error>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <sys/uio.h>
#include <unistd.h>

#ifdef __USE_KJ__
#include "kj/debug.h"
#include "kj/async.h"
#include "kj/async-io.h"
#endif
enum class directory_entry_type {
    block_device,
    char_device,
    directory,
    fifo,
    link,
    regular,
    socket,
};

struct directory_entry {
    sstring name;
    std::experimental::optional<directory_entry_type> type;
};

class file_impl {
public:
    virtual ~file_impl() {}

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len) = 0;
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov) = 0;
    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len) = 0;
    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov) = 0;
    virtual future<> flush(void) = 0;
    virtual future<struct stat> stat(void) = 0;
    virtual future<> truncate(uint64_t length) = 0;
    virtual future<> discard(uint64_t offset, uint64_t length) = 0;
    virtual future<> allocate(uint64_t position, uint64_t length) = 0;
    virtual future<size_t> size(void) = 0;
    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) = 0;
#ifdef __USE_KJ__
    virtual kj::Promise<size_t> kj_write_dma(uint64_t pos, const void* buffer, size_t len) = 0;
    virtual kj::Promise<size_t> kj_write_dma(uint64_t pos, std::vector<iovec> iov) = 0;
    virtual kj::Promise<size_t> kj_read_dma(uint64_t pos, void* buffer, size_t len) = 0;
    virtual kj::Promise<size_t> kj_read_dma(uint64_t pos, std::vector<iovec> iov) = 0;
    virtual kj::Promise<void> kj_flush(void) = 0;
    virtual kj::Promise<struct stat> kj_stat(void) = 0;
    virtual kj::Promise<void> kj_truncate(uint64_t length) = 0;
    virtual kj::Promise<void> kj_discard(uint64_t offset, uint64_t length) = 0;
	virtual kj::Promise<void> kj_allocate(uint64_t position, uint64_t length) = 0;
    virtual kj::Promise<size_t> kj_size(void) = 0;

#endif    

    friend class reactor;
};

class posix_file_impl : public file_impl {
public:
    int _fd;
    posix_file_impl(int fd) : _fd(fd) {}
    ~posix_file_impl() {
        if (_fd != -1) {
            ::close(_fd);
        }
    }

    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len);
    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov);
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len);
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov);
    future<> flush(void);
    future<struct stat> stat(void);
    future<> truncate(uint64_t length);
    future<> discard(uint64_t offset, uint64_t length);
    virtual future<> allocate(uint64_t position, uint64_t length) override;
    future<size_t> size(void);
#ifdef __USE_KJ__

    kj::Promise<size_t> kj_write_dma(uint64_t pos, const void* buffer, size_t len) override;
    kj::Promise<size_t> kj_write_dma(uint64_t pos, std::vector<iovec> iov) override;
    kj::Promise<size_t> kj_read_dma(uint64_t pos, void* buffer, size_t len) override;
    kj::Promise<size_t> kj_read_dma(uint64_t pos, std::vector<iovec> iov) override;
    kj::Promise<void> kj_flush(void) override;
    kj::Promise<struct stat> kj_stat(void) override;
    kj::Promise<void> kj_truncate(uint64_t length) override;
    kj::Promise<void> kj_discard(uint64_t offset, uint64_t length) override;
	kj::Promise<void> kj_allocate(uint64_t position, uint64_t length) override;
    kj::Promise<size_t> kj_size(void) override;

#endif    
    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override;
};

class blockdev_file_impl : public posix_file_impl {
public:
    blockdev_file_impl(int fd) : posix_file_impl(fd) {}
    future<> truncate(uint64_t length) override;
    future<> discard(uint64_t offset, uint64_t length) override;
    future<size_t> size(void) override;
	virtual future<> allocate(uint64_t position, uint64_t length) override;
#ifdef __USE_KJ__
    kj::Promise<void> kj_truncate(uint64_t length);
    kj::Promise<void> kj_discard(uint64_t offset, uint64_t length);
    kj::Promise<size_t> kj_size(void);
	virtual kj::Promise<void> kj_allocate(uint64_t position, uint64_t length) override;
#endif    
};

inline
std::unique_ptr<file_impl>
make_file_impl(int fd) {
    struct stat st;
    ::fstat(fd, &st);
    if (S_ISBLK(st.st_mode)) {
        return std::unique_ptr<file_impl>(new blockdev_file_impl(fd));
    } else {
        return std::unique_ptr<file_impl>(new posix_file_impl(fd));
    }
}

class file {
    std::unique_ptr<file_impl> _file_impl;
private:
    explicit file(int fd) : _file_impl(make_file_impl(fd)) {}

public:
    file(file&& x) : _file_impl(std::move(x._file_impl)) {}
    file& operator=(file&& x) noexcept = default;

    // O_DIRECT reading requires that buffer, offset, and read length, are
    // all aligned. Alignment of 4096 was necessary in the past, but no longer
    // is - 512 is usually enough; But we'll need to use BLKSSZGET ioctl to
    // be sure it is really enough on this filesystem. 4096 is always safe.
    // In addition, if we start reading in things outside page boundaries,
    // we will end up with various pages around, some of them with
    // overlapping ranges. Those would be very challenging to cache.
    static constexpr uint64_t dma_alignment = 4096;

    // Make sure alignment is a power of 2
    static_assert(dma_alignment > 0 && !(dma_alignment & (dma_alignment - 1)),
                  "dma_alignment must be a power of 2");


    /**
     * Perform a single DMA read operation.
     *
     * @param aligned_pos offset to begin reading at (should be aligned)
     * @param aligned_buffer output buffer (should be aligned)
     * @param aligned_len number of bytes to read (should be aligned)
     *
     * Alignment is HW dependent but use 4KB alignment to be on the safe side as
     * explained above.
     *
     * @return number of bytes actually read
     * @throw exception in case of I/O error
     */
    template <typename CharType>
    future<size_t>
    dma_read(uint64_t aligned_pos, CharType* aligned_buffer, size_t aligned_len) {
        return _file_impl->read_dma(aligned_pos, aligned_buffer, aligned_len);
    }

    /**
     * Read the requested amount of bytes starting from the given offset.
     *
     * @param pos offset to begin reading from
     * @param len number of bytes to read
     *
     * @return temporary buffer containing the requested data.
     * @throw exception in case of I/O error
     *
     * This function doesn't require any alignment for both "pos" and "len"
     *
     * @note size of the returned buffer may be smaller than "len" if EOF is
     *       reached of in case of I/O error.
     */
    template <typename CharType>
    future<temporary_buffer<CharType>> dma_read(uint64_t pos, size_t len) {
        return dma_read_bulk<CharType>(pos, len).then(
                [len] (temporary_buffer<CharType> buf) {
            if (len < buf.size()) {
                buf.trim(len);
            }

            return std::move(buf);
        });
    }

    class eof_error : public std::exception {};

    /**
     * Read the exact amount of bytes.
     *
     * @param pos offset in a file to begin reading from
     * @param len number of bytes to read
     *
     * @return temporary buffer containing the read data
     * @throw end_of_file_error if EOF is reached, file_io_error or
     *        std::system_error in case of I/O error.
     */
    template <typename CharType>
    future<temporary_buffer<CharType>>
    dma_read_exactly(uint64_t pos, size_t len) {
        return dma_read<CharType>(pos, len).then(
                [pos, len] (auto buf) {
            if (buf.size() < len) {
                throw eof_error();
            }

            return std::move(buf);
        });
    }

    future<size_t> dma_read(uint64_t pos, std::vector<iovec> iov) {
        return _file_impl->read_dma(pos, std::move(iov));
    }

    template <typename CharType>
    future<size_t> dma_write(uint64_t pos, const CharType* buffer, size_t len) {
        return _file_impl->write_dma(pos, buffer, len);
    }

    future<size_t> dma_write(uint64_t pos, std::vector<iovec> iov) {
        return _file_impl->write_dma(pos, std::move(iov));
    }

    future<> flush() {
        return _file_impl->flush();
    }

    future<struct stat> stat() {
        return _file_impl->stat();
    }

    future<> truncate(uint64_t length) {
        return _file_impl->truncate(length);
    }

    /// Preallocate disk blocks for a specified byte range.
    ///
    /// Requests the file system to allocate disk blocks to
    /// back the specified range (\c length bytes starting at
    /// \c position).  The range may be outside the current file
    /// size; the blocks can then be used when appending to the
    /// file.
    ///
    /// \param position beginning of the range at which to allocate
    ///                 blocks.
    /// \parm length length of range to allocate.
    /// \return future that becomes ready when the operation completes.
    future<> allocate(uint64_t position, uint64_t length) {
        return _file_impl->allocate(position, length);
    }
    future<> discard(uint64_t offset, uint64_t length) {
        return _file_impl->discard(offset, length);
    }

    future<size_t> size() {
        return _file_impl->size();
    }

    subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) {
        return _file_impl->list_directory(std::move(next));
    }

#ifdef __USE_KJ__

/// Preallocate disk blocks for a specified byte range.
    ///
    /// Requests the file system to allocate disk blocks to
    /// back the specified range (\c length bytes starting at
    /// \c position).  The range may be outside the current file
    /// size; the blocks can then be used when appending to the
    /// file.
    ///
    /// \param position beginning of the range at which to allocate
    ///                 blocks.
    /// \parm length length of range to allocate.
    /// \return future that becomes ready when the operation completes.
    kj::Promise<void> kj_allocate(uint64_t position, uint64_t length) {
        return _file_impl->kj_allocate(position, length);
    }

    template <typename CharType>
    kj::Promise<size_t> kj_dma_read(uint64_t pos, CharType* buffer, size_t len) {
        return _file_impl->kj_read_dma(pos, buffer, len);
    }

    kj::Promise<size_t> kj_dma_read(uint64_t pos, std::vector<iovec> iov) {
        return _file_impl->kj_read_dma(pos, std::move(iov));
    }

    template <typename CharType>
    kj::Promise<size_t> kj_dma_write(uint64_t pos, const CharType* buffer, size_t len) {
        return _file_impl->kj_write_dma(pos, buffer, len);
    }

    kj::Promise<size_t> kj_dma_write(uint64_t pos, std::vector<iovec> iov) {
        return _file_impl->kj_write_dma(pos, std::move(iov));
    }

    kj::Promise<void> kj_flush() {
        return _file_impl->kj_flush();
    }

    kj::Promise<struct stat> kj_stat() {
        return _file_impl->kj_stat();
    }

    kj::Promise<void> kj_truncate(uint64_t length) {
        return _file_impl->kj_truncate(length);
    }

    kj::Promise<void> kj_discard(uint64_t offset, uint64_t length) {
        return _file_impl->kj_discard(offset, length);
    }

    kj::Promise<size_t> kj_size() {
        return _file_impl->kj_size();
    }
#endif    


private: 
 template <typename CharType>
    struct read_state {
        typedef temporary_buffer<CharType> tmp_buf_type;

        read_state(uint64_t offset, uint64_t front, size_t to_read)
        : buf(tmp_buf_type::aligned(dma_alignment,
                                    align_up(to_read, dma_alignment)))
        , _offset(offset)
        , _to_read(to_read)
        , _front(front) {}

        bool done() const {
            return eof || pos >= _to_read;
        }

        /**
         * Trim the buffer to the actual number of read bytes and cut the
         * bytes from offset 0 till "_front".
         *
         * @note this function has to be called only if we read bytes beyond
         *       "_front".
         */
        void trim_buf_before_ret() {
            assert(have_good_bytes());

            buf.trim(pos);
            buf.trim_front(_front);
        }

        uint64_t cur_offset() const {
            return _offset + pos;
        }

        size_t left_space() const {
            return buf.size() - pos;
        }
        
        size_t left_to_read() const {
            // positive as long as (done() == false)
            return _to_read - pos;
        }

        void append_new_data(tmp_buf_type& new_data) {
            auto to_copy = std::min(left_space(), new_data.size());

            std::memcpy(buf.get_write() + pos, new_data.get(), to_copy);
            pos += to_copy;
        }

        bool have_good_bytes() const {
            return pos > _front;
        }

    public:
        bool         eof      = false;
        tmp_buf_type buf;
        size_t       pos      = 0;
    private:
        uint64_t     _offset;
        size_t       _to_read;
        uint64_t     _front;
    };

public:
    /**
     * Read a data bulk containing the provided addresses range that starts at
     * the given offset and ends at either the address aligned to
     * dma_alignment (4KB) or at the file end.
     *
     * @param offset starting address of the range the read bulk should contain
     * @param range_size size of the addresses range
     *
     * @return temporary buffer containing the read data bulk.
     * @throw system_error exception in case of I/O error or eof_error when
     *        "offset" is beyond EOF.
     */
    template <typename CharType>
    future<temporary_buffer<CharType>>
    dma_read_bulk(uint64_t offset, size_t range_size);

#ifdef __USE_KJ__
    template <typename CharType>
    kj::Promise<temporary_buffer<CharType>>
    kj_dma_read_bulk(uint64_t offset, size_t range_size);
#endif    

private:
    template <typename CharType>
    struct read_state;

    /**
     * Try to read from the given position where the previous short read has
     * stopped. Check the EOF condition.
     *
     * The below code assumes the following: short reads due to I/O errors
     * always end at address aligned to HW block boundary. Therefore if we issue
     * a new read operation from the next position we are promised to get an
     * error (different from EINVAL). If we've got a short read because we have
     * reached EOF then the above read would either return a zero-length success
     * (if the file size is aligned to HW block size) or an EINVAL error (if
     * file length is not aligned to HW block size).
     *
     * @param pos offset to read from
     * @param len number of bytes to read
     *
     * @return temporary buffer with read data or zero-sized temporary buffer if
     *         pos is at or beyond EOF.
     * @throw appropriate exception in case of I/O error.
     */
    template <typename CharType>
    future<temporary_buffer<CharType>>
    read_maybe_eof(uint64_t pos, size_t len);

#ifdef __USE_KJ__
    template <typename CharType>
    kj::Promise<temporary_buffer<CharType>>
    kj_read_maybe_eof(uint64_t pos, size_t len);
#endif    

    friend class reactor;
};


template <typename CharType>
future<temporary_buffer<CharType>>
file::dma_read_bulk(uint64_t offset, size_t range_size) {
    using tmp_buf_type = typename read_state<CharType>::tmp_buf_type;

    auto front = offset & (dma_alignment - 1);
    offset -= front;
    range_size += front;

    auto rstate = make_lw_shared<read_state<CharType>>(offset, front,
                                                       range_size);

    //
    // First, try to read directly into the buffer. Most of the reads will
    // end here.
    //
    auto read = dma_read(offset, rstate->buf.get_write(),
                         rstate->buf.size());

    return read.then([rstate, this] (size_t size) mutable {
        rstate->pos = size;

        //
        // If we haven't read all required data at once -
        // start read-copy sequence. We can't continue with direct reads
        // into the previously allocated buffer here since we have to ensure
        // the aligned read length and thus the aligned destination buffer
        // size.
        //
        // The copying will actually take place only if there was a HW glitch.
        // In EOF case or in case of a persistent I/O error the only overhead is
        // an extra allocation.
        //
        return do_until(
            [rstate] { return rstate->done(); },
            [rstate, this] () mutable {
            return read_maybe_eof<CharType>(
                rstate->cur_offset(), rstate->left_to_read()).then(
                    [rstate] (auto buf1) mutable {
                if (buf1.size()) {
                    rstate->append_new_data(buf1);
                } else if (rstate->have_good_bytes()){
                    rstate->eof = true;
                } else {
                    throw eof_error();
                }

                return make_ready_future<>();
            });
        }).then([rstate] () mutable {
            //
            // If we are here we are promised to have read some bytes beyond
            // "front" so we may trim straight away.
            //
            rstate->trim_buf_before_ret();
            return make_ready_future<tmp_buf_type>(std::move(rstate->buf));
        });
    });
}

#ifdef __USE_KJ__
template <typename CharType>
kj::Promise<temporary_buffer<CharType>>
file::kj_dma_read_bulk(uint64_t offset, size_t range_size) {
    using tmp_buf_type = typename read_state<CharType>::tmp_buf_type;

    auto front = offset & (dma_alignment - 1);
    offset -= front;
    range_size += front;

    auto rstate = make_lw_shared<read_state<CharType>>(offset, front,
                                                       range_size);

    //
    // First, try to read directly into the buffer. Most of the reads will
    // end here.
    //
    auto read = kj_dma_read(offset, rstate->buf.get_write(),
                         rstate->buf.size());

    return read.then([rstate, this] (size_t size) mutable {
        rstate->pos = size;

        //
        // If we haven't read all required data at once -
        // start read-copy sequence. We can't continue with direct reads
        // into the previously allocated buffer here since we have to ensure
        // the aligned read length and thus the aligned destination buffer
        // size.
        //
        // The copying will actually take place only if there was a HW glitch.
        // In EOF case or in case of a persistent I/O error the only overhead is
        // an extra allocation.
        //
        return make_kj_promise( do_until(
            [rstate] { return rstate->done(); },
            [rstate, this] () mutable {
            return read_maybe_eof<CharType>(
                rstate->cur_offset(), rstate->left_to_read()).then(
                    [rstate] (auto buf1) mutable {
                if (buf1.size()) {
                    rstate->append_new_data(buf1);
                } else if (rstate->have_good_bytes()){
                    rstate->eof = true;
                } else {
                    throw eof_error();
                }

                return make_ready_future<>();
            });
        }).then([rstate] () mutable {
            //
            // If we are here we are promised to have read some bytes beyond
            // "front" so we may trim straight away.
            //
            rstate->trim_buf_before_ret();
            return make_ready_future<tmp_buf_type>(std::move(rstate->buf));
        }));
    });
}

#endif

template <typename CharType>
future<temporary_buffer<CharType>>
file::read_maybe_eof(uint64_t pos, size_t len) {
    //
    // We have to allocate a new aligned buffer to make sure we don't get
    // an EINVAL error due to unaligned destination buffer.
    //
    temporary_buffer<CharType> buf = temporary_buffer<CharType>::aligned(
               dma_alignment, align_up(len, dma_alignment));

    // try to read a single bulk from the given position
    return dma_read(pos, buf.get_write(), buf.size()).then_wrapped(
            [buf = std::move(buf)](future<size_t> f) mutable {
        try {
            size_t size = std::get<0>(f.get());

            buf.trim(size);

            return std::move(buf);
        } catch (std::system_error& e) {
            //
            // TODO: implement a non-trowing file_impl::dma_read() interface to
            //       avoid the exceptions throwing in a good flow completely.
            //       Otherwise for users that don't want to care about the
            //       underlying file size and preventing the attempts to read
            //       bytes beyond EOF there will always be at least one
            //       exception throwing at the file end for files with unaligned
            //       length.
            //
            if (e.code().value() == EINVAL) {
                buf.trim(0);
                return std::move(buf);
            } else {
                throw;
            }
        }
    });
}

#ifdef __USE_KJ__
template <typename CharType>
kj::Promise<temporary_buffer<CharType>>
file::kj_read_maybe_eof(uint64_t pos, size_t len) {
    //
    // We have to allocate a new aligned buffer to make sure we don't get
    // an EINVAL error due to unaligned destination buffer.
    //
    temporary_buffer<CharType> buf = temporary_buffer<CharType>::aligned(
               dma_alignment, align_up(len, dma_alignment));

    // try to read a single bulk from the given position
    return kj_dma_read(pos, buf.get_write(), buf.size()).then(
            [buf = std::move(buf)](auto size) mutable {                    

            buf.trim(size);
            return std::move(buf);        
    });
}

#endif

#endif /* FILE_HH_ */
