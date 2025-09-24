<?php

declare(strict_types=1);

namespace Flow\Parquet\Thrift;

use Thrift\Exception\TTransportException;
use Thrift\Transport\TTransport;

/**
 * A memory buffer is a tranpsort that simply reads from and writes to an
 * in-memory string buffer. Anytime you call write on it, the data is simply
 * placed into a buffer, and anytime you call read, data is read from that
 * buffer.
 */
class MemoryBuffer extends TTransport
{
    private int $length;

    /**
     * Constructor. Optionally pass an initial value
     * for the buffer.
     */
    public function __construct(protected string $buf_ = '')
    {
        $this->length = \strlen($this->buf_);
    }

    public function available() : int
    {
        return $this->length;
    }

    public function close() : void
    {
    }

    public function getBuffer() : string
    {
        return $this->buf_;
    }

    public function isOpen() : bool
    {
        return true;
    }

    public function open() : void
    {
    }

    public function putBack($data) : void
    {
        $this->buf_ = $data . $this->buf_;
        $this->length += \strlen((string) $data);
    }

    public function read($len) : string
    {
        $bufLength = $this->length;

        if ($bufLength === 0) {
            throw new TTransportException(
                'TMemoryBuffer: Could not read ' .
                $len . ' bytes from buffer.',
                TTransportException::UNKNOWN
            );
        }

        if ($bufLength <= $len) {
            $ret = $this->buf_;
            $this->buf_ = '';
            $this->length = 0;

            return $ret;
        }

        $ret = substr($this->buf_, 0, $len);
        $this->buf_ = substr($this->buf_, $len);
        $this->length -= $len;

        return $ret;
    }

    public function write($buf) : void
    {
        $this->buf_ .= $buf;
        $this->length += \strlen($buf);
    }
}
