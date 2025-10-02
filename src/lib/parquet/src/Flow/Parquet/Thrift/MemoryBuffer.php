<?php

declare(strict_types=1);

namespace Flow\Parquet\Thrift;

use Thrift\Exception\TTransportException;

/**
 * A memory buffer is a transport that simply reads from and writes to an
 * in-memory string buffer. Anytime you call write on it, the data is simply
 * placed into a buffer, and anytime you call read, data is read from that
 * buffer.
 */
class MemoryBuffer implements Transport
{
    private int $length;

    private int $position = 0;

    /**
     * Constructor. Optionally pass an initial value
     * for the buffer.
     */
    public function __construct(protected string $data = '')
    {
        $this->length = \strlen($this->data);
    }

    public function available() : int
    {
        return $this->length - $this->position;
    }

    public function close() : void
    {
    }

    public function data() : string
    {
        return $this->data;
    }

    public function isOpen() : bool
    {
        return true;
    }

    public function open() : void
    {
    }

    public function read(int $len) : string
    {
        $availableBytes = $this->length - $this->position;

        if ($availableBytes === 0) {
            throw new TTransportException('TMemoryBuffer: Could not read ' . $len . ' bytes from buffer.');
        }

        if ($availableBytes <= $len) {
            $ret = substr($this->data, $this->position);
            $this->position = $this->length;

            return $ret;
        }

        $ret = substr($this->data, $this->position, $len);
        $this->position += $len;

        return $ret;
    }

    public function write(string $buf) : void
    {
        $this->data .= $buf;
        $this->length += \strlen($buf);
    }
}
