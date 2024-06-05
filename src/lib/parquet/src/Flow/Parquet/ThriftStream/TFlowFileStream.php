<?php

declare(strict_types=1);

namespace Flow\Parquet\ThriftStream;

use Flow\Parquet\Stream;
use Thrift\Exception\TException;
use Thrift\Transport\TTransport;

final class TFlowFileStream extends TTransport
{
    public function __construct(private Stream $stream, private int $offset = 0)
    {
    }

    public function close() : void
    {
        $this->stream->close();
    }

    public function flush() : void
    {
        throw new \RuntimeException('Not implemented');
    }

    public function isOpen() : bool
    {
        return $this->stream->isOpen();
    }

    public function open() : void
    {
        if (!$this->stream->isOpen()) {
            throw new TException('TPhpStream: Could not open stream');
        }
    }

    public function read($len) : string
    {
        $data = $this->stream->read($len, $this->offset, SEEK_SET);

        if ($data === false || $data === '') {
            throw new TException('TPhpStream: Could not read ' . $len . ' bytes');
        }

        $this->offset += $len;

        return $data;
    }

    public function write($buf) : void
    {
        throw new TException('TPhpStream: Could not write ' . \strlen($buf) . ' bytes');
    }
}
