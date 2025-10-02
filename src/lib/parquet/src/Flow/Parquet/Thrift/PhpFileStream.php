<?php

declare(strict_types=1);

namespace Flow\Parquet\Thrift;

use Thrift\Exception\TTransportException;

final class PhpFileStream implements Transport
{
    /**
     * @var resource
     */
    private $stream;

    /**
     * @param resource $resource
     *
     * @throws TTransportException
     */
    public function __construct($resource)
    {
        if (!\is_resource($resource)) {
            throw new TTransportException('Expecting open stream resource');
        }

        $this->stream = $resource;
    }

    public function available() : int
    {
        return 1;
    }

    public function close() : void
    {
        @\fclose($this->stream);
        $this->stream = null;
    }

    public function flush() : void
    {
        @\fflush($this->stream);
    }

    public function isOpen() : bool
    {
        return \is_resource($this->stream);
    }

    public function open() : void
    {
        if (!\is_resource($this->stream)) {
            throw new TTransportException('TPhpStream: Could not open stream');
        }
    }

    public function read(int $len) : string
    {
        $data = @\fread($this->stream, $len);

        if ($data === false || $data === '') {
            throw new TTransportException('PhpStream: Could not read ' . $len . ' bytes');
        }

        return $data;
    }

    public function write(string $buf) : void
    {
        while ($buf !== '') {
            $got = @\fwrite($this->stream, $buf);

            if ($got === 0 || $got === false) {
                throw new TTransportException('PhpStream: Could not write ' . \strlen($buf) . ' bytes');
            }

            $buf = \substr($buf, $got);
        }
    }
}
