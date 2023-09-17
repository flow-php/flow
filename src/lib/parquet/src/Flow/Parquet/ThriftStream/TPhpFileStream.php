<?php declare(strict_types=1);

namespace Flow\Parquet\ThriftStream;

use Thrift\Exception\TException;
use Thrift\Factory\TStringFuncFactory;
use Thrift\Transport\TTransport;

final class TPhpFileStream extends TTransport
{
    /**
     * @var resource
     */
    private $stream;

    /**
     * @param resource $resource
     *
     * @throws TException
     */
    public function __construct($resource)
    {
        if (!\is_resource($resource)) {
            throw new TException('Expecting open stream resource');
        }

        $this->stream = $resource;
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

    public function isOpen()
    {
        return \is_resource($this->stream);
    }

    public function open() : void
    {
        if (!\is_resource($this->stream)) {
            throw new TException('TPhpStream: Could not open php://input');
        }
    }

    public function read($len)
    {
        $data = @\fread($this->stream, $len);

        if ($data === false || $data === '') {
            throw new TException('TPhpStream: Could not read ' . $len . ' bytes');
        }

        return $data;
    }

    public function write($buf) : void
    {
        while (TStringFuncFactory::create()->strlen($buf) > 0) {
            $got = @\fwrite($this->stream, $buf);

            if ($got === 0 || $got === false) {
                throw new TException(
                    'TPhpStream: Could not write ' . TStringFuncFactory::create()->strlen($buf) . ' bytes'
                );
            }
            $buf = TStringFuncFactory::create()->substr($buf, $got);
        }
    }
}
