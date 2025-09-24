<?php

declare(strict_types=1);

namespace Flow\Parquet\Thrift;

class BufferedTransport implements Transport
{
    private string $rBuf = '';

    private string $wBuf = '';

    public function __construct(
        private readonly Transport $transport,
        private readonly int $rBufSize = 512,
        private readonly int $wBufSize = 512,
    ) {
    }

    public function available() : int
    {
        return \strlen($this->rBuf) + $this->transport->available();
    }

    public function close() : void
    {
        $this->transport->close();
    }

    public function flush() : void
    {
        if ($this->wBuf !== '') {
            $this->transport->write($this->wBuf);
            $this->wBuf = '';
        }

        if (\method_exists($this->transport, 'flush')) {
            $this->transport->flush();
        }
    }

    public function isOpen() : bool
    {
        return $this->transport->isOpen();
    }

    public function open() : void
    {
        $this->transport->open();
    }

    public function read(int $len) : string
    {
        if ($this->rBuf === '') {
            $this->rBuf = $this->transport->read($this->rBufSize);
        }

        // If we need more data than what's in the buffer, read more
        while (\strlen($this->rBuf) < $len) {
            $moreData = $this->transport->read($this->rBufSize);
            $this->rBuf .= $moreData;
        }

        if (\strlen($this->rBuf) <= $len) {
            $ret = $this->rBuf;
            $this->rBuf = '';

            return $ret;
        }

        $ret = \substr($this->rBuf, 0, $len);
        $this->rBuf = \substr($this->rBuf, $len);

        return $ret;
    }

    public function write(string $buf) : void
    {
        $this->wBuf .= $buf;

        if (\strlen($this->wBuf) >= $this->wBufSize) {
            $this->transport->write($this->wBuf);
            $this->wBuf = '';
        }
    }
}
