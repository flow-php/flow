<?php

declare(strict_types=1);

namespace Flow\Parquet\Thrift;

interface Transport
{
    public function available() : int;

    public function close() : void;

    public function isOpen() : bool;

    public function open() : void;

    public function read(int $len) : string;

    public function write(string $buf) : void;
}
