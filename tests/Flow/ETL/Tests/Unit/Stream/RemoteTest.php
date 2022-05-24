<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Stream;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Stream\RemoteFile;
use PHPUnit\Framework\TestCase;

final class RemoteTest extends TestCase
{
    public function test_invalid_stream() : void
    {
        $this->expectExceptionMessage('Stream uri is missing scheme');
        $this->expectException(InvalidArgumentException::class);

        new RemoteFile('test');
    }

    public function test_invalid_stream_scheme() : void
    {
        $this->expectExceptionMessage('Stream scheme must starts with "flow-"');
        $this->expectException(InvalidArgumentException::class);

        new RemoteFile('file://some_file.txt');
    }

    public function test_unknown_stream_scheme() : void
    {
        $this->expectExceptionMessage('Unknown scheme "flow-invalid"');
        $this->expectException(InvalidArgumentException::class);

        new RemoteFile('flow-invalid://some_file.txt');
    }
}
