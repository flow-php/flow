<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ReactPHP\Tests\Unit\Server;

use Flow\ETL\Async\ReactPHP\Server\SocketServer;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

final class SocketServerTest extends TestCase
{
    public function test_start_server_without_initialization() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Server not initialized');

        $server = SocketServer::tcp(6541, new NullLogger());

        $server->start();
    }

    public function test_unix_socket_using_non_existing_folder_path() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Given path does not exists or is not valid folder: non_existing_folder');
        SocketServer::unixDomain('non_existing_folder', new NullLogger());
    }
}
